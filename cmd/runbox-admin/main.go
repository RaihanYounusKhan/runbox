// Command runbox-admin performs operator tasks directly against the database:
// migrations, API key management and audit-log verification.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"runbox/internal/audit"
	"runbox/internal/db"
	"runbox/internal/store"
)

const usage = `usage: runbox-admin <command> [flags]

commands:
  migrate                         apply database migrations
  keys create -name NAME [-max-concurrent N] [-rps R -burst B]
  keys list
  keys revoke ID
  audit verify                    check the audit hash chain (exit 1 if broken)
  audit tail [-n N] [-job ID]     print recent audit entries as JSON lines

DATABASE_URL selects the database.`

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	url := os.Getenv("DATABASE_URL")
	if url == "" {
		url = "postgres://runbox:runbox@localhost:5432/runbox?sslmode=disable"
	}
	pool, err := db.Open(ctx, url)
	if err != nil {
		return err
	}
	defer pool.Close()
	st := store.New(pool)
	actor := "cli:" + os.Getenv("USER")

	switch {
	case args[0] == "migrate":
		applied, err := db.Migrate(ctx, pool)
		if err != nil {
			return err
		}
		fmt.Printf("applied %d migration(s) %v\n", len(applied), applied)

	case len(args) >= 2 && args[0] == "keys" && args[1] == "create":
		fs := flag.NewFlagSet("keys create", flag.ExitOnError)
		name := fs.String("name", "", "key name (required)")
		maxConc := fs.Int("max-concurrent", 4, "max jobs running at once for this key")
		rps := fs.Float64("rps", 0, "requests/second (0 = server default)")
		burst := fs.Int("burst", 0, "burst size (0 = server default)")
		_ = fs.Parse(args[2:])
		if *name == "" {
			return fmt.Errorf("-name is required")
		}
		p := store.NewKeyParams{Name: *name, MaxConcurrent: *maxConc}
		if *rps > 0 {
			p.RateRPS = rps
		}
		if *burst > 0 {
			p.RateBurst = burst
		}
		k, plain, err := st.CreateKey(ctx, p, actor)
		if err != nil {
			return err
		}
		fmt.Printf("created key %s (%s)\n\n  %s\n\nStore it now; it cannot be shown again.\n", k.ID, k.Name, plain)

	case len(args) >= 2 && args[0] == "keys" && args[1] == "list":
		keys, err := st.ListKeys(ctx)
		if err != nil {
			return err
		}
		tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "ID\tNAME\tPREFIX\tMAX_CONC\tCREATED\tREVOKED")
		for _, k := range keys {
			rev := "-"
			if k.RevokedAt != nil {
				rev = k.RevokedAt.Format(time.RFC3339)
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\t%s\n", k.ID, k.Name, k.Prefix, k.MaxConcurrent, k.CreatedAt.Format(time.RFC3339), rev)
		}
		tw.Flush()

	case len(args) == 3 && args[0] == "keys" && args[1] == "revoke":
		k, err := st.RevokeKey(ctx, args[2], actor)
		if err != nil {
			return err
		}
		fmt.Printf("revoked %s (%s)\n", k.ID, k.Name)

	case len(args) >= 2 && args[0] == "audit" && args[1] == "verify":
		start := time.Now()
		rep, err := audit.Verify(ctx, pool)
		if err != nil {
			return err
		}
		b, _ := json.MarshalIndent(rep, "", "  ")
		fmt.Println(string(b))
		fmt.Fprintf(os.Stderr, "verified in %s\n", time.Since(start).Round(time.Millisecond))
		if !rep.OK {
			os.Exit(1)
		}

	case len(args) >= 2 && args[0] == "audit" && args[1] == "tail":
		fs := flag.NewFlagSet("audit tail", flag.ExitOnError)
		n := fs.Int("n", 20, "number of entries")
		job := fs.String("job", "", "only entries for this job id")
		_ = fs.Parse(args[2:])
		var head int64
		if err := pool.QueryRow(ctx, `SELECT seq FROM audit_head WHERE id = 1`).Scan(&head); err != nil {
			return err
		}
		after := int64(0)
		if *job == "" {
			after = max(0, head-int64(*n))
		}
		entries, err := audit.List(ctx, pool, audit.ListFilter{AfterSeq: after, JobID: *job, Limit: *n})
		if err != nil {
			return err
		}
		enc := json.NewEncoder(os.Stdout)
		for _, e := range entries {
			_ = enc.Encode(e)
		}

	default:
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	return nil
}
