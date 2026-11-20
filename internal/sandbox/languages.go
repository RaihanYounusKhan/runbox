package sandbox

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
)

// Language describes how to run source code in a given language.
//
// Command is run by /bin/sh inside the container with stdin already
// redirected from the job's stdin file. The source file is mounted read-only
// at /workspace/<FileName>; /tmp is a small writable tmpfs.
type Language struct {
	Name     string   `json:"name"`
	Version  string   `json:"version"`
	Image    string   `json:"image"`
	FileName string   `json:"file_name"`
	Command  string   `json:"command"`
	Env      []string `json:"env,omitempty"`
}

var (
	langNameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9_+-]{0,31}$`)
	fileNameRe = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)
)

// DefaultLanguages match the images built from sandbox-images/.
func DefaultLanguages() []Language {
	return []Language{
		{
			Name: "python", Version: "3.13", Image: "runbox/python:3.13",
			FileName: "main.py", Command: "python3 -I -B /workspace/main.py",
		},
		{
			Name: "javascript", Version: "node 22", Image: "runbox/node:22",
			FileName: "main.js", Command: "node /workspace/main.js",
		},
		{
			Name: "c", Version: "gcc 14 / C17", Image: "runbox/gcc:14",
			FileName: "main.c",
			Command:  "gcc -std=c17 -O2 -pipe -o /tmp/main /workspace/main.c -lm && /tmp/main",
		},
		{
			Name: "cpp", Version: "g++ 14 / C++20", Image: "runbox/gcc:14",
			FileName: "main.cpp",
			Command:  "g++ -std=c++20 -O2 -pipe -o /tmp/main /workspace/main.cpp && /tmp/main",
		},
		{
			Name: "bash", Version: "5", Image: "runbox/python:3.13",
			FileName: "main.sh", Command: "bash /workspace/main.sh",
		},
	}
}

// LoadLanguages returns the language table, from a JSON file if path is set
// (an array of Language objects) or the built-in defaults otherwise.
func LoadLanguages(path string) (map[string]Language, error) {
	list := DefaultLanguages()
	if path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read languages file: %w", err)
		}
		list = nil
		if err := json.Unmarshal(b, &list); err != nil {
			return nil, fmt.Errorf("parse languages file: %w", err)
		}
	}
	out := make(map[string]Language, len(list))
	for _, l := range list {
		if !langNameRe.MatchString(l.Name) {
			return nil, fmt.Errorf("invalid language name %q", l.Name)
		}
		if !fileNameRe.MatchString(l.FileName) || l.FileName == "stdin" {
			return nil, fmt.Errorf("language %s: invalid file_name %q", l.Name, l.FileName)
		}
		if l.Image == "" || l.Command == "" {
			return nil, fmt.Errorf("language %s: image and command are required", l.Name)
		}
		if _, dup := out[l.Name]; dup {
			return nil, fmt.Errorf("duplicate language %q", l.Name)
		}
		out[l.Name] = l
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no languages configured")
	}
	return out, nil
}

// SortedLanguages returns the table ordered by name, for stable API output.
func SortedLanguages(m map[string]Language) []Language {
	out := make([]Language, 0, len(m))
	for _, l := range m {
		out = append(out, l)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
