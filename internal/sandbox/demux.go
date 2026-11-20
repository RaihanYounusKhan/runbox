package sandbox

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Docker multiplexes stdout and stderr on one connection when no TTY is
// allocated. Each frame is an 8-byte header followed by the payload:
//
//	[stream byte][0][0][0][uint32 big-endian payload length]
//
// stream is 1 for stdout and 2 for stderr.
const (
	streamStdout = 1
	streamStderr = 2
)

// cappedBuffer keeps at most limit bytes and remembers whether more arrived.
type cappedBuffer struct {
	buf       bytes.Buffer
	limit     int
	truncated bool
}

func (c *cappedBuffer) write(p []byte) {
	if room := c.limit - c.buf.Len(); room > 0 {
		if len(p) > room {
			c.buf.Write(p[:room])
			c.truncated = true
			return
		}
		c.buf.Write(p)
		return
	}
	if len(p) > 0 {
		c.truncated = true
	}
}

// demuxOutput is the result of reading a multiplexed stream.
type demuxOutput struct {
	stdout, stderr cappedBuffer
}

// demux reads frames from r until EOF. Each stream keeps at most limit bytes.
// The first time either stream overflows, onOverflow is called (once) so the
// caller can kill the container rather than let it keep producing output.
func demux(r io.Reader, limit int, onOverflow func()) (*demuxOutput, error) {
	out := &demuxOutput{
		stdout: cappedBuffer{limit: limit},
		stderr: cappedBuffer{limit: limit},
	}
	overflowed := false
	var hdr [8]byte
	chunk := make([]byte, 32<<10)
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return out, nil
			}
			return out, err
		}
		size := int(binary.BigEndian.Uint32(hdr[4:8]))
		var dst *cappedBuffer
		switch hdr[0] {
		case streamStdout:
			dst = &out.stdout
		case streamStderr:
			dst = &out.stderr
		case 0: // stdin echo; never requested, but skip it if present
			dst = nil
		default:
			return out, fmt.Errorf("demux: unknown stream id %d", hdr[0])
		}
		for size > 0 {
			n := min(size, len(chunk))
			if _, err := io.ReadFull(r, chunk[:n]); err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
					return out, nil
				}
				return out, err
			}
			if dst != nil {
				dst.write(chunk[:n])
			}
			size -= n
		}
		if !overflowed && (out.stdout.truncated || out.stderr.truncated) {
			overflowed = true
			if onOverflow != nil {
				onOverflow()
			}
		}
	}
}
