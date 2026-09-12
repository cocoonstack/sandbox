// qaab measures one A/B arm against a live sandboxd: a concurrent cold-claim
// burst, then a concurrent in-guest write burst, reported as one JSON row.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	sandbox "github.com/cocoonstack/sandbox/sdk/go"
)

// one dd keeps exactly one virtio queue busy, so the writers are the queue fan-out
const ddScript = `pids=; for i in $(seq 1 %d); do dd if=/dev/zero of=/root/qaab.$i bs=%s count=%d oflag=direct conv=fsync 2>/dev/null & pids="$pids $!"; done; status=0; for pid in $pids; do wait "$pid" || status=1; done; exit "$status"`

type stats struct {
	P50 float64 `json:"p50"`
	P90 float64 `json:"p90"`
	Max float64 `json:"max"`
}

type row struct {
	Label       string  `json:"label"`
	N           int     `json:"n"`
	IOBlock     string  `json:"io_bs"`
	IOCount     int     `json:"io_count"`
	IOJobs      int     `json:"io_jobs"`
	Claim       stats   `json:"claim_ms"`
	ClaimWallMS float64 `json:"claim_wall_ms"`
	IO          stats   `json:"io_ms"`
	IOWallMS    float64 `json:"io_wall_ms"`
	IOMiBps     float64 `json:"io_aggregate_mibps"`
	Failures    int     `json:"failures"`
}

func main() {
	var (
		addr     = flag.String("addr", "127.0.0.1:18777", "sandboxd address")
		token    = flag.String("token", "", "node api token")
		template = flag.String("template", "", "template ref; leave it unpooled so every claim cold-boots")
		size     = flag.String("size", "2xlarge", "size tier")
		label    = flag.String("label", "arm", "arm label for the output row")
		n        = flag.Int("n", 24, "concurrent sandboxes")
		ioBS     = flag.String("io-bs", "1M", "dd block size per write")
		ioCount  = flag.Int("io-count", 64, "dd block count per writer")
		ioJobs   = flag.Int("io-jobs", 8, "concurrent in-guest writers per sandbox")
	)
	flag.Parse()
	if err := run(*addr, *token, *template, *size, *label, *ioBS, *n, *ioCount, *ioJobs); err != nil {
		fmt.Fprintln(os.Stderr, "qaab:", err)
		os.Exit(1)
	}
}

func run(addr, token, template, size, label, ioBS string, n, ioCount, ioJobs int) error {
	blockSize := blockBytes(ioBS)
	if blockSize == 0 {
		return fmt.Errorf("--io-bs %q must be a plain byte count or carry a k or M suffix", ioBS)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	client, err := sandbox.Connect(addr, sandbox.WithAPIToken(token))
	if err != nil {
		return err
	}

	boxes := make([]*sandbox.Sandbox, n)
	claimMS := make([]float64, n)
	claimErrs := make([]error, n)

	claimStart := time.Now()
	var claims sync.WaitGroup
	for i := range n {
		claims.Go(func() {
			start := time.Now()
			sb, claimErr := client.New(ctx, template,
				sandbox.WithSize(sandbox.Size(size)), sandbox.WithTimeout(15*time.Minute))
			claimMS[i] = msSince(start)
			boxes[i], claimErrs[i] = sb, claimErr
		})
	}
	claims.Wait()
	claimWall := msSince(claimStart)

	ioMS := make([]float64, n)
	ioErrs := make([]error, n)
	ioStart := time.Now()
	var writes sync.WaitGroup
	for i, sb := range boxes {
		if sb == nil {
			continue
		}
		writes.Go(func() {
			start := time.Now()
			var stderr strings.Builder
			code, runErr := sb.Run(ctx, sandbox.Cmd{
				Argv:   []string{"sh", "-c", fmt.Sprintf(ddScript, ioJobs, ioBS, ioCount)},
				Stderr: &stderr,
			})
			ioMS[i] = msSince(start)
			if runErr == nil && code != 0 {
				runErr = fmt.Errorf("writers exit %d: %s", code, strings.TrimSpace(stderr.String()))
			}
			ioErrs[i] = runErr
		})
	}
	writes.Wait()
	ioWall := msSince(ioStart)

	releaseErrs := make([]error, n)
	for i, sb := range boxes {
		if sb == nil {
			continue
		}
		if closeErr := sb.Close(); closeErr != nil {
			releaseErrs[i] = fmt.Errorf("release sandbox %s: %w", sb.ID, closeErr)
		}
	}

	claimOK := observed(claimMS, claimErrs)
	ioOK := observed(ioMS, ioErrs)
	out := row{
		Label: label, N: n, IOBlock: ioBS, IOCount: ioCount, IOJobs: ioJobs,
		Claim: summarize(claimOK), ClaimWallMS: claimWall,
		IO: summarize(ioOK), IOWallMS: ioWall,
		Failures: failed(claimErrs) + failed(ioErrs) + failed(releaseErrs),
	}
	if ioWall > 0 {
		written := float64(len(ioOK)*ioCount*ioJobs) * float64(blockSize) / (1 << 20)
		out.IOMiBps = written / (ioWall / 1000)
	}
	encoded, err := json.Marshal(out)
	if err != nil {
		return err
	}
	fmt.Println(string(encoded))

	for _, e := range slices.Concat(claimErrs, ioErrs, releaseErrs) {
		if e != nil {
			return e
		}
	}
	return nil
}

func msSince(t time.Time) float64 { return float64(time.Since(t).Microseconds()) / 1000 }

func blockBytes(bs string) int {
	unit := 1
	switch {
	case strings.HasSuffix(bs, "k"), strings.HasSuffix(bs, "K"):
		unit, bs = 1<<10, bs[:len(bs)-1]
	case strings.HasSuffix(bs, "M"):
		unit, bs = 1<<20, bs[:len(bs)-1]
	}
	n, err := strconv.Atoi(bs)
	if err != nil {
		return 0
	}
	return n * unit
}

func failed(errs []error) int {
	n := 0
	for _, e := range errs {
		if e != nil {
			n++
		}
	}
	return n
}

func observed(values []float64, errs []error) []float64 {
	out := make([]float64, 0, len(values))
	for i, v := range values {
		if errs[i] == nil && v > 0 {
			out = append(out, v)
		}
	}
	slices.Sort(out)
	return out
}

func summarize(sorted []float64) stats {
	if len(sorted) == 0 {
		return stats{}
	}
	at := func(p float64) float64 { return sorted[int(float64(len(sorted)-1)*p)] }
	return stats{P50: at(0.5), P90: at(0.9), Max: sorted[len(sorted)-1]}
}
