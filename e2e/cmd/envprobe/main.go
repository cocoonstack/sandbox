package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	sandbox "github.com/cocoonstack/sandbox/sdk/go"
)

func main() {
	tok := flag.String("token", "", "")
	tpl := flag.String("template", "", "")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	c, _ := sandbox.Connect("127.0.0.1:7777", sandbox.WithAPIToken(*tok))
	sb, err := c.New(ctx, *tpl, sandbox.WithNetwork(sandbox.NetNone))
	if err != nil {
		fmt.Println("claim:", err)
		return
	}
	defer func() { _ = sb.Close() }()
	run := func(label string, argv ...string) {
		var o, e strings.Builder
		code, err := sb.Run(ctx, sandbox.Cmd{Argv: argv, Stdout: &o, Stderr: &e})
		s := strings.TrimSpace(o.String() + e.String())
		if len(s) > 260 {
			s = s[:260]
		}
		fmt.Printf("  %-34s exit=%d err=%v\n     %s\n", label, code, err, s)
	}
	// No Env passed anywhere below: this is the "no configuration" contract.
	run("silkd unit has the vars", "/bin/sh", "-c", "grep Environment /etc/systemd/system/silkd.service | tr '\\n' ' '")
	run("silkd process env", "/bin/sh", "-c", "tr '\\0' '\\n' < /proc/$(pidof silkd)/environ | grep -i proxy | tr '\\n' ' '")
	run("exec'd process env", "/bin/sh", "-c", "env | grep -i proxy | tr '\\n' ' '")
	run("bare curl, no config", "/bin/sh", "-c", "curl -sS -m 20 -o /dev/null -w '%{http_code}' https://code.byted.org 2>&1 | tail -1")
}
