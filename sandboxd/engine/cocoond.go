package engine

import (
	"bufio"
	"context"
	"encoding/json/v2"
	"fmt"
	"net"
	"net/http"
	"strings"
)

const (
	cocoondEventsURL = "http://cocoond/v1/events"
	sseMaxLine       = 1 << 20

	// VMDeleted is the change kind cocoon daemon sends when a VM record goes away.
	VMDeleted = "DELETED"
)

// VMStatus is cocoon daemon's view of one supervised VM.
type VMStatus struct {
	Name string `json:"name"`
	// Live reports that the daemon holds a live VMM process for the VM.
	Live bool `json:"live"`
}

// VMChange is one entry of cocoon daemon's change stream.
type VMChange struct {
	Kind string   `json:"type"`
	VM   VMStatus `json:"vm"`
}

// VMSyncFunc receives the stream's opening snapshot, again after every reconnect.
type VMSyncFunc func([]VMStatus)

// VMChangeFunc receives each change after the snapshot.
type VMChangeFunc func(VMChange)

// VMEvents follows cocoon daemon's event stream on socket until ctx ends or the stream breaks.
func (e *Engine) VMEvents(ctx context.Context, socket string, onSync VMSyncFunc, onChange VMChangeFunc) error {
	client := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		},
	}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, cocoondEventsURL, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("open cocoon daemon events: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("open cocoon daemon events: %s", resp.Status)
	}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64<<10), sseMaxLine)
	var event string
	for sc.Scan() {
		line := sc.Text()
		if name, ok := strings.CutPrefix(line, "event: "); ok {
			event = name
			continue
		}
		data, ok := strings.CutPrefix(line, "data: ")
		if !ok {
			continue
		}
		if err := dispatchVMEvent(event, data, onSync, onChange); err != nil {
			return err
		}
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("read cocoon daemon events: %w", err)
	}
	return fmt.Errorf("cocoon daemon closed the event stream")
}

func dispatchVMEvent(event, data string, onSync VMSyncFunc, onChange VMChangeFunc) error {
	switch event {
	case "sync":
		var snap struct {
			VMs []VMStatus `json:"vms"`
		}
		if err := json.Unmarshal([]byte(data), &snap); err != nil {
			return fmt.Errorf("parse cocoon daemon sync: %w", err)
		}
		onSync(snap.VMs)
	case "change":
		var change VMChange
		if err := json.Unmarshal([]byte(data), &change); err != nil {
			return fmt.Errorf("parse cocoon daemon change: %w", err)
		}
		onChange(change)
	}
	return nil
}
