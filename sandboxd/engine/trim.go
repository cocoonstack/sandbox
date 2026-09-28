package engine

import "context"

// trimCowScript mounts the copy-on-write disk a second time, found by the serial init resolved it from (a device path on the Firecracker lane), and trims its free blocks.
const trimCowScript = `cow=$(sed -n 's/.*cocoon\.cow=\([^ ]*\).*/\1/p' /proc/cmdline)
case "$cow" in /dev/*) dev=$cow ;; *) dev=/dev/disk/by-id/virtio-$cow ;; esac
[ -b "$dev" ] || { echo "no copy-on-write disk at $dev"; exit 1; }
mkdir -p /run/sandboxd-trim && mount -t ext4 -o noatime "$dev" /run/sandboxd-trim || exit 1
fstrim /run/sandboxd-trim; rc=$?
umount /run/sandboxd-trim
exit $rc`

// TrimCow discards the blocks the guest's copy-on-write disk no longer uses, so a capture of it carries only live data.
func (e *Engine) TrimCow(ctx context.Context, vsockSocket string) error {
	ctx, cancel := context.WithTimeout(ctx, trimTimeout)
	defer cancel()
	return e.silkdExec(ctx, vsockSocket, "sh", "-c", trimCowScript)
}
