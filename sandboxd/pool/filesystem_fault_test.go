package pool

import (
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

func withoutDACOverride(t *testing.T) bool {
	t.Helper()
	if os.Geteuid() != 0 || os.Getenv("SANDBOX_TEST_WITHOUT_DAC") == t.Name() {
		return false
	}
	if runtime.GOOS != "linux" {
		t.Skip("post-rename permission fixture requires non-root or Linux setpriv")
	}
	setpriv, err := exec.LookPath("setpriv")
	if err != nil {
		t.Skip("post-rename permission fixture requires setpriv to drop root DAC capabilities")
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(t.Name(), "/")
	for i, part := range parts {
		parts[i] = "^" + regexp.QuoteMeta(part) + "$"
	}
	cmd := exec.Command(setpriv, "--bounding-set=-dac_override,-dac_read_search", binary, "-test.run="+strings.Join(parts, "/"), "-test.v")
	cmd.Env = append(os.Environ(), "SANDBOX_TEST_WITHOUT_DAC="+t.Name())
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("test without DAC override: %v\n%s", err, output)
	}
	t.Logf("test without DAC override:\n%s", output)
	return true
}
