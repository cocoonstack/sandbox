module github.com/cocoonstack/sandbox/mcp

go 1.27.0

require (
	github.com/cocoonstack/sandbox/sdk/go v0.0.0
	github.com/projecteru2/core v0.1.3
)

require (
	github.com/cockroachdb/errors v1.14.0 // indirect
	github.com/cockroachdb/logtags v0.0.0-20241215232642-bb51bb14a506 // indirect
	github.com/cockroachdb/redact v1.1.8 // indirect
	github.com/cocoonstack/sandbox/protocol/wire v0.1.8 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/getsentry/sentry-go v0.48.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/mattn/go-colorable v0.1.15 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/rogpeppe/go-internal v1.16.0 // indirect
	github.com/rs/zerolog v1.35.1 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260825221802-da73d73af1c5 // indirect
	google.golang.org/grpc v1.83.1 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
)

replace github.com/cocoonstack/sandbox/sdk/go => ../sdk/go

replace github.com/cocoonstack/sandbox/protocol/wire => ../protocol/wire
