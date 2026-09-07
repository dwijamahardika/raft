.PHONY: test test-race stress demo

test:
	go test -count=1 -cover ./...

test-race:
	CGO_ENABLED=1 go test -race -count=3 -timeout 15m ./...

stress:
	go test -count=20 -run 'Unreliable|Minority|Concurrent' -timeout 30m .

demo:
	@echo "see README: run three raftkv processes"
