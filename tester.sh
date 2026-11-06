#!/usr/bin/env bin
set -e

run_command() {
	printf "\033[0;36m[run] %s\033[0m\n" "$*"

	"$@"
}

run_e2e_tests() {
	run_command cargo build
	run_command ./infra --nodes 3000
	run_command ./tester --node-id 1 --port 3000

	run_command echo "removing stale binaries $PWD"
	run_command rm ./infra ./bunshin ./tester || true

	run_command echo "copying binaries to current directory $PWD"
	run_command cp ./target/debug/infra .
	run_command cp ./target/debug/bunshin .
	run_command cp ./target/debug/tester .

	printf "\ndone running e2e_tests\n"
	run_command ./infra stop-all

	pkill bunshin
}

run_e2e_tests
