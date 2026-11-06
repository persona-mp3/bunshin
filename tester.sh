#!/usr/bin/env bin
set -e

run_command() {
	# cyan color
	printf "\033[0;36m[run] %s\033[0m\n" "$*"

	"$@"
}

run_e2e_tests() {
	run_command cargo build

	if  [[ -v CI_ENV ]] ;then
		run_command echo "currently in CI_ENV:$CI_ENV environment"
	else
		run_command echo "clearing stale binaries since not on CI"
		run_command rm ./infra ./bunshin ./tester || true
	fi
	run_command echo "copying binaries to current directory $PWD"

	run_command cp ./target/debug/infra .
	run_command cp ./target/debug/bunshin .
	run_command cp ./target/debug/tester .

	run_command ./infra --nodes 3000
	run_command ./tester --node-id 1 --port 3000

	run_command ./infra stop-all
	run_command echo "done running e2e_tests"

}

run_e2e_tests
