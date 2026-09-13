#!/usr/bin/env bash
# Runs the demo in a tmux session you attach to yourself.
#
#   ./demo/run.sh                   full run
#   ./demo/run.sh 3                 just act 3 (pub/sub)
#   TYPE_DELAY=0.02 ./demo/run.sh   faster rehearsal pass
#
# Attach with `tmux attach -t steller-demo`. To record as well, use demo/record.sh.

set -euo pipefail
cd "$(dirname "$0")/.."
source demo/lib.sh
source demo/acts.sh

require tmux "brew install tmux"
require redis-cli "brew install redis"

run_acts() {
  case "${1:-all}" in
    1) start_server; act1_basics ;;
    2) start_server; connect "$A"; act2_ttl ;;
    3) start_server; connect "$A"; act3_pubsub ;;
    4) start_server; act4_persistence ;;
    5) start_server; connect "$A"; act5_errors ;;
    all) act1_basics; act2_ttl; act3_pubsub; act4_persistence; act5_errors ;;
    *) echo "unknown act: $1" >&2; exit 1 ;;
  esac
  say "done"
}

build_layout
run_acts "${1:-all}"
