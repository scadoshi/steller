# Demo acts and tmux layout, sourced by run.sh and record.sh.
#
# Kept separate so recording can build the layout, open a window and start capture
# in between building the panes and running the acts.

CAP=0.1; SRV=0.2; A=0.3; B=0.4   # pane addresses, set after layout

build_layout() {
  tmux kill-session -t "$SESSION" 2>/dev/null || true
  rm -f "$CAPTION_FILE"; : > "$CAPTION_FILE"
  rm -rf cache                      # start from an empty dataset every take
  cargo build --quiet

  tmux new-session -d -s "$SESSION" -x "$(tput cols)" -y "$(tput lines)"
  tmux set-option -t "$SESSION" -g status off
  tmux set-option -t "$SESSION" -g pane-border-status top
  tmux set-option -t "$SESSION" -g pane-border-format ' #{pane_title} '

  # Caption strip across the top, then server on the left, two clients stacked right.
  # tmux 3.4 deprecated -p and 3.7 ignores it, which silently gives an even split; -l
  # with an explicit size is the supported spelling. The caption is a fixed 3 rows
  # rather than a percentage so it stays a thin strip at any window size.
  tmux split-window -v -b -l 3 -t "$SESSION":0.0
  tmux split-window -h -l 55% -t "$SESSION":0.1
  tmux split-window -v -l 50% -t "$SESSION":0.2

  CAP="$SESSION":0.0; SRV="$SESSION":0.1; A="$SESSION":0.2; B="$SESSION":0.3
  tmux select-pane -t "$CAP" -T 'steller'
  tmux select-pane -t "$SRV" -T 'server'
  tmux select-pane -t "$A"   -T 'client A'
  tmux select-pane -t "$B"   -T 'client B'

  for p in "$SRV" "$A" "$B"; do tmux send-keys -t "$p" 'clear' Enter; done
  tmux send-keys -t "$CAP" "source demo/lib.sh; caption_loop" Enter
  sleep 0.6
}

start_server() { tmux send-keys -t "$SRV" './target/debug/steller' Enter; sleep 1.2; }

# Leave redis-cli cleanly. Ctrl-C only drops out of subscribe mode; the process keeps
# running, so without the explicit quit the next "redis-cli -p 3000" gets typed into
# redis-cli as a command instead of starting a new one.
# Ctrl-C behaves differently depending on redis-cli's mode: in subscribe mode it drops
# back to the prompt, in command mode it exits the process outright. So send both, then
# clear, which wipes whichever one turned out to be a no-op (a stray "command not found"
# from quit hitting the shell, say) and leaves a clean pane for the next act.
disconnect() {
  tmux send-keys -t "$1" C-c; sleep 0.4
  tmux send-keys -t "$1" 'quit' Enter; sleep 0.5
  tmux send-keys -t "$1" 'clear' Enter; sleep 0.3
}

connect() { tmux send-keys -t "$1" 'redis-cli -p 3000' Enter; sleep 0.8; }

# Only act 3 needs a second client. The others left an empty pane sitting in frame for
# the whole clip, so drop it and let client A take the column; recreate it when needed.
want_two_clients() {
  local have
  have=$(tmux list-panes -t "$SESSION" | wc -l | tr -d ' ')
  if [ "$1" = "yes" ] && [ "$have" -lt 4 ]; then
    tmux split-window -v -l 50% -t "$SESSION":0.2
    tmux select-pane -t "$SESSION":0.3 -T 'client B'
    B="$SESSION":0.3
  elif [ "$1" = "no" ] && [ "$have" -ge 4 ]; then
    tmux kill-pane -t "$SESSION":0.3 2>/dev/null || true
  fi
  sleep 0.3
}

# Pin the caption to a thin strip and give the working panes the rest.
#
# This has to run after a client attaches at its final size. Sizes set on a detached
# session are proportional hints: tmux redistributes them on resize, and a 3-row
# caption set at 80x32 came back as 19 rows once the window opened at 183x51.
pin_layout() {
  # Only balance the two client panes when there are two. With a single pane in the
  # right column, resizing it to 50% hands the other half back to the caption row,
  # which is how the caption quietly grew to 24 rows.
  local panes
  panes=$(tmux list-panes -t "$SESSION" | wc -l | tr -d ' ')
  [ "$panes" -ge 4 ] && tmux resize-pane -t "$A" -y 50% 2>/dev/null
  tmux resize-pane -t "$CAP" -y 2 2>/dev/null || true
  sleep 0.3
}

# Return every pane to a known state without tearing down the tmux session, so the
# attached window (and the recording setup around it) survives between acts.
reset_state() {
  # Enumerate the client panes that actually exist. Acts that only need one client
  # kill pane 3, so a fixed list would address a pane that is gone; under `set -e`
  # that aborts the whole run rather than just the reset.
  local idx
  for idx in $(tmux list-panes -t "$SESSION" -F '#{pane_index}' | grep -v '^0$'); do
    [ "$idx" = "1" ] && continue   # 1 is the server, handled below
    tmux send-keys -t "$SESSION":0."$idx" C-c; sleep 0.2
    tmux send-keys -t "$SESSION":0."$idx" 'quit' Enter; sleep 0.2
    tmux send-keys -t "$SESSION":0."$idx" 'clear' Enter
  done
  tmux send-keys -t "$SRV" 'quit' Enter; sleep 1.2
  pkill -f 'target/debug/steller' 2>/dev/null || true
  rm -rf cache
  tmux send-keys -t "$SRV" 'clear' Enter
  : > "$CAPTION_FILE"
  sleep 0.5
}

act1_basics() {
  say "This is steller. A Redis-compatible server, written from scratch in Rust."
  start_server
  say "Let's point a real redis-cli at it and see whether it notices."
  connect "$A"
  say "Start with a ping."
  run_in "$A" 'PING'
  say "Let's set a key."
  run_in "$A" 'SET language rust'
  say "And read it back."
  run_in "$A" 'GET language'
  say "Is it there?"
  run_in "$A" 'EXISTS language'
  say "Let's get rid of it."
  run_in "$A" 'DEL language'
  say "And now it's gone."
  run_in "$A" 'GET language'
  pause
}

act2_ttl() {
  say "Keys can expire. Let's give one a sixty second deadline."
  run_in "$A" 'SET session token EX 60'
  say "Sixty seconds, counting down."
  run_in "$A" 'TTL session'
  say "PX works in milliseconds. Let's make one that only lives two seconds."
  run_in "$A" 'SET blink gone PX 2000'
  run_in "$A" 'TTL blink'
  say "Now let's wait it out."
  sleep 2.2
  say "And it expired on its own."
  run_in "$A" 'GET blink'
  say "A plain SET clears any TTL the key had. Let's check that."
  run_in "$A" 'SET session fresh'
  run_in "$A" 'TTL session'
  pause
}

act3_pubsub() {
  say "Now pub/sub. Let's bring in a second client."
  connect "$B"
  say "Client A subscribes to a channel."
  run_in "$A" 'SUBSCRIBE news'
  say "A is blocked on its own socket now. Let's have B publish something."
  run_in "$B" 'PUBLISH news "the jay is a convincing hawk"' 2.0
  say "There it is. Delivered while A's reader was still waiting."
  run_in "$B" 'PUBLISH news "second message"' 2.0
  say "What if we publish where nobody is listening?"
  run_in "$B" 'PUBLISH quiet nothing' 1.6
  say "Zero. It only counts subscribers that actually received it."
  disconnect "$A"
  disconnect "$B"
  pause
}

act4_persistence() {
  say "Let's write some keys, then take the whole server down."
  connect "$A"
  run_in "$A" 'SET survivor yes'
  say "One with a TTL too, so we can watch the clock survive as well."
  run_in "$A" 'SET expiring soon EX 3600'
  run_in "$A" 'TTL expiring'
  say "Now shut it down. No signal crate, just stdin."
  disconnect "$A"
  tmux send-keys -t "$SRV" 'quit' Enter; sleep 2.5
  say "Every thread joined. Let's start it back up."
  start_server
  connect "$A"
  say "Did the data survive?"
  run_in "$A" 'GET survivor'
  say "And the TTL? It should have kept counting while the server was down."
  run_in "$A" 'TTL expiring'
  say "Not reset. It came back lower."
  pause
}

act5_errors() {
  say "Last thing. Let's throw some bad input at it."
  run_in "$A" 'SET k v junk'
  say "An option it doesn't know."
  run_in "$A" 'SET k v EX'
  say "An option missing its argument."
  run_in "$A" 'BOGUS command'
  say "And something that isn't a command at all."
  say "Three errors, and the session is still up. Let's prove it."
  run_in "$A" 'SET k v EX 30'
  run_in "$A" 'TTL k'
  say "5,862 lines of Rust, 237 tests. github.com/scadoshi/steller"
  pause 4
}

run_act() {
  case "$1" in
    1) want_two_clients no;  pin_layout; start_server; act1_basics ;;
    2) want_two_clients no;  pin_layout; start_server; connect "$A"; act2_ttl ;;
    3) want_two_clients yes; pin_layout; start_server; connect "$A"; act3_pubsub ;;
    4) want_two_clients no;  pin_layout; start_server; act4_persistence ;;
    5) want_two_clients no;  pin_layout; start_server; connect "$A"; act5_errors ;;
    *) echo "unknown act: $1" >&2; return 1 ;;
  esac
}
