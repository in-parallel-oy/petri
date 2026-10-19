# Fabro's service supervisor: keeps one `[[run.services]]` command up for as
# long as the exec that runs this script is held open. Portable POSIX sh.
#
#   sh -c "$script" fabro-service-supervisor NAME COMMAND [PROBE TARGET]
#
# COMMAND runs with `sh -c` in the current directory, its output appended to
# $FABRO_SERVICES_DIR/NAME/log (trimmed to its newest half past
# FABRO_SERVICE_LOG_MAX bytes). When it exits, it is started again after a
# backoff that doubles from FABRO_SERVICE_BACKOFF seconds; after
# FABRO_SERVICE_MAX_CRASHES quick crashes in a row (each run shorter than
# FABRO_SERVICE_QUICK seconds) it is left down, `crashed`, until
# `fabro-service start` or `restart`. PROBE (http, tcp or command) with its
# TARGET says when a started service is `ready`; without one it is
# `running`. `status` holds the state for `fabro-service`, which asks for a
# stop, start or restart through the `control` file: a command in another
# PID namespace cannot signal this one. Every state change is also a
# `state=...` line on stdout, which the host mirrors into the run log; the
# service's own output stays in its log, which may hold secrets. A TERM
# stops the service and ends the supervisor, and so does a newer
# supervisor of the same service taking it over.

name=$1
command=$2
probe=${3:-}
target=${4:-}
root=${FABRO_SERVICES_DIR:-/tmp/fabro-services}
dir=$root/$name

# A tunable that is not a whole number keeps its default.
number() {
	case $1 in
	'' | *[!0-9]*) echo "$2" ;;
	*) echo "$1" ;;
	esac
}
backoff=$(number "${FABRO_SERVICE_BACKOFF:-}" 1)
max_crashes=$(number "${FABRO_SERVICE_MAX_CRASHES:-}" 5)
quick=$(number "${FABRO_SERVICE_QUICK:-}" 30)
log_max=$(number "${FABRO_SERVICE_LOG_MAX:-}" 4194304)
grace=5

# This supervisor, as `supervisor` names the one that holds the service:
# PIDs repeat across PID namespaces, so the namespace is part of it.
namespace=$(readlink /proc/self/ns/pid 2>/dev/null)
me="$$${namespace:+ $namespace}"
id=$$-$(printf '%s' "$namespace" | tr -cd 0-9)

mkdir -p "$dir" "$root/.bin" || exit 1
if [ -n "${FABRO_SERVICE_CLI:-}" ]; then
	printf '%s\n' "$FABRO_SERVICE_CLI" >"$root/.bin/fabro-service.$id" &&
		chmod 755 "$root/.bin/fabro-service.$id" &&
		mv -f "$root/.bin/fabro-service.$id" "$root/.bin/fabro-service"
fi
# The service inherits none of the supervisor's own settings.
unset FABRO_SERVICE_CLI FABRO_SERVICE_BACKOFF FABRO_SERVICE_MAX_CRASHES FABRO_SERVICE_QUICK \
	FABRO_SERVICE_LOG_MAX

# Every process as `pid ppid`: from /proc where there is one, else from ps.
pairs() {
	if [ -r /proc/self/stat ]; then
		for stat in /proc/[0-9]*/stat; do
			{ read -r line <"$stat"; } 2>/dev/null || continue
			# `pid (comm) state ppid ...`; comm may hold spaces and parens.
			rest=${line##*) }
			rest=${rest#* }
			pid_of=${stat#/proc/}
			echo "${pid_of%/stat} ${rest%% *}"
		done
	else
		ps -A -o pid= -o ppid= 2>/dev/null
	fi
}

# A process and every descendant it still has.
tree() {
	pairs | awk -v root="$1" '
		{ parent[$1] = $2 }
		END {
			keep[root] = 1
			for (changed = 1; changed;) {
				changed = 0
				for (p in parent) if (!(p in keep) && (parent[p] in keep)) { keep[p] = 1; changed = 1 }
			}
			for (p in keep) print p
		}'
}

# TERMs a process tree, gives it $grace seconds (or until the file $2
# appears), then KILLs it. The KILL goes to the tree as the TERM found it:
# a descendant whose parent exited on the TERM is no longer below the root.
stop_tree() {
	pids=$(tree "$1")
	[ -n "$pids" ] || pids=$1
	# shellcheck disable=SC2086 # one pid per word
	kill -TERM $pids 2>/dev/null
	n=0
	while [ ! -f "${2:-}" ] && kill -0 "$1" 2>/dev/null && [ "$n" -lt "$grace" ]; do
		sleep 1
		n=$((n + 1))
	done
	# shellcheck disable=SC2086
	kill -KILL $pids 2>/dev/null
}

# Whether PID is a supervisor, not a process that took its number.
is_supervisor() {
	if [ -r "/proc/$1/cmdline" ]; then
		tr '\0' '\n' <"/proc/$1/cmdline" | grep -qx fabro-service-supervisor
	else
		ps -o args= -p "$1" 2>/dev/null | grep -q fabro-service-supervisor
	fi
}

# Whether this supervisor still holds the service: a newer one takes it
# over by naming itself in `supervisor`.
owner() {
	{ read -r claim <"$dir/supervisor"; } 2>/dev/null
	[ "${claim:-}" = "$me" ]
}

# Take the service over from an earlier supervisor (its exec's connection
# dropped, say). One in this PID namespace is stopped here, its service
# with it; one in another namespace sees the claim within a second and
# stops its own.
{ read -r old old_namespace <"$dir/supervisor"; } 2>/dev/null
printf '%s\n' "$me" >"$dir/supervisor"
if [ -n "${old:-}" ] && [ "$old" != "$$" ] && [ "${old_namespace:-}" = "$namespace" ] &&
	is_supervisor "$old"; then
	kill -TERM "$old" 2>/dev/null
	n=0
	while kill -0 "$old" 2>/dev/null && [ "$n" -lt $((grace * 3)) ]; do
		sleep 1
		n=$((n + 1))
	done
	if kill -0 "$old" 2>/dev/null; then
		kill -KILL "$old" 2>/dev/null
		{ read -r service service_namespace <"$dir/service"; } 2>/dev/null
		if [ -n "${service:-}" ] && [ "${service_namespace:-}" = "$namespace" ]; then
			stop_tree "$service"
		fi
	fi
fi

state=
pid=
launches=0
restarts=0
last_exit=
crashes=0
started_at=0
next=0
desired=up

status() {
	state=$1
	printf 'state=%s\nrestarts=%s\nlast_exit=%s\n' "$state" "$restarts" "$last_exit" \
		>"$dir/status.$id" && mv -f "$dir/status.$id" "$dir/status"
	echo "state=$state restarts=$restarts last_exit=${last_exit:--}${2:+ $2}"
}

launch() {
	rm -f "$dir/exit.$id"
	launches=$((launches + 1))
	restarts=$((launches - 1))
	printf '%s\n' "--- fabro-service: $name started" >>"$dir/log"
	(
		sh -c "$command" </dev/null >>"$dir/log" 2>&1
		echo "$?" >"$dir/exit.$id.tmp" && mv -f "$dir/exit.$id.tmp" "$dir/exit.$id"
	) &
	pid=$!
	printf '%s %s\n' "$pid" "$namespace" >"$dir/service"
	started_at=$(date +%s)
	if [ -n "$probe" ]; then status starting; else status running; fi
}

stop_service() {
	[ -n "$pid" ] || return 0
	stop_tree "$pid" "$dir/exit.$id"
	wait "$pid" 2>/dev/null
	rm -f "$dir/exit.$id"
	pid=
}

# The probe runs in a subshell of its own: the http and tcp probes reach
# the sandbox's own ports, never through a proxy the workload env names.
ready() (
	case $probe in
	http)
		unset http_proxy HTTP_PROXY https_proxy HTTPS_PROXY all_proxy ALL_PROXY
		if command -v curl >/dev/null 2>&1; then
			curl -fsS -o /dev/null --max-time 5 "$target"
		else
			wget -q -O /dev/null -T 5 "$target"
		fi
		;;
	tcp)
		unset http_proxy HTTP_PROXY https_proxy HTTPS_PROXY all_proxy ALL_PROXY
		if command -v nc >/dev/null 2>&1; then
			nc -z -w 2 "${target%:*}" "${target##*:}"
		elif command -v curl >/dev/null 2>&1; then
			# 7: refused. Anything else reached a listener.
			curl -s -o /dev/null --max-time 2 "telnet://$target" </dev/null
			[ $? -ne 7 ]
		else
			false
		fi
		;;
	command) sh -c "$target" </dev/null ;;
	*) false ;;
	esac >/dev/null 2>&1
)

# `fabro-service start|stop|restart`: the CLI waits until `control.taken`
# is gone, so it can report the state the action left.
control() {
	[ -f "$dir/control" ] || return 0
	mv -f "$dir/control" "$dir/control.taken" || return 0
	read -r action <"$dir/control.taken"
	case $action in
	stop)
		desired=down
		stop_service
		status stopped
		;;
	start)
		desired=up
		crashes=0
		[ -n "$pid" ] || launch
		;;
	restart)
		desired=up
		crashes=0
		stop_service
		launch
		;;
	esac
	rm -f "$dir/control.taken"
}

trim_log() {
	[ -f "$dir/log" ] || return 0
	size=$(($(wc -c <"$dir/log")))
	[ "$size" -gt "$log_max" ] || return 0
	tail -c $((log_max / 2)) "$dir/log" >"$dir/log.$id" && cat "$dir/log.$id" >"$dir/log"
	rm -f "$dir/log.$id"
}

# A TERM ends the supervisor; a supervisor that was taken over leaves the
# state to the one that took it.
finish() {
	trap '' TERM INT HUP
	stop_service
	if owner; then
		status stopped
		rm -f "$dir/supervisor"
	fi
	exit 0
}
trap finish TERM INT HUP

launch
while :; do
	sleep 1
	owner || finish
	control
	trim_log
	now=$(date +%s)
	if [ -n "$pid" ]; then
		if [ -f "$dir/exit.$id" ]; then
			read -r last_exit <"$dir/exit.$id"
			rm -f "$dir/exit.$id"
			wait "$pid" 2>/dev/null
			pid=
			if [ $((now - started_at)) -lt "$quick" ]; then
				crashes=$((crashes + 1))
			else
				crashes=1
			fi
			if [ "$crashes" -ge "$max_crashes" ]; then
				desired=down
				status crashed "after $crashes quick crashes; see fabro-service logs $name, then fabro-service start $name"
			else
				delay=$backoff
				n=1
				while [ "$n" -lt "$crashes" ] && [ "$delay" -lt 30 ]; do
					delay=$((delay * 2))
					n=$((n + 1))
				done
				[ "$delay" -le 30 ] || delay=30
				next=$((now + delay))
				status starting "exited; restarting in ${delay}s"
			fi
		elif [ "$state" = starting ] && ready; then
			status ready
		fi
	elif [ "$desired" = up ] && [ "$now" -ge "$next" ]; then
		launch
	fi
done
