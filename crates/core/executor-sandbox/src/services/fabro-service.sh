#!/bin/sh
# fabro-service: the run's services, from any command in the sandbox.
#
#   fabro-service list
#   fabro-service logs [-n N] NAME
#   fabro-service start|stop|restart NAME
#
# Each service's supervisor keeps its state under
# ${FABRO_SERVICES_DIR:-/tmp/fabro-services}/NAME/ and owns its restarts;
# this only reads that state and leaves a request in the `control` file.

root=${FABRO_SERVICES_DIR:-/tmp/fabro-services}

usage() {
	echo "usage: fabro-service list | logs [-n N] <name> | start <name> | stop <name> | restart <name>" >&2
	exit 2
}

field() {
	value=$(sed -n "s/^$2=//p" "$root/$1/status" 2>/dev/null)
	echo "${value:--}"
}

known() {
	[ -f "$root/$1/status" ] && return 0
	echo "fabro-service: no service named '$1'" >&2
	exit 1
}

case ${1:-} in
list)
	[ $# -eq 1 ] || usage
	printf '%-24s %-9s %-9s %s\n' NAME STATE RESTARTS LAST_EXIT
	for status in "$root"/*/status; do
		[ -f "$status" ] || continue
		name=${status%/status}
		name=${name##*/}
		printf '%-24s %-9s %-9s %s\n' "$name" "$(field "$name" state)" \
			"$(field "$name" restarts)" "$(field "$name" last_exit)"
	done
	;;
logs)
	shift
	lines=100
	if [ "${1:-}" = -n ]; then
		[ $# -ge 2 ] || usage
		lines=$2
		shift 2
	fi
	[ $# -eq 1 ] || usage
	known "$1"
	tail -n "$lines" "$root/$1/log"
	;;
start | stop | restart)
	[ $# -eq 2 ] || usage
	known "$2"
	dir=$root/$2
	printf '%s\n' "$1" >"$dir/control.$$" && mv -f "$dir/control.$$" "$dir/control" || exit 1
	n=0
	while { [ -f "$dir/control" ] || [ -f "$dir/control.taken" ]; } && [ "$n" -lt 30 ]; do
		sleep 1
		n=$((n + 1))
	done
	if [ -f "$dir/control" ]; then
		echo "fabro-service: $2's supervisor is not running; it will $1 the service when it is" >&2
		exit 1
	fi
	echo "$2: $(field "$2" state)"
	;;
*) usage ;;
esac
