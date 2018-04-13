#!/bin/bash
set -eo pipefail

host="$(hostname -i || echo '127.0.0.1')"

curl -k http://localhost:8086/ping 2> /dev/null

if [ $? = 0 ]; then
	exit 0
fi

exit 1
