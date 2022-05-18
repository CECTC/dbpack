#!/bin/sh
# wait-for-mysql.sh

set -e

sleep 30

>&2 echo "Mysql is up - executing command"
exec "$@"