#!/usr/bin/env bash
# wait_for_it.sh host:port -- command to run
hostport=$1
shift
until nc -z $(echo $hostport | cut -d: -f1) $(echo $hostport | cut -d: -f2); do
  echo "Waiting for $hostport..."
  sleep 2
done
exec "$@"
