#!/bin/sh
# wait-for-it.sh
# Script para esperar a que un servicio esté disponible

set -e

host="$1"
shift
cmd="$@"

until nc -z "$host" 2181; do
  >&2 echo "Zookeeper no está listo, esperando..."
  sleep 1
done

>&2 echo "Zookeeper está listo. Ejecutando comando."
exec $cmd