#!/bin/sh
# Composes dogecoind flags from environment variables so Compose files stay
# declarative. Any extra CLI arguments are appended verbatim.
set -eu

DATADIR="${DOGECOIN_DATADIR:-/data}"

if [ "$1" = "dogecoind" ]; then
  shift
  set -- dogecoind \
    -datadir="${DATADIR}" \
    -server=1 \
    -listen=1 \
    -printtoconsole=${DOGECOIN_PRINT_TO_CONSOLE:-1} \
    -disablewallet=${DOGECOIN_DISABLE_WALLET:-1} \
    -txindex=${DOGECOIN_TXINDEX:-1} \
    -dbcache=${DOGECOIN_DBCACHE:-2048} \
    -maxconnections=${DOGECOIN_MAX_CONNECTIONS:-64} \
    -rpcuser="${DOGECOIN_RPC_USER:-onlydoge}" \
    -rpcpassword="${DOGECOIN_RPC_PASSWORD:-onlydoge}" \
    -rpcport="${DOGECOIN_RPC_PORT:-22555}" \
    -rpcbind=0.0.0.0 \
    -rpcallowip="${DOGECOIN_RPC_ALLOW_IP:-0.0.0.0/0}" \
    -rpcthreads=${DOGECOIN_RPC_THREADS:-8} \
    -rpcworkqueue=${DOGECOIN_RPC_WORKQUEUE:-256} \
    -zmqpubrawblock="tcp://0.0.0.0:${DOGECOIN_ZMQ_PORT:-28332}" \
    -zmqpubhashblock="tcp://0.0.0.0:${DOGECOIN_ZMQ_PORT:-28332}" \
    -zmqpubrawtx="tcp://0.0.0.0:${DOGECOIN_ZMQ_PORT:-28332}" \
    -zmqpubhashtx="tcp://0.0.0.0:${DOGECOIN_ZMQ_PORT:-28332}" \
    -zmqpubhashtxhwm=${DOGECOIN_ZMQ_HASHTX_HWM:-100000} \
    -uacomment="${DOGECOIN_UACOMMENT:-OnlyDoge}" \
    ${DOGECOIN_EXTRA_ARGS:-} \
    "$@"
fi

# Drop privileges when the data directory belongs to the dogecoin user
# (fresh Docker volumes). Pre-existing root-owned data directories keep
# running as root so migrations from other images work without a chown.
if [ "$(id -u)" = "0" ] && [ "$(stat -c %u "${DATADIR}")" = "1000" ]; then
  exec setpriv --reuid=1000 --regid=1000 --clear-groups "$@"
fi

exec "$@"
