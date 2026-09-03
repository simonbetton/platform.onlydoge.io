# Native multi-arch Dogecoin Core image (amd64 + arm64).
#
# Built from the official release tarballs with checksum verification so
# Apple Silicon / Graviton hosts run dogecoind natively instead of under
# amd64 emulation, which starves RPC throughput during indexer sync.
#
#   docker build -f docker/dogecoin/Dockerfile -t onlydoge/dogecoin-core:1.14.9 docker/dogecoin

FROM debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171 AS fetch

ARG DOGECOIN_VERSION=1.14.9
ARG TARGETARCH

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates curl; \
    rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    case "${TARGETARCH}" in \
      amd64) PLATFORM="x86_64-linux-gnu"; SHA256="4f227117b411a7c98622c970986e27bcfc3f547a72bef65e7d9e82989175d4f8" ;; \
      arm64) PLATFORM="aarch64-linux-gnu"; SHA256="6928c895a20d0bcb6d5c7dcec753d35c884a471aaf8ad4242a89a96acb4f2985" ;; \
      *) echo "unsupported TARGETARCH=${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    TARBALL="dogecoin-${DOGECOIN_VERSION}-${PLATFORM}.tar.gz"; \
    curl -fsSL -o "/tmp/${TARBALL}" \
      "https://github.com/dogecoin/dogecoin/releases/download/v${DOGECOIN_VERSION}/${TARBALL}"; \
    echo "${SHA256}  /tmp/${TARBALL}" | sha256sum -c -; \
    mkdir -p /opt/dogecoin; \
    tar -xzf "/tmp/${TARBALL}" -C /opt/dogecoin --strip-components=1 \
      "dogecoin-${DOGECOIN_VERSION}/bin/dogecoind" \
      "dogecoin-${DOGECOIN_VERSION}/bin/dogecoin-cli"; \
    rm -f "/tmp/${TARBALL}"

FROM debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171

ARG DOGECOIN_VERSION=1.14.9
ENV DOGECOIN_VERSION=${DOGECOIN_VERSION}

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates util-linux; \
    rm -rf /var/lib/apt/lists/*; \
    groupadd -r -g 1000 dogecoin; \
    useradd -r -u 1000 -g dogecoin -d /data -s /usr/sbin/nologin dogecoin; \
    mkdir -p /data; \
    chown dogecoin:dogecoin /data

COPY --from=fetch /opt/dogecoin/bin/dogecoind /opt/dogecoin/bin/dogecoin-cli /usr/local/bin/
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

VOLUME ["/data"]
EXPOSE 22555 22556 28332

HEALTHCHECK --interval=15s --timeout=10s --start-period=60s --retries=20 \
  CMD dogecoin-cli -datadir=/data -rpcuser="${DOGECOIN_RPC_USER:-onlydoge}" -rpcpassword="${DOGECOIN_RPC_PASSWORD:-onlydoge}" -rpcport="${DOGECOIN_RPC_PORT:-22555}" getblockchaininfo >/dev/null 2>&1 || exit 1

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["dogecoind"]
