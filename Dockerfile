FROM oven/bun:1.3.14-alpine AS base
WORKDIR /app
RUN apk upgrade --no-cache
# Node runs the out-of-process ZMQ rawtx bridge; native zeromq crashes Bun in-process.
RUN apk add --no-cache nodejs npm

FROM base AS deps
COPY package.json bun.lock tsconfig.base.json biome.json vitest.config.ts ./
COPY apps/api/package.json apps/api/package.json
COPY apps/onlydoge/package.json apps/onlydoge/package.json
COPY apps/indexer/package.json apps/indexer/package.json
COPY packages/platform/package.json packages/platform/package.json
COPY packages/shared-kernel/package.json packages/shared-kernel/package.json
COPY packages/modules/access-control/package.json packages/modules/access-control/package.json
COPY packages/modules/analytics-query/package.json packages/modules/analytics-query/package.json
COPY packages/modules/explorer-query/package.json packages/modules/explorer-query/package.json
COPY packages/modules/indexing-pipeline/package.json packages/modules/indexing-pipeline/package.json
RUN bun install --frozen-lockfile
RUN find node_modules -name bun.lock -delete

FROM deps AS development
# Keep native-build tooling: local compose bind-mounts the bridge and npm-installs into a volume at start.
RUN apk add --no-cache python3 make g++ cmake linux-headers
COPY . .
RUN npm ci --omit=dev --prefix scripts/zmq-rawtx-bridge
EXPOSE 2277
CMD ["bun", "run", "--watch", "apps/onlydoge/src/index.ts", "--mode=both", "--ip=0.0.0.0", "--port=2277"]

FROM deps AS prod-deps
RUN rm -rf node_modules && bun install --frozen-lockfile --production
RUN find node_modules -name bun.lock -delete

FROM base AS production
ENV NODE_ENV=production
LABEL org.opencontainers.image.source="https://github.com/simonbetton/onlydoge-indexer"
LABEL org.opencontainers.image.title="OnlyDoge"
LABEL org.opencontainers.image.description="Dogecoin explorer backend and indexer"
COPY --from=prod-deps /app/node_modules /app/node_modules
COPY . .
RUN apk add --no-cache --virtual .zmq-build python3 make g++ cmake linux-headers \
    && npm ci --omit=dev --prefix scripts/zmq-rawtx-bridge \
    && apk del .zmq-build
EXPOSE 80
ENTRYPOINT ["bun", "run", "apps/onlydoge/src/index.ts"]
CMD ["--mode=both", "--ip=0.0.0.0", "--port=80"]
