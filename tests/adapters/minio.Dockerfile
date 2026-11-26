# Build the last community release from immutable upstream source. Its published
# Docker Hub/Quay images and binary download endpoints have been withdrawn.
# Source: RELEASE.2025-10-15T17-29-55Z (9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a).
FROM golang:1.27.4-alpine AS build
RUN apk add --no-cache git
WORKDIR /src
ADD https://codeload.github.com/minio/minio/tar.gz/9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a /tmp/minio.tar.gz
RUN tar -xzf /tmp/minio.tar.gz --strip-components=1 -C /src \
    && CGO_ENABLED=0 go build -trimpath -o /minio .

FROM alpine:3.24
RUN apk add --no-cache ca-certificates
COPY --from=build /minio /usr/bin/minio
ENTRYPOINT ["/usr/bin/minio"]
