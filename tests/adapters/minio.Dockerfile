# Build the last community release from immutable upstream source. Its published
# Docker Hub/Quay images and binary download endpoints have been withdrawn.
FROM golang:1.26.6-alpine AS build
RUN apk add --no-cache git
WORKDIR /src
ADD https://codeload.github.com/minio/minio/tar.gz/01ce918d8279a20e4706b96a64396146894adee4 /tmp/minio.tar.gz
RUN tar -xzf /tmp/minio.tar.gz --strip-components=1 -C /src \
    && CGO_ENABLED=0 go build -trimpath -o /minio .

FROM alpine:3.22
RUN apk add --no-cache ca-certificates
COPY --from=build /minio /usr/bin/minio
ENTRYPOINT ["/usr/bin/minio"]
