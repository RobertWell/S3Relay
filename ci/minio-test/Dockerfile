# The MinIO server the s3relay-tests CI job runs as its `minio` service (.gitlab-ci.yml).
#
# Why this exists: MinIO stopped publishing container images around 2026-09-30 (Docker Hub
# minio/minio is gone, quay is 401, dl.min.io is 410). GitHub Releases still carry the server
# binary with a published SHA-256. This is the HEL-614 recipe (pkgrovekit MinioTestImage.kt),
# kept here so the service image CI pulls can be reviewed and rebuilt from this repo.
#
# Build and publish (only when changing VERSION or SHA256):
#   DOCKER_BUILDKIT=0 docker build -t roguerzzz123/minio-test:<VERSION> ci/minio-test
#   docker push roguerzzz123/minio-test:<VERSION>
#   docker buildx imagetools inspect roguerzzz123/minio-test:<VERSION>   # pin THIS digest in .gitlab-ci.yml
# Check a published digest: the binary inside must hash to SHA256 below:
#   docker run --rm --entrypoint sha256sum roguerzzz123/minio-test:<VERSION>@<digest> /usr/bin/minio
#
# Published 2026-10-25: RELEASE.2025-09-07T16-13-09Z
#   -> sha256:7ddc0698071c25ef8be60e0c031969699aa95c373137b93f82409aeb64f5b3c0 (binary checked).
FROM alpine:3.20
ARG VERSION=RELEASE.2025-09-07T16-13-09Z
# The release's published minio.linux-amd64.<VERSION>.sha256sum; the build fails on any mismatch.
ARG SHA256=7c5bd8512c6e966455b1d198209358b2d191c77a83ab377c4073281065fb855f
ADD https://github.com/minio/minio/releases/download/${VERSION}/minio.linux-amd64.${VERSION} /usr/bin/minio
RUN echo "${SHA256}  /usr/bin/minio" | sha256sum -c - && chmod 0755 /usr/bin/minio && mkdir -p /data
EXPOSE 9000 9001
ENTRYPOINT ["/usr/bin/minio"]
CMD ["server", "/data"]
