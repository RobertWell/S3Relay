.PHONY: test run build help
MVN := $(shell command -v ./mvnw || command -v mvn)
help:
	@echo "make test  — @QuarkusTest against a real MinIO (needs one at S3RELAY_TEST_ENDPOINT, default http://localhost:9000)"
	@echo "make run   — dev mode on :8080 (point s3relay.upstream.* at a MinIO)"
	@echo "make build — package the runner jar"
test:
	$(MVN) test
run:
	$(MVN) quarkus:dev
build:
	$(MVN) -DskipTests package
