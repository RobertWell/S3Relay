# S3Relay production image (HEL-421). Self-contained multi-stage build: CI
# runners have no Maven/JDK 21, so the build happens inside. Runtime matches the
# rest of the estate's Quarkus/Kotlin services (ubi9/openjdk-21, non-root 185).
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -B -ntp -DskipTests dependency:resolve-plugins dependency:resolve
COPY src ./src
# Tests are @QuarkusTest against a real MinIO and need Docker — they gate in CI,
# not in the image build. Skipping here is a fact about the sandbox, not a relaxed gate.
RUN mvn -B -ntp -DskipTests package

FROM registry.access.redhat.com/ubi9/openjdk-21:1.21
ENV LANGUAGE='en_US:en'
COPY --from=build --chown=185 /src/target/quarkus-app/lib/     /deployments/lib/
COPY --from=build --chown=185 /src/target/quarkus-app/*.jar    /deployments/
COPY --from=build --chown=185 /src/target/quarkus-app/app/     /deployments/app/
COPY --from=build --chown=185 /src/target/quarkus-app/quarkus/ /deployments/quarkus/
EXPOSE 8080
USER 185
ENV JAVA_OPTS_APPEND="-Dquarkus.http.host=0.0.0.0 -Djava.util.logging.manager=org.jboss.logmanager.LogManager"
ENV JAVA_APP_JAR="/deployments/quarkus-run.jar"
ENTRYPOINT [ "/opt/jboss/container/java/run/run-java.sh" ]
