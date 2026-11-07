# syntax=docker/dockerfile:1.7
# Holodeck B2B, an open source AS4 message service handler, as the partner of a local test exchange.
# A test stand, not an image for production. Holodeck publishes no image of its own; this one is built from the
# release distribution, pinned by version and by the SHA-256 of the archive (the digest GitHub lists for the asset).
# Holodeck B2B is GPLv3: the module only talks to it over HTTP, nothing of it is linked.

FROM alpine:3.20 AS dist
ARG HB2B_VERSION=8.1.1
ADD --checksum=sha256:a63355fc02dab653102f47769ecf877d6619779f9dfca6bff3539b42b3fabc02 \
    https://github.com/holodeck-b2b/Holodeck-B2B/releases/download/v${HB2B_VERSION}/holodeckb2b-distribution-${HB2B_VERSION}.zip \
    /tmp/holodeckb2b.zip
RUN unzip -q /tmp/holodeckb2b.zip -d /tmp \
 && mv /tmp/holodeckb2b-${HB2B_VERSION} /opt/holodeckb2b \
 && chmod +x /opt/holodeckb2b/bin/*.sh

FROM eclipse-temurin:17-jre
COPY --from=dist /opt/holodeckb2b /opt/holodeckb2b
ENV HB2B_HOME=/opt/holodeckb2b
WORKDIR /opt/holodeckb2b
EXPOSE 8080
CMD ["/opt/holodeckb2b/bin/startServer.sh"]
