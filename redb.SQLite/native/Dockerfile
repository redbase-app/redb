# Toolchain image for the REDB SQLite loadable extension: native x64 + arm64 cross-compile.
#
# Why an image at all: the recipe used to be `docker run --rm debian:12` + apt-get install on every
# build, so each rebuild re-downloaded ~190 MB of toolchain and the container took it away again.
# Built once, this image makes a rebuild take seconds and work offline — which also means a release
# does not depend on a Debian mirror answering at that moment.
#
#   docker build -t redb-sqlite-native native/          # once (or after changing this file)
#
# crossbuild-essential-arm64, not a bare gcc-aarch64-linux-gnu: the cross compiler alone has no
# arm64 C runtime and fails at link with "cannot find Scrt1.o / crti.o".
FROM debian:12

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        crossbuild-essential-arm64 \
        ca-certificates \
        curl \
        unzip \
        file && \
    rm -rf /var/lib/apt/lists/*

# The sqlite3 CLI of the verification step, built from the SAME amalgamation the extension compiles
# against (keep in step with SQLITE_AMALGAMATION_URL in CMakeLists.txt). Debian 12 ships 3.40.1, and
# the extension refuses to load below 3.44 (it needs sqlite3_get_clientdata), so the packaged CLI
# cannot run the check at all. Building it here also keeps the check offline.
ARG SQLITE_AMALGAMATION_URL=https://www.sqlite.org/2024/sqlite-amalgamation-3460100.zip
RUN curl -sSL "$SQLITE_AMALGAMATION_URL" -o /tmp/sqlite.zip && \
    unzip -q /tmp/sqlite.zip -d /tmp && \
    cd /tmp/sqlite-amalgamation-* && \
    gcc -O2 -DSQLITE_ENABLE_LOAD_EXTENSION=1 shell.c sqlite3.c -o /usr/local/bin/sqlite3 -ldl -lm -lpthread && \
    rm -rf /tmp/sqlite.zip /tmp/sqlite-amalgamation-* && \
    sqlite3 --version

WORKDIR /src
