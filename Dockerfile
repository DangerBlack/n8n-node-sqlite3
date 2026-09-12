# Fallback builder for platforms better-sqlite3 ships no prebuilt binding for.
#
# You should not normally need this. Since v13 better-sqlite3 is an N-API addon and
# ships a binary for every supported platform inside its npm tarball, and because
# N-API binaries are not tied to the Node ABI, the same file keeps working when n8n
# bumps its Node version. Run `npm run prebuilds` to bundle those binaries.
#
# Build a binding by hand only for an exotic platform or a locked-down image:
#
#   docker build -t n8n-sqlite3-builder .
#   docker run --rm -v "$PWD/native:/output" n8n-sqlite3-builder
#
# The binding lands in native/<platform>-<libc>-<arch>/better_sqlite3.node, which is
# where nodes/SqliteNode/shared/binding.ts looks for it. You can also point the
# N8N_SQLITE3_NATIVE_BINDING environment variable at a binding anywhere on disk.

FROM node:26-alpine

RUN apk add --no-cache python3 make g++ linux-headers sqlite-dev

WORKDIR /build

CMD set -e && \
    # Pinned to the range this package depends on: N-API keeps binaries compatible
    # across Node versions, not across better-sqlite3 major versions.
    npm install "better-sqlite3@^13.0.3" node-gyp && \
    # better-sqlite3 skips the build when it already ships a prebuild for the host,
    # so force it; this is the same thing its own "build-release" script does.
    (cd node_modules/better-sqlite3 && ../.bin/node-gyp rebuild --release --force_build=1) && \
    TARGET="$(node -e 'const l = process.report.getReport().header.glibcVersionRuntime ? "glibc" : "musl"; process.stdout.write(`${process.platform}-${l}-${process.arch}`)')" && \
    mkdir -p "/output/$TARGET" && \
    cp node_modules/better-sqlite3/build/Release/better_sqlite3.node "/output/$TARGET/" && \
    echo "Built binding for $TARGET in /output/$TARGET/better_sqlite3.node"
