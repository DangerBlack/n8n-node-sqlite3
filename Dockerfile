FROM node:24-alpine

RUN apk add --no-cache python3 make g++ linux-headers sqlite-dev

WORKDIR /app

# Expect your package to be mounted at /app
# Install ONLY better-sqlite3 from source
CMD npm install better-sqlite3 --build-from-source && \
    mkdir -p /output && \
    # Copy the built native module (adjust path if needed)
    cp node_modules/better-sqlite3/build/Release/better_sqlite3.node /output/ && \
    echo "Build complete. Native module is in /output/better_sqlite3.node"