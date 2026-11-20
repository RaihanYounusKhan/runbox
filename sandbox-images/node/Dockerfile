FROM node:22-alpine
# npm/corepack aren't usable without network; remove them to shrink attack surface.
RUN rm -rf /usr/local/lib/node_modules/npm /usr/local/lib/node_modules/corepack \
           /usr/local/bin/npm /usr/local/bin/npx /usr/local/bin/corepack
USER 65534:65534
WORKDIR /tmp
