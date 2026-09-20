FROM node:24-alpine@sha256:50c8e8ca1d27439048670df5883f32d57cf81cff6233222c893fd0d9884cbd81
WORKDIR /app
COPY server.js safe-fetch.cjs catalog-rag.cjs chatwoot-webhook-security.cjs direct-channel.cjs ./
RUN mkdir -p /app/logs && chown -R node:node /app
USER node
EXPOSE 8787
CMD ["node", "server.js"]
