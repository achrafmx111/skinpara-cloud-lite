FROM node:24-alpine@sha256:50c8e8ca1d27439048670df5883f32d57cf81cff6233222c893fd0d9884cbd81
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force
COPY server.js kapso-adapter.js direct-channel.js safe-fetch.cjs automation-engine.js campaign-engine.js whatsapp-provider.js delivery-provider.js analytics-service.js ./
COPY public ./public
USER node
EXPOSE 8790
CMD ["node", "server.js"]
