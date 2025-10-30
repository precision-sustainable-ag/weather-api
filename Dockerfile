FROM node:20-alpine AS deps
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev

FROM node:20-alpine
# security: run as the precreated node user
USER node
WORKDIR /app
COPY --chown=node:node --from=deps /app/node_modules ./node_modules
COPY --chown=node:node . .
ENV NODE_ENV=production
EXPOSE 3000
CMD ["node", "server.js"]