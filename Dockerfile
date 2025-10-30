FROM node:20-alpine AS deps
WORKDIR /app
COPY package.json package-lock.json ./
# Show versions to be sure what we're running
RUN node -v && npm -v
# Use the lockfile exactly
RUN npm ci --omit=dev --no-audit --no-fund

FROM node:20-alpine
WORKDIR /app
ENV NODE_ENV=production
COPY --from=deps /app/node_modules ./node_modules
COPY . .
EXPOSE 3000
CMD ["node", "server.js"]
