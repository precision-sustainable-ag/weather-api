FROM node:22-alpine AS deps
WORKDIR /app
COPY package.json package-lock.json ./
# sidestep the npm@10 extraneous crash
RUN npm i -g npm@9
RUN npm ci --omit=dev --no-audit --no-fund

FROM node:22-alpine
WORKDIR /app
ENV NODE_ENV=production
COPY --from=deps /app/node_modules ./node_modules
COPY . .
EXPOSE 3000
CMD ["node", "server.js"]
