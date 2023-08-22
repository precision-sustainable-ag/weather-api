FROM node:18 as builder
WORKDIR /
COPY . .
RUN npm install

WORKDIR /public/client
RUN npm install
RUN npm run build

WORKDIR /

EXPOSE 80
ENTRYPOINT npm start