FROM node:22-alpine as builder
WORKDIR /
COPY . .
RUN npm install

EXPOSE 80
ENTRYPOINT npm start