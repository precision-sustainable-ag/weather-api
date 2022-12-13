FROM node:18 as builder
WORKDIR /
COPY . .
RUN npm install

EXPOSE 80
ENTRYPOINT npm start