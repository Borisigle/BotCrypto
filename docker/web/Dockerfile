# syntax=docker/dockerfile:1
FROM node:18-alpine AS runtime

WORKDIR /app

RUN apk add --no-cache curl

COPY web/package*.json ./
RUN npm install --omit=dev

COPY web/ ./

EXPOSE 3000

CMD ["npm", "start"]
