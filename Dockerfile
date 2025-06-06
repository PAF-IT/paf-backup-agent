FROM node:lts-alpine

COPY . .
RUN npm install --production

CMD ["node", "backup.js"]