FROM node:0.12
RUN npm install debug && npm install rhea
COPY configured.js /usr/sbin/configured.js
EXPOSE 55672
CMD ["node", "/usr/sbin/configured.js"]