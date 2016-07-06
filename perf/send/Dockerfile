FROM node:0.12
RUN npm install debug express prom-client rhea yargs
COPY send.js /usr/sbin/send.js
EXPOSE 9090
CMD ["node", "/usr/sbin/send.js"]
