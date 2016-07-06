FROM node:0.12
RUN npm install debug express prom-client rhea yargs
COPY recv.js /usr/sbin/recv.js
EXPOSE 9090
CMD ["node", "/usr/sbin/recv.js"]
