FROM nginx:alpine
ARG DIST_PATH=canon-demo/frontend/dist
COPY ${DIST_PATH} /usr/share/nginx/html
COPY canon-demo/frontend/nginx.conf /etc/nginx/conf.d/default.conf
COPY canon-demo/frontend/docker-entrypoint.sh /docker-entrypoint.d/40-canon-config.sh
RUN chmod +x /docker-entrypoint.d/40-canon-config.sh
EXPOSE 80
