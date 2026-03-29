FROM nginx:1.27-alpine

COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY index.html /usr/share/nginx/html/
COPY style/ /usr/share/nginx/html/style/
COPY js/ /usr/share/nginx/html/js/

EXPOSE 80
