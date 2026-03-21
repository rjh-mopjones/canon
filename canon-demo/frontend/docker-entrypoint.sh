#!/bin/sh
# Write runtime gateway configuration into config.js.
# Gateway URL is empty — nginx proxies all API/WS routes to the gateway.
# The frontend uses the current origin (window.location).

cat > /usr/share/nginx/html/config.js <<JSEOF
// Gateway URL is empty — nginx proxies all API/WS routes
window.CANON_GATEWAY_URL = "";
JSEOF
