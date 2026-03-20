#!/bin/sh
# Write runtime gateway configuration into config.js.
# The WASM bundle reads window.CANON_CONFIG at startup.
GATEWAY_URL="${GATEWAY_URL:-http://localhost:8080}"
WS_URL=$(echo "$GATEWAY_URL" | sed 's|^http|ws|')

cat > /usr/share/nginx/html/config.js <<JSEOF
window.CANON_CONFIG = {
  gatewayUrl: "${GATEWAY_URL}",
  wsUrl: "${WS_URL}/events"
};
JSEOF
