#!/bin/sh
# Write runtime gateway configuration into config.js.
# The WASM bundle reads window.CANON_GATEWAY_URL at startup (a plain string).
#
# When nginx proxies API/WebSocket routes (the default Docker setup), no
# explicit URL is needed — the frontend uses the current origin. This
# config is only written when GATEWAY_URL is explicitly set, as an escape
# hatch for non-proxied deployments.

if [ -n "${GATEWAY_URL}" ]; then
  # Sanitise: strip characters that could break out of a JS string literal
  SAFE_URL=$(printf '%s' "${GATEWAY_URL}" | sed 's/[\\"""'"'"'`]//g')
  cat > /usr/share/nginx/html/config.js <<JSEOF
window.CANON_GATEWAY_URL = "${SAFE_URL}";
JSEOF
else
  # Empty config — frontend will use current origin via nginx proxy
  cat > /usr/share/nginx/html/config.js <<JSEOF
// No explicit gateway URL configured — using current origin via nginx proxy.
JSEOF
fi
