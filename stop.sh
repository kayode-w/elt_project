# docker compose down -v

# sleep 5

# cd airbyte

# docker compose down 


# #!/usr/bin/env bash
# set -euo pipefail

# # Stop Airbyte first
# cd airbyte
# docker compose down || true
# cd ..

# # Then stop your ELT stack
# docker compose down

#!/usr/bin/env bash
set -euo pipefail

# 1) Kill the Airbyte UI port-forward (does NOT uninstall the cluster)
PF="$HOME/.airbyte/abctl/pf_ingress.pid"
if [ -f "$PF" ]; then
  PID=$(cat "$PF" || true)
  [ -n "${PID:-}" ] && kill "$PID" 2>/dev/null || true
  rm -f "$PF"
  echo "Closed Airbyte UI port-forward."
fi

# 2) Stop your Compose services
docker compose down

echo "Done. (Airbyte is still running in kind. To remove it entirely: abctl local uninstall)"