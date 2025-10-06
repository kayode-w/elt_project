# docker compose up init airflow

# sleep 5

# docker compose up -d

# sleep 5

# cd airbyte

# if [-f docker-compose.yaml]; then
#   docker compose up -d
# else
#   ./run-ab-platform.sh
# fi

# #!/usr/bin/env bash
# set -euo pipefail

# # Start your ELT stack (Airflow, Postgres, etc.)
# docker compose up -d

# # Start Airbyte (from the cloned folder)
# cd airbyte
# if [ -f docker-compose.yaml ]; then
#   docker compose up -d
# else
#   echo "Airbyte compose files missing. Fetching..."
#   curl -fSL https://raw.githubusercontent.com/airbytehq/airbyte-platform/main/compose.yaml -o docker-compose.yaml
#   curl -fSL https://raw.githubusercontent.com/airbytehq/airbyte-platform/main/.env -o .env
#   docker compose up -d
# fi

# echo "Open Airbyte at: http://localhost:8000"


#!/usr/bin/env bash
set -euo pipefail

# 0) Clean env (avoid proxy breaking kubectl) + point kubectl
unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy ALL_PROXY all_proxy || true
export NO_PROXY="localhost,127.0.0.1,::1,.local,.svc,.cluster.local"
export KUBECONFIG="$HOME/.airbyte/abctl/abctl.kubeconfig"

# 1) Start your ELT stack (Postgres, etc.)
docker compose up -d

# 2) Ensure Airbyte-on-kind is running (safe to re-run)
abctl local install --port 8010 --no-browser

# 3) Wait for the server to be ready
kubectl -n airbyte-abctl rollout status deploy/airbyte-abctl-server -w

# 4) Port-forward Ingress (8010 in cluster → 8010 on your laptop)
#    Runs in background; PID saved so stop.sh can kill it.
kubectl -n ingress-nginx port-forward svc/ingress-nginx-controller 8010:8010 \
  --address=127.0.0.1 >"$HOME/.airbyte/abctl/pf_ingress.log" 2>&1 &
echo $! >"$HOME/.airbyte/abctl/pf_ingress.pid"

# 5) Print URL + creds
echo "Airbyte UI: http://localhost:8010"
abctl local credentials || true