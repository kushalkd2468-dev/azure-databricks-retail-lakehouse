#!/usr/bin/env bash
# Drop generated raw files into the upstream "source" container (what ADF pulls from).
# Usage: ./scripts/upload_to_adls.sh <storage_account_name> [container]
#   container defaults to "source"; use "landing" to skip ADF and feed Databricks directly.
set -euo pipefail
ACCOUNT="${1:?usage: $0 <storage_account_name> [container]}"
CONTAINER="${2:-source}"
az storage blob upload-batch \
  --account-name "$ACCOUNT" \
  --destination "$CONTAINER" \
  --destination-path retail \
  --source data/raw \
  --auth-mode login \
  --overwrite
echo "Uploaded to ${CONTAINER}/retail on ${ACCOUNT}"
