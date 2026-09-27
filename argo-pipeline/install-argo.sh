#!/usr/bin/env bash
# Install the argo CLI at the version of the Argo Workflows server of the
# cluster, so that `argo submit argo-pipeline/pipeline.yaml ...` works.
#
# Usage:
#   argo-pipeline/install-argo.sh [namespace]
#
# The version is read from the image of the workflow controller running in
# the namespace (default: projet-cartiflette). Environment variables:
#   ARGO_VERSION  force a version (e.g. v3.6.4) instead of detecting it
#   INSTALL_DIR   where to put the binary (default: ~/.local/bin)
set -euo pipefail

NAMESPACE="${1:-projet-cartiflette}"
INSTALL_DIR="${INSTALL_DIR:-$HOME/.local/bin}"

# 1. Version of the server
if [ -z "${ARGO_VERSION:-}" ]; then
  if ! command -v kubectl >/dev/null; then
    echo "kubectl not found: set ARGO_VERSION (e.g. ARGO_VERSION=v3.6.4)" >&2
    exit 1
  fi
  image=$(
    kubectl get pods -n "$NAMESPACE" \
      -o jsonpath='{range .items[*]}{range .spec.containers[*]}{.image}{"\n"}{end}{end}' \
      | grep 'workflow-controller' | sort -u | head -n 1 || true
  )
  if [ -z "$image" ]; then
    echo "No workflow controller found in namespace $NAMESPACE." >&2
    echo "Pass the namespace as argument, or set ARGO_VERSION." >&2
    exit 1
  fi
  ARGO_VERSION="${image##*:}"
  echo "Argo Workflows server: $image"
fi
if ! [[ "$ARGO_VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "Unexpected version '$ARGO_VERSION': set ARGO_VERSION (e.g. v3.6.4)" >&2
  exit 1
fi

# 2. Nothing to do if this version is already installed
if command -v argo >/dev/null \
  && argo version --short 2>/dev/null | grep -q "$ARGO_VERSION"; then
  echo "argo $ARGO_VERSION already installed: $(command -v argo)"
  exit 0
fi

# 3. Download and install
case "$(uname -m)" in
  x86_64) arch=amd64 ;;
  aarch64 | arm64) arch=arm64 ;;
  *) echo "Unsupported architecture: $(uname -m)" >&2; exit 1 ;;
esac
os=$(uname -s | tr '[:upper:]' '[:lower:]')
url="https://github.com/argoproj/argo-workflows/releases/download/${ARGO_VERSION}/argo-${os}-${arch}.gz"

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
echo "Downloading $url"
curl -fsSL "$url" -o "$tmp/argo.gz"
gunzip "$tmp/argo.gz"
chmod +x "$tmp/argo"
mkdir -p "$INSTALL_DIR"
mv "$tmp/argo" "$INSTALL_DIR/argo"

"$INSTALL_DIR/argo" version --short
case ":$PATH:" in
  *":$INSTALL_DIR:"*) ;;
  *) echo "Add it to your PATH: export PATH=\"$INSTALL_DIR:\$PATH\"" ;;
esac
