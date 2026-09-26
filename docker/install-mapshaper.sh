#!/bin/bash
set -e

apt-get update -y
apt-get install -y --no-install-recommends nodejs npm
rm -rf /var/lib/apt/lists/*

# Pinned: same version as in the CI (.github/workflows/check.yml)
npm install -g mapshaper@0.6.59
npm cache clean --force
mapshaper -v
