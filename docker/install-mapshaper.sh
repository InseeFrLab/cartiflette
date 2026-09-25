#!/bin/bash
set -e

apt-get update -y
apt-get install -y npm nodejs

# Same version as the one previously built from the pinned commit ec6e7a4
npm install -g mapshaper@0.6.59
mapshaper -v
