#!/bin/bash

apt-get update -y
apt install npm nodejs -y
apt-get install libmagic-dev -y

git clone https://github.com/mbloch/mapshaper.git --single-branch
cd mapshaper
git checkout ec6e7a40bc875ee6d113045e5ee86f47abf57bc7
yes | npm install       # install dependencies
yes yes | npm run build     # bundle source code files
npm link          # (optional) add global symlinks so scripts are available systemwide
