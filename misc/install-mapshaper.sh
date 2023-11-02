sudo apt-get update -y
sudo apt install npm nodejs -y
sudo apt-get install libmagic-dev -y

git clone https://github.com/mbloch/mapshaper.git --single-branch
cd mapshaper
yes | npm install       # install dependencies
yes yes | npm run build     # bundle source code files
sudo npm link          # (optional) add global symlinks so scripts are available systemwide