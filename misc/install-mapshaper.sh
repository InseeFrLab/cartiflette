sudo apt-get update -y
sudo apt install npm nodejs -y

git clone https://github.com/mbloch/mapshaper.git
cd mapshaper
npm install       # install dependencies
npm run build     # bundle source code files
sudo npm link          # (optional) add global symlinks so scripts are available systemwide