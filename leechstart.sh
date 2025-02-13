#!/bin/bash
cd $(dirname $0)
#sudo docker ps -aq | xargs sudo docker stop | xargs sudo docker rm
sudo apt install python3 python3-pip
echo yes | pip3 install -r requirements-cli.txt
echo yes | pip3 install -r requirements.txt
#cp config_sample.env config.env
echo yes | sudo docker container prune
#echo yes | sudo docker image prune -a
sudo dockerd
sudo docker build . -t aleech
sudo docker run -p 61:61 aleech
