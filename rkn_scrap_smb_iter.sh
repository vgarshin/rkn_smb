#!/bin/bash

cd `dirname "$0"`

VOLUMES="-v /home/user/extradata:/home/user/extradata"
PROC_DATE=`date +%Y%m%d`
CACHE_DIR="cache_rkn_scraping_smb"
CACHE_DIR_XML="cache_rkn_scraping_smb_xml"
DATA_DIR="_data"
EMAIL="vgarshin@yandex.ru"
CONT_NAME="parcesites_rkn_iter"

sudo mkdir "../$CACHE_DIR_XML"
sudo mkdir "../$CACHE_DIR"

sudo docker run -i --name $CONT_NAME $VOLUMES extradata python -u rkn/rkn_scrap_smb_iter.py $DATA_DIR $PROC_DATE $CACHE_DIR $EMAIL $CACHE_DIR_XML
sudo docker rm -f $CONT_NAME

sudo rm -r "../$CACHE_DIR_XML"
sudo rm -r "../$CACHE_DIR"
sudo cp /home/user/extradata/_data/* /home/usersftp/chroot/