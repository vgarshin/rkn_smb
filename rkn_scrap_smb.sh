#!/bin/bash

cd `dirname "$0"`

VOLUMES="-v /home/user/extradata:/home/user/extradata"
PROC_DATE=`date +%Y%m%d`
CACHE_DIR="cache_rkn_scraping_smb"
CACHE_DIR_XML="cache_rkn_scraping_smb_xml"
DATA_DIR="_data"
EMAIL="vgarshin@yandex.ru"
CONT_NAME="parcesites_rkn"
ACTUAL_DATE="2019-02"

sudo mkdir "../$CACHE_DIR_XML"
sudo mkdir "../$CACHE_DIR"

#python rkn_scrap_smb.py _data 20190718 cache_rkn_scraping_smb vgarshin@yandex.ru 2019-02 cache_rkn_scraping_smb_xml
sudo docker run -i --name $CONT_NAME $VOLUMES extradata python -u rkn/rkn_scrap_smb.py $DATA_DIR $PROC_DATE $CACHE_DIR $EMAIL $ACTUAL_DATE $CACHE_DIR_XML
sudo docker rm -f $CONT_NAME

sudo rm -r "../$CACHE_DIR_XML"
sudo rm -r "../$CACHE_DIR"
sudo cp /home/user/extradata/_data/* /home/usersftp/chroot/