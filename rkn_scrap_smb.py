# coding: utf-8
import re
import sys
import json
import os
import gc
import socket
import requests
import zipfile
import pandas as pd
import smtplib as smtp
import xml.etree.ElementTree as ET
from urllib.request import Request, urlopen, URLError, HTTPError
from bs4 import BeautifulSoup
from random import randint
from time import sleep
from pandas.io.json import json_normalize

USER_AGENT = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 YaBrowser/19.6.1.153 Yowser/2.5 Safari/537.36'
URL_ZIP = 'http://rkn.gov.ru/opendata/7705846236-OperatorsPD/data-20190712T0000-structure-20180129T0000.zip'
MIN_TIME_SLEEP = 1
MAX_TIME_SLEEP = 5
MAX_COUNTS = 5
TIMEOUT = 10

def get_dataframe(directory):
    files = [os.path.join(directory, file) for file in os.listdir(directory)]
    print('found {} files, creating dataframe...'.format(len(files)))
    df = pd.DataFrame()
    for file_load in files:
        with open(file_load) as file:
            data_json = json.load(file)
        if data_json:
            df = df.append(json_normalize(data_json, sep='_'))
    df = df.reset_index()
    del df['index']
    return df
def load_unpack_xml_file(url, path):
    print('loading zip...')
    flag = False
    errors = {}
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        print('response: ', response)
        name_zip = '{}{}'.format(path, 'temp.zip')
        with open(name_zip, 'wb') as file:
            file.write(response.content)
        print('zip loaded, unpacking...')
        with zipfile.ZipFile(name_zip, 'r') as zip_ref:
            zip_ref.extractall(path)
        flag = True
    except BaseException as e:
        errors.update({'error': e})
    return errors, flag
def get_html(url_page, timeout):
    counts = 0
    html = None
    while counts < MAX_COUNTS:
        try:
            request = Request(url_page)
            request.add_header('User-Agent', USER_AGENT)
            html = urlopen(request, timeout=timeout)
            break
        except URLError as e:
            counts += 1
            print('URLError | ', url_page, ' | ', e, ' | counts: ', counts)
            sleep(randint(MIN_TIME_SLEEP, MAX_TIME_SLEEP))
        except HTTPError as e:
            counts += 1
            print('HTTPError | ', url_page, ' | ', e, ' | counts: ', counts)
            sleep(randint(MIN_TIME_SLEEP, MAX_TIME_SLEEP))
        except socket.timeout as e:
            counts += 1
            print('socket timeout | ', url_page, ' | ', e, ' | counts: ', counts)
            sleep(randint(MIN_TIME_SLEEP, MAX_TIME_SLEEP))
    return html
def send_mail(dest_email, email_text):
    error = []
    try:
        email = 'app.notifications@yandex.ru'
        password = 'AppNotify2019'
        subject = 'Data load notification'
        message = 'From: {}\nTo: {}\nSubject: {}\n\n{}'.format(email, dest_email, subject, email_text)
        server = smtp.SMTP_SSL('smtp.yandex.com')
        server.login(email, password)
        server.auth_plain()
        server.sendmail(email, dest_email, message)
        server.quit()
    except smtp.SMTPException as e:
        error.append(e)
    return error
def get_data_pd_operator_num(url_main, pd_operator_num):
    dict_data_pd = {}
    url_i = '{}{}'.format(url_main, pd_operator_num)
    html_i = get_html(url_i, TIMEOUT)
    if html_i:
        soup_i = BeautifulSoup(html_i, 'html.parser')
        table_i = soup_i.find('table', {'class': 'TblList'})
        for row in table_i.find_all('tr'):
            try:
                cols = row.find_all('td')
                cols = [' '.join(x.text.split()) for x in cols]
                dict_data_pd.update({cols[0]: cols[1]})
            except:
                print('no correct data: ', pd_operator_num)
    else:
        print('bad response for ', pd_operator_num)
    return dict_data_pd
def main():
    #---base input parameters---
    print('url zip file: ', URL_ZIP)
    url_main = 'http://pd.rkn.gov.ru/operators-registry/operators-list/?id='
    print('url: ', url_main)
    path = '{}/'.format(sys.argv[1]) 
    print('got path to save data: ', path)
    table_name =  '{}rkn_scraping_smb_{}.csv'.format(path, str(sys.argv[2]))
    print('got date: ', str(sys.argv[2]), ' | table name: ', table_name)
    cache_path =  '{}/'.format(sys.argv[3])
    print('got directory for cache: ', cache_path)
    dest_email = sys.argv[4] 
    print('got email for notifications: ', dest_email)
    cache_path_xml = '{}/'.format(sys.argv[5])
    print('got directory for zip, xml cache: ', cache_path_xml)
    #---load zip file, unpack to xml, parce xml tree---
    flag = False
    while not flag:
        errors, flag = load_unpack_xml_file(URL_ZIP, cache_path_xml)
        print('errors: ', errors, ' | flag: ', flag)
        sleep(randint(MIN_TIME_SLEEP, MAX_TIME_SLEEP))
    xml_files =  [x for x in os.listdir(cache_path_xml) if '.xml' in x]
    print('xml files loaded: ', xml_files)
    #---main part---
    count_trial = 0
    flag = True
    while flag:
        try:
	        print('main iterparse, trial: ', count_trial)
	        count_records = 0
	        count_no_enter_date = 0
	        count_errors = 0
	        count_addresses = 0
	        data_list = []
	        dict_temp = {}
	        tree_iterator = ET.iterparse('{}{}'.format(cache_path_xml, xml_files[0]), events=('start', 'end'))
	        for event, elem in tree_iterator:  
	            if event == 'start':
	                tag = re.sub(r'\{.*?\}', '', elem.tag)
	                text = elem.text
	                if tag == 'record':
	                    try:
	                        if dict_temp['enter_date']:
	                            #---get extra data---
	                            pd_operator_num = dict_temp['pd_operator_num']
	                            dict_data_pd = get_data_pd_operator_num(url_main, pd_operator_num)
	                            dict_temp.update(dict_data_pd)
	                            data_list.append(dict_temp)
	                            #---save to file---
	                            if dict_temp['номера их контактных телефонов, почтовые адреса и адреса электронной почты']:
	                                filename = '{}batch_reestr_num_{}.txt'.format(cache_path, pd_operator_num)
	                                with open(filename, 'w') as file:
	                                    json.dump(dict_temp, file)
	                                count_addresses += 1
	                            #---delay for next url---
	                            sleep(randint(MIN_TIME_SLEEP, MAX_TIME_SLEEP))
	                        else:
	                            count_no_enter_date += 1
	                            pass
	                    except BaseException as e:
	                        print('BaseException iter cycle | ', e)
	                        print('error dictionary: ', dict_temp)
	                        count_errors += 1
	                        pass
	                    dict_temp = {}
	                    count_records += 1
	                else:
	                    dict_temp.update({tag: text})
	            elem.clear()
	            #---test limit: comment for prod---
	            #if count_records == 50:
	            #    break
	        print('total records: ', count_records)
	        print('records with no enter date: ', count_no_enter_date)
	        print('errors: ', count_errors)
	        print('records with address: ', count_addresses)
	        flag = False
        except BaseException as e:
	        print('BaseException main cycle | ', e)
	        count_trial += 1
	        sleep(randint(MIN_TIME_SLEEP, MAX_TIME_SLEEP))
	        flag = True
    print('data collected, saved to json files to folder: {}'.format(cache_path))
    #---collect dataframe and write to csv---
    df = get_dataframe(cache_path)
    print('data frame created of shape: ', df.shape)
    df.to_csv(table_name, sep='\t')
    print('saved to file: ', table_name)
    #---notification via email----
    email_text = 'VTB Ya.Cloud: Data collected, table {} created'.format(table_name)
    error_mail = send_mail(dest_email, email_text)
    if error_mail:
        print('email was not sent to: {} | error: {}'.format(dest_email, error_mail))
    else:
        print('email was sent to: ', dest_email)

if __name__ == '__main__':
    main()
