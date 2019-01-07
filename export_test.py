import requests, os, sys, pathlib, zipfile
import pandas as pd
from io import BytesIO
from time import sleep

token = input('Token: ')
bearer_token = input('Bearer Token: ')
headers = {'Authorization': 'Bearer {}'.format(bearer_token)}
api_secret = input('API Secret: ')

def grouper (iterable, n):
    iterable = iter(iterable)
    count = 0
    group = []
    while True:
        try:
            group.append(next(iterable))
            count += 1
            if count % n == 0:
                yield group
                group = []
        except StopIteration:
            yield group
            break

groups =  list(grouper(distinct_ids, 100))
print(len(groups))

id_url = 'https://mixpanel.com/api/app/data-retrievals/v2.0/?token={}'.format(token)

csv = input('CSV: ')
data = pd.read_csv(csv)
distinct_ids = data['$distinct_id']
tasks = []

def get_tasks():
    counter = 0
    while counter < len(groups):
        try:
            for i in range(len(groups[counter:])):
                for id in groups[i]:
                    payload = {'distinct_id': '{}'.format(id)}
                    r = requests.post(id_url, json=payload, headers=headers).json()
                    tasks.append(r['results']['task_id'])


                with open('tasks.csv', "w") as outfile:
                    for id in tasks:
                        outfile.write(id)
                        outfile.write("\n")
                    counter += 1
                    print(counter)
                    sleep(1)
        except:
            print('get tasks error')
            continue
        break

get_tasks()

status = pd.read_csv('tasks.csv',index_col=False, header=None)
status = status[status.columns[0]]
status_urls = []

status =  list(grouper(status, 100))
print(len(status))

def get_url():
    counter = 0
    while counter < len(status):
        try:
            for i in range(len(status[counter:])):
                for task in status[i]:
                    r = requests.get('https://mixpanel.com/api/app/data-retrievals/v2.0/{}/?token={}'.format(task,token), headers=headers).json()
                    print(r)
                    if r['results']['status'] == 'SUCCESS':
                        status_urls.append(r['results']['result'])

                with open('status_url.csv', "w") as outfile:
                    for url in status_urls:
                        outfile.write(url)
                        outfile.write("\n")
                    counter += 1
                    print(counter)
                    sleep(1)
        except:
            print('error')
            continue
        break

get_url()

def extract_zips(dir_name, csv):
    pathlib.Path(dir_name).mkdir(parents=True, exist_ok=True)
    urls = pd.read_csv(csv,index_col=False, header=None)
    urls = urls[urls.columns[0]]

    for url in urls:
        r = requests.get(url)
        z = zipfile.ZipFile(BytesIO(r.content))
        z.extractall(path=os.getcwd() + '/{}'.format(dir_name),pwd=bytes(api_secret, encoding= 'utf-8'))

dir_name = input('Directory Name: ')
extract_zips('{}'.format(dir_name), 'status_url.csv')

def zipfolder(foldername, target_dir):
    zipobj = zipfile.ZipFile(foldername + '.zip', 'w', zipfile.ZIP_DEFLATED)
    rootlen = len(target_dir) + 1
    for base, dirs, files in os.walk(target_dir):
        for file in files:
            fn = os.path.join(base, file)
            zipobj.write(fn, fn[rootlen:])

zip_name = input('Zip Folder Name')
zipfolder('{}'.format(zip_name), os.getcwd() + '/{}'.format(dir_name))
sys.exit()
