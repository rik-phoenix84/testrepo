#import requests
import os
#import shutil
#import 
import csv
#import collections
#import datetime
#import time
import sys
#from devices import devices
from variables import names
#from json import loads
#from requests.auth import HTTPBasicAuth

# Prova OUTPUT
#sys.stdout.write("HELLO!!")

filename = '/home/abbapid1/PROVE_PYTHON/TEST.csv'
sys.stdout.write(filename)
#os.path.exists(os.path.dirname(filename))

#if not os.path.exists(os.path.dirname(filename)):
#	sys.stdout.write("File does not exist")

with open(filename) as csv_file:
	csv_file = csv.reader(csv_file)


           # fp = csv.DictWriter(csv_file, od_columns.keys())
           # fp.writeheader()
"""	
if os.path.isfile(filename):
            with open(filename) as csv_file, open('data/' + start_date + '_to_' + end_date  + '/' + topic[0] + '.csv', 'w') as out:
                csv_reader = csv.reader(csv_file)
				
for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file))
"""
"""
def main():
    start_date = '2017-05-15'
    end_date = '2017-05-22'
    limit = 1000
    offset = 0
    topics = [[x.split(' ')[0],'ari/' + x.split(' ')[1]] for x in devices]
    columns = {}
    
    # setup progress bar
    toolbar_width = 50
    progress_10_int = 0
    sys.stdout.write("[%s]" % (" " * toolbar_width))
    sys.stdout.flush()
    sys.stdout.write("\b" * (toolbar_width+1)) # return to start of line, after '['
    totalState=len(topics)

    for index, topic in enumerate(topics):
        filename = 'data/' + start_date + '_to_' + end_date  + '/' + topic[0] + 'notordered.csv'
        params = {'topic' : topic[1] + '/ar1/Log/log', 'limit' : limit, 'offset' : offset} 
        res = requests.get('https://api.everyware-cloud.com/v2/metrics/searchByTopic', auth=HTTPBasicAuth('davidegentile', 'gientpid9@G@G'), params=params)
        result = loads(res.text)
        
        columns['Timestamp'] = ''
        columns['Status'] = ''
        columns['FIdx'] = ''
        columns['Mult'] = ''
        columns['Own'] = ''        

        last_progress_10_int=progress_10_int
        progress=((index+1)*100/totalState*100)/100
        # Minium % progress (progress/%)
        progress_10_int=int(progress/2)
        step=progress_10_int-last_progress_10_int

        for i in range(0,step):
            # update the bar
            sys.stdout.write("=")
            sys.stdout.flush()

        if not os.path.exists(os.path.dirname(filename)):
            try:
                os.makedirs(os.path.dirname(filename))
            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    raise

        if 'metricInfo' in result:
            for column in result['metricInfo']:
                columns[names[column['name']]] = ''
        
        params = {'topic' : topic[1] + '/ar1/Err/ErrListRst', 'limit' : limit, 'offset' : offset}  
        res = requests.get('https://api.everyware-cloud.com/v2/metrics/searchByTopic', auth=HTTPBasicAuth('davidegentile', 'gientpid9@G@G'), params=params)
        result = loads(res.text)
    
        if 'metricInfo' in result:
            for column in result['metricInfo']:
                if column['name'] != 'T':
                    columns[column['name']] = ''

        od_columns = collections.OrderedDict(reversed(sorted(columns.items())))
        params = {'topic' : topic[1] + '/ar1/Log/log', 'limit' : limit, 'offset' : offset}
        params['startDate'] = start_date
        params['endDate'] = end_date
        res = requests.get('https://api.everyware-cloud.com/v2/messages/searchByTopic', auth=HTTPBasicAuth('davidegentile', 'gientpid9@G@G'), params=params)
        result = loads(res.text)

        with open(filename, 'wb') as csv_file:
            fp = csv.DictWriter(csv_file, od_columns.keys())
            fp.writeheader()

        if 'message' in result:
            messages_sent_on = [x['payload']['sentOn'] for x in [x for x in result['message']]]
            messages = [x['payload']['metrics']['metric'] for x  in [x for x in result['message']]]

            l1 = []
            for k, message in enumerate(messages):
                metrics = {}
                metrics['Timestamp'] = messages_sent_on[k]
                if type(message) is list:
                    for m in message:
                            metrics[names[m['name']]] = m['value']
                elif type(message) is dict:
                    metrics[names[message['name']]] = message['value']
                else:
                    pass
                 
                row = od_columns.copy()
                row.update(metrics)
                l1.append(row)

            with open(filename, 'a') as csv_file:
                fp = csv.DictWriter(csv_file, od_columns.keys())
                fp.writerows(l1)

        params = {'topic' : topic[1] + '/ar1/Err/DevErr', 'limit' : limit, 'offset' : offset, 'startDate' : start_date, 'endDate' : end_date} 
        res = requests.get('https://api.everyware-cloud.com/v2/messages/searchByTopic', auth=HTTPBasicAuth('davidegentile', 'gientpid9@G@G'), params=params)
        result = loads(res.text)
            
        if 'message' in result:
            errors_sent_on = [x['payload']['sentOn'] for x in [x for x in result['message']]]
            errors = [x['payload']['metrics']['metric'] for x in [x for x in result['message']]]
                
            l2 = []
            for k, error in enumerate(errors):
                ers = {}
                ers['Timestamp'] = errors_sent_on[k]
                for e in error:
                    if e['name'] == 'FIdx':
                        ers['FIdx'] = e['value']
                    elif e['name'] == 'Stat':
                        ers['Status'] = e['value']
                    elif e['name'] == 'Mult':
                        ers['Mult'] = e['value']
                    elif e['name'] == 'Own':
                        ers['Own'] = e['value']
                    else:
                        pass
                
                row = od_columns.copy()
                row.update(ers)
                l2.append(row)
    
            with open(filename, 'a') as csv_file:
                fp = csv.DictWriter(csv_file, od_columns.keys())
                fp.writerows(l2)
        
        params = {'topic' : topic[1] + '/ar1/Err/ErrListRst', 'limit' : limit, 'offset' : offset, 'startDate' : start_date, 'endDate' : end_date}
        res = requests.get('https://api.everyware-cloud.com/v2/messages/searchByTopic', auth=HTTPBasicAuth('davidegentile', 'gientpid9@G@G'), params=params)
        result = loads(res.text)

        if 'message' in result:
            err_list_reset_sent_on = [x['payload']['sentOn'] for x in [x for x in result['message']]]
            error_list_reset = [x['payload']['metrics']['metric'] for x in [x for x in result['message']]]

            l3 = []
            for k, error in enumerate(error_list_reset):
                try:
                    # Manage case with message ErrListRst with N=0 and no error on boot on bus
                    if error_list_reset[k]['name'] == 'N' and error_list_reset[k]['value'] == '0':
                        pass
                except:
                    ers = {}
                    ers['Status'] = 'true'
                    ers['Timestamp'] = err_list_reset_sent_on[k]
                    for e in error:
                        if e['name'] != 'T':
                            ers[e['name']] = e['value']

                    row = od_columns.copy()
                    row.update(ers)
                    l3.append(row)
            
            with open(filename, 'a') as csv_file:
                fp = csv.DictWriter(csv_file, od_columns.keys())
                fp.writerows(l3)

        if os.path.isfile(filename):
            with open(filename) as csv_file, open('data/' + start_date + '_to_' + end_date  + '/' + topic[0] + '.csv', 'w') as out:
                csv_reader = csv.reader(csv_file)
                header = next(csv_reader, None)
                csv_writer = csv.writer(out)
                if header:
                    csv_writer.writerow(header)
                # 2017-01-30T12:17:30.592Z
                #try:
                csv_writer.writerows(sorted(csv_reader, key=lambda x:datetime.datetime.strptime(x[0][:19], '%Y-%m-%dT%H:%M:%S')))
                #except ValueError:
                    #csv_writer.writerows(sorted(csv_reader, key=lambda x:datetime.datetime.strptime(x[0], '%Y-%m-%dT%H:%M:%SZ')))

            os.remove(filename)

        zipf = zipfile.ZipFile(os.path.dirname(filename) + '.zip', 'w', zipfile.ZIP_DEFLATED)
        zipdir(os.path.dirname(filename), zipf)
        zipf.close()

    shutil.rmtree('data/' + start_date + '_to_' + end_date)
    
    # \n for the progress bar
    sys.stdout.write("\n EXTRACTION COMPLETE \n")


def zipdir(path, ziph):
    # ziph is zipfile handle
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file))

main()
"""
