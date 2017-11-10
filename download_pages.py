import requests
import time
import codecs
import os

PAGE_LIMIT = 16750
OUTPUT_PREFIX = os.path.join('raw_data', 'math_se_')

def get(url, throttle = 2):
    time.sleep(throttle)
    print('sent request to ' + url)
    return requests.get(url)

urls = ('https://math.stackexchange.com/questions?pagesize=50&sort=newest&page=' + str(i) 
    for i in range(PAGE_LIMIT))
responses = (get(url) for url in urls)
good_responses = (resp for resp in responses if (resp.status_code == 200))

output_file_names = (OUTPUT_PREFIX + str(i) for i in range(PAGE_LIMIT))

for resp, output in zip(good_responses, output_file_names):
    with codecs.open(output, 'w', 'utf-8') as output_f:
        output_f.write(resp.text)
    print('wrote to ' + output)
