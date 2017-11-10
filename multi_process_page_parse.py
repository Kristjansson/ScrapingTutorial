from bs4 import BeautifulSoup
import codecs
import re
import csv
import time

import traceback
import sys
from multiprocessing import Process

def get_tags(tags_div):
    return list(tag_link.text for tag_link in tags_div.find_all('a'))

def get_user_name(user_details_div):
    user_name_link = user_details_div.find('a')
    if (user_name_link):
        return user_name_link.text.strip()
    return user_details_div.text.strip()

def parse_page(page_id):
    with codecs.open('raw_data/math_se_' + str(page_id), 'r', 'utf-8') as raw_page_file:
        raw_page = BeautifulSoup(raw_page_file, 'html5lib')

    votes = (int(vote_div.find('strong').text) 
        for vote_div in raw_page.find_all('div', 'votes'))

    answers = (int(ans_div.find('strong').text) 
        for ans_div in raw_page.find_all('div', 'status'))

    views = (re.search('\d+', view_div.text).group(0) 
        for view_div in raw_page.find_all('div', 'views'))

    titles = (title_div.find('a').text 
        for title_div in raw_page.find_all('div', 'summary'))

    tags = (get_tags(summary_div.find('div', 'tags')) 
        for summary_div in raw_page.find_all('div', 'summary'))

    user_names = (get_user_name(user_details_div) 
        for user_details_div in raw_page.find_all('div', 'user-details'))

    times = (time_div.find('span').attrs['title']
        for time_div in raw_page.find_all('div', 'user-action-time'))

    with codecs.open("parsed/out_%07d.csv"%(page_id), 'w', 'utf-8') as csv_out_file:
        writer = csv.writer(csv_out_file)
        writer.writerow(['vote', 'answer', 'view', 'title', 'tag', 'user_name', 'time'])
        for fields in zip(votes, answers, views, titles, tags, user_names, times):
            writer.writerow(fields)


def parse_subset(start, end, step, start_time=False):
    for raw_itr in range(start, end, step):
        try:
            parse_page(raw_itr)
            if (start_time):
                print('done: ', page_id)
                print('Elapsed: ', time.time() - start_time)
                
        except (Exception):
            with open('errors/error_' + str(raw_itr) + '.txt', 'w') as error_out:
                error_out.write(traceback.format_exc())


if __name__ == '__main__':
    num_thread = int(sys.argv[1]) if len(sys.argv) > 1 else 8
    start = 0
    num_rec = 16720

    start_time = time.time()
    
    leader_proc = Process(target=parse_subset, args=(start, num_rec, num_thread, start_time))
    procs = ([leader_proc] + 
             [Process(target=parse_subset, args=(start + proc_itr, num_rec, num_thread)) for proc_itr in range(1, num_thread)])
    for proc in procs:
        proc.start()
    for proc in procs:
        proc.join()

    print('Elapsed: ', time.time() - start_time)