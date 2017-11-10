# Web Scraping Tutorial
This is sample code demonstrating some basic techniques.
1. download_pages.py : pulls down raw html pages from math.stackexchange.com
2. multi_process_page_parse.py : produces a small .csv from each page pulled down.
3. concat_parsed_pages.py : combines these small .csv files together, taking care to eliminate overlapping entries. 

The code depends on the BeautifulSoup library and html5lib.  

Remember to be polite when web-scraping.
