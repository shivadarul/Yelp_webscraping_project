#!/usr/bin/env python
# coding: utf-8

# <h1>Table of Contents<span class="tocSkip"></span></h1>
# <div class="toc"><ul class="toc-item"><li><span><a href="#Loading-the-search-result-pages" data-toc-modified-id="Loading-the-search-result-pages-1"><span class="toc-item-num">1&nbsp;&nbsp;</span>Loading the search result pages</a></span></li><li><span><a href="#Getting-top-40-listings-from-each-page" data-toc-modified-id="Getting-top-40-listings-from-each-page-2"><span class="toc-item-num">2&nbsp;&nbsp;</span>Getting top 40 listings from each page</a></span></li><li><span><a href="#Scraping-information-from-each-listing" data-toc-modified-id="Scraping-information-from-each-listing-3"><span class="toc-item-num">3&nbsp;&nbsp;</span>Scraping information from each listing</a></span></li><li><span><a href="#Importing-data-into-MongoDB" data-toc-modified-id="Importing-data-into-MongoDB-4"><span class="toc-item-num">4&nbsp;&nbsp;</span>Importing data into MongoDB</a></span></li><li><span><a href="#Downloading-each-shop-page" data-toc-modified-id="Downloading-each-shop-page-5"><span class="toc-item-num">5&nbsp;&nbsp;</span>Downloading each shop page</a></span></li><li><span><a href="#Getting-shop-information" data-toc-modified-id="Getting-shop-information-6"><span class="toc-item-num">6&nbsp;&nbsp;</span>Getting shop information</a></span></li><li><span><a href="#Update-geolocation-and-shop-info-&amp;-Indexing" data-toc-modified-id="Update-geolocation-and-shop-info-&amp;-Indexing-7"><span class="toc-item-num">7&nbsp;&nbsp;</span>Update geolocation and shop info &amp; Indexing</a></span></li></ul></div>

# In[639]:


import requests
import pandas as pd
import os
import time
import random
import codecs
from bs4 import BeautifulSoup
import numpy as np
import re


# ### Loading the search result pages

# In[640]:


url_donut_listings = []

for i in page_seq:
    url_page = 'https://www.yelp.com/search?find_desc=Donut+Shop&find_loc=San+Francisco%2C+CA&start='+str(i)
    print(url_page)
    time.sleep(np.random.randint(low = 5, high = 9))
    page = requests.get(url_page)
    soup = BeautifulSoup(page.content, 'html.parser')

    file_name = 'sf_donut_shop_search_page_'+str((i/10)+1)+'.htm'
    print(file_name)
    Html_file= open(file_name,"w")
    Html_file.write(str(soup))
    Html_file.close()
    


# ### Getting top 40 listings from each page

# In[644]:


## Getting all listings 

actual_listings = []

for j in range(4):

    webpage = 'sf_donut_shop_search_page_'+str(j+1)+'.0.htm'
    # looping through search pages
    with open(webpage) as page:
        soup = BeautifulSoup(page, 'html.parser')

        # Finding all listings
        listings = soup.find_all('div' , {'class' : 'arrange-unit__09f24__rqHTg arrange-unit-fill__09f24__CUubG border-color--default__09f24__NPAKY'})
       
        for i in listings:
            if len(i.find_all('a', {'class' : 'css-1422juy', 'name': True} )) !=0:
                actual_listings.append(i)
        


# In[645]:


len(actual_listings)


# ### Scraping information from each listing

# In[647]:


counter = 0
yelp_dictionary = []
for ls in actual_listings:

    # getting review count
    reviews = ls.find_all('span', {'class': re.compile('reviewCount')})
    review_count = int(reviews[0].text)
    
    # getting ratings
    rating = ls.find_all('div', {'class': re.compile('i-stars')})
    ratings = rating[0]['aria-label']

    # URL of listing
    url_ele = ls.find_all('a', {'class' : 'css-1422juy', 'name': True} )
    url = 'https://www.yelp.com/'+str(url_ele[0]['href'])
    url

    # Name of shop
    name = url_ele[0]['name']
    name 
    
    # Price range
    try:    
        price_range = ls.find_all('span', {'class' : re.compile('priceRange')})
        price_range = price_range[0].text
    except: 
        prince_range = []
  
    # Tags
    tags = []
    for i in ls.find_all('button'):
        tags.append(i.text)

    # Dine in Tags
    delivery_tags = []
    for i in ls.find_all('span', {'class' : 'raw__09f24__T4Ezm'}):
        delivery_tags.append(i.text)

    delivery_tag_value = []
    for i in ls.find_all('svg'):
        if i.find('path', {'d': re.compile('M6')}):
            delivery_tag_value.append(1)
        elif i.find('path', {'d': re.compile('M9')}): 
            delivery_tag_value.append(0)
        else: 
            pass

    sample_dic = {  'search_rank': counter+1,
                    'name': name,
                    'url': url,
                    'review_count': review_count, 
                    'ratings': ratings,
                    'price_range': price_range,
                    'tags': tags,
                    'delivery': dict(zip(delivery_tags, delivery_tag_value))
                    }
    yelp_dictionary.append(sample_dic)
    counter+=1
    
    


# In[648]:


yelp_dictionary


# ### Importing data into MongoDB

# In[653]:


import json
from pymongo import MongoClient

# Writing data into a jason file

with open('yelp_jason.json', 'w') as fout:
    json.dump(yelp_dictionary , fout)

client = MongoClient('localhost', 27017)
db = client['yelp_db']
collection_yelp = db['yelp_collection']

with open('yelp_jason.json') as f:
    file_data = json.load(f)

# if pymongo >= 3.0 use insert_many() for inserting many documents
collection_yelp.insert_many(file_data)

# client.close()


# In[654]:


print(db.list_collection_names())


# ### Downloading each shop page

# In[659]:


query = {}
projection = {}
projection["url"] = u"$url"
projection["_id"] = 0

cursor = db.yelp_collection.find(query, projection = projection)

url_list = []

try:
    for doc in cursor:
        url_list.append(doc['url'])
        
finally:
    print('done')


# In[656]:


search_rank_list = []


query = {}
projection = {}
projection["search_rank"] = u"$search_rank"
projection["_id"] = 0

cursor = db.yelp_collection.find(query, projection = projection)

url_list = []

try:
    for doc in cursor:
        search_rank_list.append(doc['search_rank'])
        
finally:
    print('done')


# In[661]:


url_list


# In[665]:


url_donut_listings = []
counter2 = 0
for urls in url_list:
    print(urls)
    time.sleep(np.random.randint(low = 5, high = 9))
    page = requests.get(urls)
    soup = BeautifulSoup(page.content, 'html.parser')

    print(counter2)
    file_name = 'sf_donut_shop_'+str(search_rank_list[counter2])+'.htm'
    print(file_name)
    Html_file= open(file_name,"w")
    Html_file.write(str(soup))
    Html_file.close()
    counter2+=1
    


# ### Getting shop information

# In[698]:


## Getting all listings 
website_urls = []
phone_nums = []
address_list = []


for j in range(40):

    webpage = 'sf_donut_shop_'+str(j+1)+'.htm'
    with open(webpage) as page:
        soup = BeautifulSoup(page, 'html.parser')

        try :
            website_pre = soup.find('p', string = re.compile('Business website'))
            website = website_pre.find_next('p')
            website_urls.append(website.text)            
        except:
            website_urls.append('No website')

        try :
            phone = soup.find('p', string = re.compile('Phone')).find_next('p')
            phone_nums.append(phone.text)
        except:
            phone_nums.append('No phone')

        try :
            address = soup.find('p', {'class':'css-qyp8bo'})
            address_list.append(address.text)
        
        except:
            address_list.append('No address')




# ### Update geolocation and shop info & Indexing

# **Pulling lat long from positionstack api**

# In[688]:


url = "http://api.positionstack.com/v1/forward"


# In[699]:


lat_long = []

for address in address_list:
    params = {
        "access_key" : '920b3f2e71a46982161b1a15d3e56fcc',
        "query" : str(address)

    }
    api_page = requests.get(url,params = params)
    api_soup = BeautifulSoup(api_page.content, 'html.parser')
    json_obj = json.loads(str(api_soup))

    lat_long.append([json_obj['data'][0]['latitude'], json_obj['data'][0]['longitude']])


# **Create list of shoppinging information**

# In[ ]:


shop_info = []

for j in range(40):
    document = {
                'website_url' : website_urls[j],
                'phone_number' : phone_nums[j],
                'address' : address_list[j], 
                'lat_long' : lat_long[j]
                }
    shop_info.append(document)
    


# **Update mongo DB with shopping info**

# In[690]:


cursor = db.yelp_collection.find()
j = 0 
for doc in cursor:
    print(doc)


# In[693]:


cursor = db.yelp_collection.find()
j = 0 
for doc in cursor:
    db.yelp_collection.update_one(filter = {'_id':  doc['_id'] },update = {'$set': shop_info[j]} ,upsert = True)
    j+=1


# In[694]:


cursor = db.yelp_collection.find()
j = 0 
for doc in cursor:
    print(doc)


# **Creating index**

# In[623]:


db.yelp_collection.create_index('search_rank', unique = True)


# In[635]:


cursor = db.yelp_collection.find(  { "search_rank" : 3 } )
j = 0 
for doc in cursor:
    print(doc)


# **The sesrch uses IXSCAN- thus index has been created**

# In[696]:


db.yelp_collection.find( { "search_rank" : 3 } ).explain()

