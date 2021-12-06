#!/usr/bin/env python
# coding: utf-8

# In[1]:


pip install kafka-python


# In[1]:


from time import sleep
from json import dumps
from kafka import KafkaProducer


# In[4]:


producer = KafkaProducer(bootstrap_servers=['kafka'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))


# In[ ]:


for e in range(1000):
    data = {'number' : e}
    producer.send('numtest', value=data)
    sleep(5)

