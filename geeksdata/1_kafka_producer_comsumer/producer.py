from kafka import KafkaProducer 
from json import dumps
from time import sleep

my_producer = KafkaProducer(  
    bootstrap_servers = ['localhost:29092'],  
    value_serializer = lambda x:dumps(x).encode('utf-8')  
)  

for index in range(500):  
    my_data = {'number' : index}  
    my_producer.send('testnum', value = my_data)  
    sleep(5)  
    print(f"Sent data: {index}")