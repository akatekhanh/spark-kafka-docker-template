from json import loads  
from kafka import KafkaConsumer  

my_consumer = KafkaConsumer(  
        'testnum',  
        bootstrap_servers = ['localhost:29092'],  
        auto_offset_reset = 'earliest',  
        enable_auto_commit = True,  
        group_id = 'my-group',  
        value_deserializer = lambda x : loads(x.decode('utf-8'))  
    )  

for message in my_consumer:  
    message = message.value  
    print(message)  