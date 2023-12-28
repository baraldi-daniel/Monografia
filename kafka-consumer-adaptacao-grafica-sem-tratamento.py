#Importing libraries
from kafka import KafkaConsumer as kc
import matplotlib.pyplot as plt
import matplotlib.style as mplstyle
from ast import literal_eval
from datetime import datetime

#Setting Apache Kafka consumer
consumidor = kc("mensagens-consumidor",bootstrap_servers="192.168.126.128:9092",group_id="consumidores",auto_offset_reset='earliest')

#Initializing variables for chart
values = {}
x = []
y = values.values()

#Setting matplotlib to better performance
mplstyle.use('fast')

#Setting interactive mode in matplotlib
plt.ion()


#Setting figures, axis and chart type in matplotlib
fig = plt.figure()
ax = fig.add_subplot()




#Drawing figure and sending events
fig.canvas.draw()
fig.canvas.flush_events()



#Initializing counter
counter=0

#Iterating over messages in consumer
for mensagem in consumidor:

    #Comitting 
    #consumidor.commit()

    

    #Avoiding zeros and non-decoding values that generate errors
    try:
        variable_name = literal_eval(mensagem.value.decode('utf-8'))['valorA']
        variable_value = int(literal_eval(mensagem.value.decode('utf-8'))['valorB'])
    except:
        variable_value=0

    #Creating values accordingly
    values[variable_name]=variable_value

    
    #Cleaning and setting axis and xticks
    ax.clear()
    ax.bar(values.keys(),height=values.values(),color='tab:red')

    


    #Registering the iteration
    print(counter)

    #Adding 1 to the counter
    counter=counter+1
    total=sum(values.values())
    print(total)
    
    with open(r"C:\Users\baral\OneDrive\Documents\Engenharia de Dados - USP\Monografia\teste2.txt", 'a') as f:
        f.write(str(mensagem.value.decode('utf-8'))+'\n')
        f.flush()



    #Showing the data and adjusting
    fig.canvas.draw()
    fig.canvas.flush_events()



    
    