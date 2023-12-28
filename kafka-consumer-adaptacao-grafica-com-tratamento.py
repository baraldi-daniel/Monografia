#Importing libraries
from kafka import KafkaConsumer as kc
import matplotlib.pyplot as plt
import matplotlib.style as mplstyle
from ast import literal_eval
from datetime import datetime
from collections import Counter

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
plt.tight_layout()



#Initializing counter
counter=0

#Iterating over messages in consumer
for mensagem in consumidor:
    with open(r"C:\Users\baral\OneDrive\Documents\Engenharia de Dados - USP\Monografia\teste2.txt", 'a') as f:
        f.write(str(mensagem.value.decode('utf-8'))+'\n')
        f.flush()
    #Avoiding zeros and non-decoding values that generate errors
    try:
        variable_name = literal_eval(mensagem.value.decode('utf-8'))['valorA']
        variable_value = int(literal_eval(mensagem.value.decode('utf-8'))['valorB'])
    except:
        variable_value=0

    #Sorting values accordingly
    values[variable_name]=variable_value
    values_ordered=sorted(values.items(), key=lambda x:x[1],reverse=True)
    values_converted=dict(values_ordered)

    #Setting top n values
    n = 30
    counter_values = Counter(values_converted)
    top30 = dict(counter_values.most_common(n))
 


    #Cleaning and setting axis and xticks
    ax.clear()
    ax.bar(top30.keys(),height=top30.values(),color='tab:red')
    plt.xticks(rotation=90)

    #Showing the data and adjusting
    fig.canvas.draw()
    fig.canvas.flush_events()
    plt.tight_layout()


    #Registering the iteration
    print(counter)

    #Adding 1 to the counter
    counter=counter+1
    