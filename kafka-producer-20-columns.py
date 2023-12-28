#Importing Libraries
import random
import string
from kafka import KafkaProducer as kp
import time

#Setting producer
produtor = kp(bootstrap_servers="192.168.126.128:9092")

#Setting counter and lists of values
counter=0
lista_valorA=['Freezer','Ar Condicionado','Microondas','Computador','Ventilador','Fone de Ouvido','Celular','Carregador de Celular','Lustre','Relógio',
              'Chuveiro Elétrico','Aspirador','Roteador','Fogão','Teclado de Computador','Liquidificador','Purificador de Água','Mouse de Computador',
              'Carregador de Computador','Filtro de Linha']
lista_valorB=[1]

#Iterating through words
for i in range(500):
    text = random.choice(lista_valorA)
    for i in str(text).lower().translate(str.maketrans('', '', string.punctuation)).split('\n'):
        valorA=str(i)

        valorB=random.choice(lista_valorB)
        with open(r"C:\Users\baral\OneDrive\Documents\Engenharia de Dados - USP\Monografia\teste.txt", 'a') as f:
            f.write(valorA+'\n')
            f.flush()
        string_variable=f'{{"chave":"{counter}","valorA":"{valorA}","valorB":"{valorB}"}}'
        bytes_variable=bytes(string_variable,encoding='utf-8')
        produtor.send("mensagens-produtor",key=b'Chave %d' % counter, value=bytes_variable)
        produtor.flush()
        time.sleep(0.5)
    counter=counter+1
