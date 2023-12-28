#Importing Libraries
import random
import string
from kafka import KafkaProducer as kp
from llama_cpp import Llama
import random

#Setting producer
produtor = kp(bootstrap_servers="192.168.126.128:9092")

#Setting counter and lists of values
counter=0
lista_valorB=[1]

#Setting LLM
llm = Llama(model_path=r"C:\Users\baral\Downloads\llama-2-13b-chat.Q4_K_M.gguf",verbose=False)

#Iterating through words using prompt to create comments
for i in range(50):
    output = llm(f"""Crie um comentário bastante curto positivo ou negativo sobre um produto que você comprou recentemente. Use os exemplos para melhorar os resultados.
                    Comentário: Eu odeio meu liquidificador.
                    Comentário: Eu amo meu microondas.
                    Comentário: Meu computador é incrível.
                    Comentário: Meu mouse de computador não funciona muito bem.
                    Comentário: Relógio muito bom. Bem melhor que o meu antigo.
                    Comentário: O fogão que comprei já está com defeito. Não recomendo não.
                    Comentário: Melhor ar condicionado que já comprei. Funciona perfeitamente.
                    Comentário: Meu celular não funciona direito. Me arrependo de ter comprado.
                    Comentário: Comprei esse modelo de fone de ouvido ontem. Hoje eu abri e veio com vários defeitos.
                    Comentário: Paguei muito caro desse freezer e não valeu a pena.
                    Comentário: Muito bom e barato esse purificador de água.
                    Comentário: Ventilador perfeito. Gostei bastante.
                    Comentário: Achei terrível esse roteador. Muito barulhento e difícil de configurar.
                    Comentário: Achava que esse lustre seria melhor. Não gostei.
                    Comentário: """, max_tokens=30, stop=["Comentário:"], stream=False,repeat_penalty=1.15)
    text=output["choices"][0]['text']
    with open(r"C:\Users\baral\OneDrive\Documents\Engenharia de Dados - USP\Monografia\frases.txt", 'a') as f:
        f.write(str(text)+'\n\n')
        f.flush()

    for i in str(text).lower().translate(str.maketrans('', '', string.punctuation)).split():
        valorA=str(i)
        valorB=random.choice(lista_valorB)
        with open(r"C:\Users\baral\OneDrive\Documents\Engenharia de Dados - USP\Monografia\teste.txt", 'a') as f:
            f.write(valorA+'\n')
            f.flush()
        string_variable=f'{{"chave":"{counter}","valorA":"{valorA}","valorB":"{valorB}"}}'
        bytes_variable=bytes(string_variable,encoding='utf-8')
        produtor.send("mensagens-produtor",key=b'Chave %d' % counter, value=bytes_variable)
        produtor.flush()
    counter=counter+1