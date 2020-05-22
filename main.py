import time
import ctypes

from multiprocessing import Process, Manager
from multiprocessing.sharedctypes import Array

ARQUIVO = "BASEPROJETO.txt"

PESOS_CPF_PRIMEIRO_DIGITO = [10, 9, 8, 7, 6, 5, 4, 3, 2]
PESOS_CPF_SEGUNDO_DIGITO = [11, 10, 9, 8, 7, 6, 5, 4, 3, 2]
PESOS_CNPJ_PRIMEIRO_DIGITO = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
PESOS_CNPJ_SEGUNDO_DIGITO = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]

NUM_PROCESSOS = 4

def processa_cpf_cnpj(dado, primeiro_peso, segundo_peso):
    soma_primeiro_digito = 0
    for algarismo, peso in zip(dado, primeiro_peso):
        soma_primeiro_digito += int(algarismo) * peso

    if soma_primeiro_digito % 11 < 2:
        primeiro_digito = 0
    else:
        primeiro_digito = 11 - (soma_primeiro_digito % 11)

    dado = dado + str(primeiro_digito)

    soma_segundo_digito = 0
    for algarismo, peso in zip(dado, segundo_peso):
        soma_segundo_digito += int(algarismo) * peso

    if soma_segundo_digito % 11 < 2:
        segundo_digito = 0
    else:
        segundo_digito = 11 - (soma_segundo_digito % 11)

    dado = dado + str(segundo_digito)
    return dado


def processa_dado(dados, thread_number, cpf_completo, cnpj_completo):
    slice_por_thread = int(len(dados)/NUM_PROCESSOS)
    inicio = int(thread_number * slice_por_thread)
    for dado in dados[inicio:inicio + slice_por_thread]:
        if len(dado) == 9:
            cpf_completo.append(processa_cpf_cnpj(dado, PESOS_CPF_PRIMEIRO_DIGITO,
                                                PESOS_CPF_SEGUNDO_DIGITO))
            # print(f'CPF: {dado}')
        elif len(dado) == 12:
            cnpj_completo.append(processa_cpf_cnpj(dado, PESOS_CNPJ_PRIMEIRO_DIGITO,
                            PESOS_CNPJ_SEGUNDO_DIGITO))
            # print(f'CNPJ {dado}')
        else:
            print(f'{dado} não tem a quantidade correta de algarismos')


def get_conteudo_arquivo():
    with open(ARQUIVO, 'r') as arq:
        cadastros_pessoas = [line.strip(' ').strip('\n')
                             for line in arq.readlines()]

    return cadastros_pessoas


def gera_arquivo_completo(cpf_completo, cnpj_completo):
    with open('output.txt', 'w') as arq:
        for cpf in cpf_completo:
            arq.write(f'{cpf}\n')
            
        for cnpj in cnpj_completo:
            arq.write(f'{cnpj}\n')


def main():  
    start = time.time()
    print("********* Inicia o processo *********")
    print("Coletando dados do arquivo de texto")
    dados = get_conteudo_arquivo()
    
    print("Iniciando calculo dos digitos verificadores")
    with Manager() as manager:
        cpf_completo = manager.list()
        cnpj_completo = manager.list()

        processes = []
        for i in range(NUM_PROCESSOS):
            p = Process(target=processa_dado, args=(dados, i, cpf_completo, cnpj_completo))  # Passing the list
            p.start()
            processes.append(p)
        for p in processes:
            p.join()
            
        fim_processamento = (time.time() - start)
        print(f'Tempo cálculo da base de dados: {fim_processamento:.3f} segundos')
        
        cpf_completo = list(cpf_completo)
        cnpj_completo = list(cnpj_completo)

    print("Iniciando geração do arquivo final")
    gera_arquivo_completo(cpf_completo, cnpj_completo)
    fim_geracao = (time.time() - start - fim_processamento)
    print(f'Tempo geração do arquivo: {fim_geracao:.3f} segundos')
    
    fim_tudo = (time.time() - start)
    print("********* Fim do processo *********")
    print(f'Tempo de execução total: {fim_tudo:.3f} segundos')

if __name__ == "__main__":
    main()
