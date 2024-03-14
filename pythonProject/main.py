import random
import string
import os
import threading

def generate_random_words(num_words, min_repeated_words, max_repeated_words):
    words = []
    num_repeated_words = random.randint(min_repeated_words, max_repeated_words)
    for _ in range(num_words - num_repeated_words):
        word = ''.join(random.choices(string.ascii_lowercase, k=random.randint(3, 10)))
        words.append(word)
    # palavras repetidas
    repeated_words = random.choices(words, k=num_repeated_words)
    words.extend(repeated_words)
    return words

# Função MapReduce
def map_reduce(file_name):
    with open(file_name, 'r') as file:
        words = file.read().split()
        word_counts = {}
        for word in words:
            if word in word_counts:
                word_counts[word] += 1
            else:
                word_counts[word] = 1
        repeated_words = {word: count for word, count in word_counts.items() if count > 1}
    return repeated_words

# criar o arquivo intermediário
def process_file(file_name):
    intermediate_file_name = f"{file_name}_intermediate.txt"
    repeated_words = map_reduce(file_name)
    with open(intermediate_file_name, 'w') as intermediate_file:
        for word, count in repeated_words.items():
            intermediate_file.write(f"{word} [{count}]\n")
    return intermediate_file_name

# contar todas as palavras que se repetem
def count_repeated_words_in_intermediate_files(intermediate_files):
    total_repeated_words = {}
    for file_name in intermediate_files:
        with open(file_name, 'r') as intermediate_file:
            for line in intermediate_file:
                word, count = line.strip().split(' [')
                count = int(count[:-1])
                if word in total_repeated_words:
                    total_repeated_words[word] += count
                else:
                    total_repeated_words[word] = count
    return total_repeated_words

# arquivos intermediários com threads
def process_intermediate_files_with_threads(intermediate_files):
    processed_files = set()  # arquivos já processados
    threads = []
    for file_name in intermediate_files:
        original_file = file_name.split('_intermediate')[0]  # pegando o arquivo original
        if original_file not in processed_files:  # verifica se o arquivo original já foi processado
            thread = threading.Thread(target=process_file, args=(original_file,))
            threads.append(thread)
            thread.start()
            processed_files.add(original_file)  # Adiciona o arquivo original no conjunto de arquivos processados

    for thread in threads:
        thread.join()

    #arquivo final
    final_repeated_words = count_repeated_words_in_intermediate_files(intermediate_files)
    final_file_name = "final_results.txt"
    with open(final_file_name, 'w') as final_file:
        for word, count in final_repeated_words.items():
            final_file.write(f"{word} [{count}]\n")

    print(f"Resultados salvos em '{final_file_name}'.")

# Gerar arquivos com palavras aleatórias
num_files = 5
num_words_per_file = 50000
min_repeated_words = int(num_words_per_file * 0.4)
max_repeated_words = int(num_words_per_file * 0.6)
for i in range(num_files):
    file_name = f"file_{i}.txt"
    with open(file_name, 'w') as file:
        words = generate_random_words(num_words_per_file, min_repeated_words, max_repeated_words)
        for word in words:
            file.write(word + '\n')

intermediate_files = [f"file_{i}.txt_intermediate.txt" for i in range(num_files)]
process_intermediate_files_with_threads(intermediate_files)

