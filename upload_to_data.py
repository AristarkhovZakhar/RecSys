from random_word import RandomWords
from tqdm import tqdm, trange

def generate_random_files():
  r = RandomWords()
  r.get_random_word()
  for i in trange(100):
    with open(f'/Users/iopmanu/RecSys/parser/backend/data/{i}.txt', 'w') as f:
      text = " ".join([r.get_random_word() for _ in range(100)])
      print(text)
      f.write(text)

generate_random_files()
