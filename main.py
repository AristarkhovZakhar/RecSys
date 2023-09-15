from loader.DataExtractor import DataExtractor
from loader.DataProcessor import DataProcessor
import pandas as pd
import torch
from models import BertDataset, Roberta
from transformers import AutoTokenizer, AdamW
from torch.utils.data import DataLoader, random_split
from torch.nn.utils.rnn import pad_sequence
from sklearn.preprocessing import MultiLabelBinarizer
import os

os.environ["TOKENIZERS_PARALLELISM"] = "false"


MAX_LEN = 200
TRAIN_BATCH_SIZE = 64
VALID_BATCH_SIZE = 64
EPOCHS = 10
LEARNING_RATE = 2e-5
TRAIN_TEST_COEF = 0.8


def binarizer(row) -> pd.Series:
    mlb = MultiLabelBinarizer()
    one_hot = mlb.fit_transform([row['tags']])
    return pd.Series(one_hot[0], index=mlb.classes_)


if __name__ == "__main__":
    path = ["csv_files/ridus.csv"]
    list_of_frame = [pd.read_csv(file) for file in path]
    top_tags, max_tags = 3, 1
    processor = DataProcessor(list_of_frame)
    data = processor(['text'], 'tags', 'without')
    de = DataExtractor(data, max_tags, top_tags)
    processed_df = de()
    processed_df['text'] = processed_df['text'].apply(lambda x: ' '.join(x))
    print(processed_df.iloc[5]['text'])

    target_columns = de.popular_tags
    one_hot_tags = processed_df.apply(binarizer, axis=1)
    one_hot_tags = one_hot_tags.fillna(0)
    print(pd.concat([processed_df, one_hot_tags]).fillna(0).columns)

    tokenizer = AutoTokenizer.from_pretrained('DeepPavlov/rubert-base-cased')
    dataset = BertDataset(processed_df['text'], one_hot_tags, tokenizer, MAX_LEN)

    train_size = int(TRAIN_TEST_COEF * len(dataset))
    test_size = len(dataset) - train_size

    train_dataset, test_dataset = random_split(dataset, [train_size, test_size])

    train_loader = DataLoader(train_dataset, batch_size=TRAIN_BATCH_SIZE,
                              num_workers=4, shuffle=True, pin_memory=True)
    test_loader = DataLoader(train_dataset, batch_size=VALID_BATCH_SIZE,
                             num_workers=4, shuffle=True, pin_memory=True)

    model = Roberta(max_tags, len(target_columns), EPOCHS)
    optimizer = AdamW(params=model.parameters(), lr=LEARNING_RATE, weight_decay=1e-6)

    # model.train_model(train_loader, optimizer)
    # model.scoring(test_loader, 0.5)
    # model.save('model.bin')
