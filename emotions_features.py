# This code attaches each tweet & retweet in the db 28 emotion scores with RoBERTa fine-tuned on go-emotions dataset - batch update

import os
import torch
import transformers
from datasets import load_dataset
from transformers import (
    AutoModelForCausalLM,
    AutoTokenizer,
    BitsAndBytesConfig,
    TrainingArguments,
    pipeline,
    logging,
)
from peft import LoraConfig
from trl import SFTTrainer
import bitsandbytes
import accelerate

import mysql as mysql
from sql_connector import mydb, mycursor
import json

transformers.set_seed(123)
mycursor.execute("SELECT * FROM thesis.en_retweets where admiration is null")
tweets = mycursor.fetchall()

logging.set_verbosity(logging.CRITICAL)
classifier = pipeline(task="text-classification", model="SamLowe/roberta-base-go_emotions", top_k=None)

batch_size = 100
count = 0

for i in range(0, len(tweets), batch_size):
    batch_tweets = tweets[i:i+batch_size]
    batch_val = []

    for tweet in batch_tweets:
        tweet_dict = {}
        tweet_text = tweet[4]
        try:
            tweet_text = tweet_text.split(': ', 1)[1]
            model_outputs = classifier(tweet_text)
            sorted_data = sorted(model_outputs[0], key=lambda x: x['label'])
            sorted_dict = {item['label']: item['score'] for item in sorted_data}
            sorted_dict["id"] = tweet[0]
            print(tweet_text)
            count += 1
            print(count)

            val = (sorted_dict["admiration"], sorted_dict['amusement'], sorted_dict["anger"], sorted_dict['annoyance'],
                   sorted_dict['approval'],
                   sorted_dict["caring"], sorted_dict['confusion'], sorted_dict["curiosity"], sorted_dict['desire'],
                   sorted_dict['disappointment'],
                   sorted_dict["disapproval"], sorted_dict['disgust'], sorted_dict["embarrassment"],
                   sorted_dict['excitement'], sorted_dict['fear'],
                   sorted_dict["gratitude"], sorted_dict['grief'], sorted_dict["joy"], sorted_dict['love'],
                   sorted_dict['nervousness'],
                   sorted_dict["neutral"], sorted_dict['optimism'], sorted_dict["pride"], sorted_dict['realization'],
                   sorted_dict['relief'],
                   sorted_dict["remorse"], sorted_dict['sadness'], sorted_dict["surprise"], sorted_dict['id'])

            batch_val.append(val)

        except:
            print("error")

        
    # Construct and execute SQL queries for batch updates
    sql = (
        "UPDATE en_retweets SET admiration = %s, amusement = %s, anger = %s, annoyance = %s, approval = %s, caring = %s, "
        "confusion = %s, curiosity = %s, desire = %s, disappointment = %s, disapproval = %s, disgust = %s, embarrassment = %s, "
        "excitement = %s, fear = %s, gratitude = %s, grief = %s, joy = %s, love = %s, nervousness = %s, neutral = %s, "
        "optimism = %s, pride = %s, realization = %s, relief = %s, remorse = %s, sadness = %s, surprise = %s "
        "WHERE id = %s"
    )
    try:
        # Execute batch updates
        mycursor.executemany(sql, batch_val)
        mydb.commit()
        print("Inserted batch")
    except mysql.connector.IntegrityError as e:
        print("Error:", e)
        ignored_records = batch_val
        with open("ignored_records.txt", "a", encoding="utf-8") as file:
            file.write("\n".join(map(str, ignored_records)) + "\n")

mycursor.close()
mydb.close()