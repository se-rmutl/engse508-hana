#!/usr/bin/env python3
import sys

current_word = None
docs_tf = []
doc_count = 0

for line in sys.stdin:
    try:
        line = line.strip()
        word, doc_id, tf, count = line.split('\t')
        tf = float(tf)
        count = int(count)
        
        if current_word == word:
            docs_tf.append((doc_id, tf))
            doc_count += count
        else:
            if current_word:
                # Output word with its document frequency and TF values
                for doc_id, tf_val in docs_tf:
                    print(f"{current_word}\t{doc_id}\t{tf_val}\t{doc_count}")
            
            current_word = word
            docs_tf = [(doc_id, tf)]
            doc_count = count
    except:
        continue

if current_word:
    for doc_id, tf_val in docs_tf:
        print(f"{current_word}\t{doc_id}\t{tf_val}\t{doc_count}")