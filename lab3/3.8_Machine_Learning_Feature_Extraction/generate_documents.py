import random

topics = {
    'technology': ['computer', 'software', 'programming', 'algorithm', 'data', 'network', 'system', 'digital'],
    'science': ['research', 'experiment', 'hypothesis', 'theory', 'analysis', 'discovery', 'study', 'method'],
    'business': ['market', 'profit', 'customer', 'strategy', 'company', 'revenue', 'growth', 'investment'],
    'sports': ['game', 'team', 'player', 'score', 'match', 'competition', 'tournament', 'champion']
}

documents = []
for doc_id in range(1000):
    topic = random.choice(list(topics.keys()))
    words = topics[topic] + ['the', 'and', 'in', 'of', 'to', 'a', 'is', 'for', 'with']
    
    doc_length = random.randint(50, 200)
    doc_words = [random.choice(words) for _ in range(doc_length)]
    
    document = ' '.join(doc_words)
    documents.append(f"doc_{doc_id}\t{topic}\t{document}")

with open('documents.txt', 'w') as f:
    for doc in documents:
        f.write(doc + '\n')