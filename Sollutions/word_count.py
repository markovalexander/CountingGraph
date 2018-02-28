from collections import Counter

import computations


def tokenizer_mapper(r):
    import re
    tokens = r['text'].split()
    for token in tokens:
        token = re.sub(r'[^\w.]', '', token)
        token = token.lower()
        if len(token) > 0 and token.isalpha():
            yield {
                'doc_id': r['doc_id'],
                'word': token
            }


def term_frequency_reducer(records):
    word_count = Counter()
    for r in records:
        word_count[r['word']] += 1
    for w, count in word_count.items():
        yield {
            # 'doc_id': r['doc_id'],
            'word': w,
            'total': count
        }


if __name__ == "__main__":
    g = computations.ComputationGraph()
    g.add_mapper(tokenizer_mapper)
    g.add_reducer(term_frequency_reducer, 'word')
    g.set_input('./data/text_corpus.txt')
    g.run()
    g.write_output('./results/word_count.txt')
