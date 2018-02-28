import re
from collections import Counter
from math import log

import computations


def tokenizer_mapper(line):
    tokens = line['text'].split()
    for token in tokens:
        word = re.search('([a-zA-Z])+', token)
        if word and len(word.group()) > 0:
            yield {
                'doc_id': line['doc_id'],
                'word': word.group().lower()
            }


def string_counter_folder(_, state):
    for column in state:
        state[column] += 1
    return state


def unique_columns_reducer(records):
    yield {
        'doc_id': records[0]['doc_id'],
        'word': records[0]['word']
    }


def docs_contain_word_reducer(records):
    yield {
        'word': records[0]['word'],
        'docs_contain_word': len(records),
        'docs_count': records[0]['docs_count']
    }


def term_frequency_reducer(records):
    word_count = Counter()
    for record in records:
        word_count[record['word']] += 1

    total = sum(word_count.values())
    for word, count in word_count.items():
        yield {
            'doc_id': record['doc_id'],
            'word': word,
            'tf': count / total
        }


def invert_reducer(records):
    for i, record in enumerate(records):
        records[i]['tf-idf'] = record['tf'] * \
                               log(record['docs_count'] / record[
                                   'docs_contain_word'])
    records = sorted(records, reverse=True, key=lambda i: i['tf-idf'])
    top = [tuple([record['doc_id'], record['tf-idf']])
           for record in records[:3]]
    yield {
        'term': records[0]['word'],
        'index': top
    }


def solve_problem():
    """
    Sollution to tf-idf problem as it is written in task file
    :return:
    """
    split_word = computations.ComputationGraph()
    count_docs = computations.ComputationGraph()
    count_idf = computations.ComputationGraph()
    calc_index = computations.ComputationGraph()

    split_word.add_mapper(tokenizer_mapper)

    count_docs.add_folder(string_counter_folder, {'docs_count': 0})

    count_idf.set_input(split_word)
    count_idf.add_reducer(unique_columns_reducer, ('doc_id', 'word'))
    count_idf.add_join((count_docs, './data/text_corpus.txt'), (),
                       strategy='outer')
    count_idf.add_reducer(docs_contain_word_reducer, 'word')

    calc_index.add_reducer(term_frequency_reducer, 'doc_id')
    calc_index.add_join((count_idf, split_word), ('word',), strategy='left')

    calc_index.add_reducer(invert_reducer, 'word')

    split_word.set_input('./data/text_corpus.txt')
    count_docs.set_input('./data/text_corpus.txt')
    calc_index.set_input(split_word)

    calc_index.run()
    calc_index.write_output("./results/tf-idf.txt")


if __name__ == "__main__":
    solve_problem()
