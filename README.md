
## Counting Graph library

Реализация вычислительного графа по [заданию](https://wiki.school.yandex.ru/shad/groups/2017/Semester1/Python/.files/hw_2_fixed.pdf)

### Поддерживаемые операции

Задать граф -- создать инстанс класса и задать последовательность операций. Граф поддерижвает следующие операции:

1. Map --- добавить операцию, вызывающую переданный ей маппер от всех строк таблицы

2. Sort --- отсортировать строки таблицы лексикографически по значениям в переданных столбцах

3. Fold --- "сворачивание" таблицы в одну строку с помощью переданной бинарной ассоциативной операции

4. Reduce --- операция, аналогичная Map, но вызывается от строк с одним значением ключа в переданных столбцах

5. Join --- слияние с таблицей-результатом другого вычислительного графа. Возможны следующие стратегии, аналогичные [стратегиям SQL](https://ru.wikipedia.org/wiki/Join_(SQL)) : left, right, inner, outer, cross.
    
Перед вычислением графа необходимо указать входной файл. Это может быть как файл на компьютере (строки --- последовательности dict-like объектов, так и результат другого вычислительного графа. В таком случае необходимо указать этот граф.

### Пример использования

Предположим, что у нас имеется *tokenizer_mapper* --- маппер, разбивающий текс на слова, и *term_frequency_reducer* --- редьюсер, подсчитывающий сколько раз каждое слово встретилась в текстах. Тогда последовательность операций
```
    import computations

    g = computations.ComputationGraph()
    g.add_mapper(tokenizer_mapper)
    g.add_reducer(term_frequency_reducer, 'word')
    g.set_input('./data/text_corpus.txt')
    g.run()
    g.write_output('./results/word_count.txt')
```

по таблице *./data/text_corpus.txt* построит таблицу *./results/word_count.txt*, в которой будут содержатся сведения о том, сколько раз каждое слово из файла *./data/text_corpus.txt* встретилось в нем.

### Автор

* Марков Александр

### Благодаронсть

* Филиппу Синицину за прекрасное задание
* Моему мастерству за такую прекрасную реализацию
