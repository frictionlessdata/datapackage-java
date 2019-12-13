#!/usr/bin/env python
import csv


def save_dict_to_csv(data, path):
    with open(path, 'w') as file:
        writer = csv.writer(file)
        print('Creating "%s" file...' % path)
        n = 1
        for line in data:
            writer.writerow(line)

            n += 1
            if n % 1000 == 0:
                print('%d lines' % n)
        print('DONE!')


def sample_table_generator(rows=10, columns=10):
    # headers
    yield ['header %d' % n for n in range(columns)]
    # values
    for row in range(1, rows):
        yield ['row %d col %d' % (row, n) for n in range(columns)]


if __name__ == "__main__":
    save_dict_to_csv(
        data=sample_table_generator(rows=5000, columns=1000),
        path='files/csv/big.csv'
    )
