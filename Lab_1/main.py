import random
import csv
import statistics
from concurrent.futures import ProcessPoolExecutor

def distribution(filename): #считавние и распределение из файла
    category_a = []
    category_b = []
    category_c = []
    category_d = []
    with open(filename, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            if row[0] == 'A':
                category_a.append(float(row[1]))
            elif row[0] == 'B':
                category_b.append(float(row[1]))
            elif row[0] == 'C':
                category_c.append(float(row[1]))
            elif row[0] == 'D':
                category_d.append(float(row[1]))
    return filename, category_a, category_b, category_c, category_d

def median_change(file): #вычисление медианы и стандартного отклонения файла
    filename, category_a, category_b, category_c, category_d = file
    file_num = filename.split('.')[0]
    categories = (('A', category_a),('B', category_b),('C', category_c),('D', category_d))
    res = []
    for name, val in categories:
        if len(val) == 0:
            median, change = 0, 0
        elif len(val) == 1:
            median, change = val[0], 0
        else:
            median = statistics.median(val)
            change = statistics.stdev(val)
        res.append((name, median, change))
    return file_num, res

def med_med_ch_ch(categories): #вычисление медианы медиан и стандартного отклонения медин
    name, val = categories
    if len(val) == 0:
        return name, 0, 0
    elif len(val) == 1:
        return name, val[0], 0
    else:
        median = statistics.median(val)
        change = statistics.stdev(val)
        return name, median, change

def main():
    categories1 = ['A', 'B', 'C', 'D']
    for i in range(5):
        rows = []
        for j in range(10):
            m = random.uniform(1.0, 1000.0)
            c = random.choice(categories1)
            row = [c, m]
            rows.append(row)
        with open(f'{i + 1}.csv', 'w+', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(rows)
    filenames = ["1.csv", "2.csv", "3.csv", "4.csv", "5.csv"]

    with ProcessPoolExecutor(max_workers=5) as executor:
        res1 = list(executor.map(distribution, filenames))
    with ProcessPoolExecutor(max_workers=5) as executor:
        res2 = list(executor.map(median_change, res1))

    medians_a = []
    medians_b = []
    medians_c = []
    medians_d = []

    for num, category_res in res2:
        print(' ')
        print(f'Медианы и стандартное отклонение файла {num}:')
        for name, median, change in category_res:
            print(f'{name}, {median}, {change};')
            if name == 'A':
                medians_a.append(median)
            elif name == 'B':
                medians_b.append(median)
            elif name == 'C':
                medians_c.append(median)
            elif name == 'D':
                medians_d.append(median)
    categories2 = [('A', medians_a), ('B', medians_b), ('C', medians_c), ('D', medians_d)]

    with ProcessPoolExecutor(max_workers=4) as executor:
        res3 = list(executor.map(med_med_ch_ch, categories2))

    print(' ')
    print("Медиан медиан и стандартное отклонение медиан: ")
    for name, median, change in res3:
        print(f'{name},{median},{change}')

if __name__ == '__main__': 
    main()
