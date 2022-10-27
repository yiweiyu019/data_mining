import csv
aa = {'a':[1,2,3],'b':[4,5,6]}
b = [[1,2,3],[4,5,6]]

with open('try.csv','w') as f:
    wrt = csv.DictWriter(f,fieldnames=list(aa.keys()))
    wrt.writeheader()
    for i in b:
        wrt.writerow(i)


