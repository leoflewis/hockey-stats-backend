import pandas 

df = pandas.read_csv('22-23-24-data.csv', header=0, index_col=[0])  
print(df)