# Used to clean and analyse the data-set
import pandas as pd
df = pd.read_excel('A3.xlsx', sheet_name='Sales Report')
print(df.head())
