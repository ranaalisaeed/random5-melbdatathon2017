import os
import pandas as pd
import time


store = pd.HDFStore('./data/store.h5')
print(store)

t0 = time.time()
print('Loading main dataframe ...')
df1 = store['df1']
df1.set_index('Store_ID', inplace=True)
t1 = time.time()
print('... loaded in {} sec'.format(round(t1 - t0, 2)))


print('Loading stores lookup ...')
path = r'/Users/Consultant/data/MelbDatathon2017/Lookups'
f2 = os.path.join(path, "stores.txt")
df2 = pd.read_table(f2, index_col=None, header=0)
df2.set_index('Store_ID', inplace=True)
t2 = time.time()
print('... loaded in {} sec'.format(round(t2 - t1, 2)))

print('Joining ...')
df = df1.join(df2, how='left')
#df = pd.merge(left=df1, right=df2, how='outer', on='Store_ID')
t3 = time.time()
print('... merged df {} in {} sec'.format(df.shape, round(t3 - t2, 2)))


print(df.head())
store['trans_with_stores'] = df
store.close()
df.to_csv('./data/trans_with_stores', index=False)
