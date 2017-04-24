import glob
import os
import time

import pandas as pd

path = r'/Users/Consultant/data/MelbDatathon2017/Transactions'
all_files = glob.glob(os.path.join(path, "patients_*.txt"))

t0 = time.time()
print('Load {} files ...'.format(len(all_files)))
df = pd.DataFrame()
l = []
for f in all_files:
    print('... {} ...'.format(f))
    df = pd.read_table(f, index_col=None, header=0)
    l.append(df)
    print('... append to list, length now {} ...'.format(len(l)))
t1 = time.time()
print('... done all in {} sec'.format(round(t1 - t0, 2)))

print('Concatenate ...')
df = pd.concat(l, ignore_index=True)
t2 = time.time()
print('... done in {} sec, df {}'.format(round(t2 - t1, 2), df.shape))
print(df.head())

print('Save df to csv ...')
df.to_csv('./data/all_trans.csv', index=False)
t3 = time.time()
print('... done in {} sec'.format(round(t3 - t2, 2)))

print('Create Store ...')
store = pd.HDFStore('./data/store.h5')
print(store)

store['all_trans'] = df
t4 = time.time()
print('... df stored in {} sec'.format(round(t4 - t3, 2)))
print(store)


# df_from_each_file = (pd.read_table(f) for f in all_files)
# print('... loaded df {} from files ...'.format(df_from_each_file))
#
# concatenated_df = pd.concat(df_from_each_file, ignore_index=True)
# print('... concatenated df {}'.format(concatenated_df.__name__))

# myinput = os.path.join(data_dir, data_file)
# X = pd.read_csv(myinput)
# print(X.head(5))
# # X = np.array(X)
#
# kmeans = KMeans(n_clusters=2)
# kmeans.fit(X)
#
# centroids = kmeans.cluster_centers_
# labels = kmeans.labels_
#
# print(centroids)
# print(labels)
