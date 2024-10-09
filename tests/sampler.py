import torch, numpy as np
import torch.utils.data as data_utils
numDataPoints = 1000
data_dim = 1
bs = 100

# Create dummy data with class imbalance 9 to 1
data = torch.FloatTensor(numDataPoints, data_dim)
target = np.hstack((np.zeros(int(numDataPoints * 0.9), dtype=np.int32), np.ones(int(numDataPoints * 0.1), dtype=np.int32)))
print(target)
print('target train 0/1: {}/{}'.format(len(np.where(target == 0)[0]), len(np.where(target == 1)[0])))

class_sample_count = np.array([len(np.where(target == t)[0]) for t in np.unique(target)])
weight = 1. / class_sample_count
print("Count", class_sample_count)
samples_weight = torch.from_numpy(np.array([weight[t] for t in target]))
print(samples_weight)
s = data_utils.WeightedRandomSampler(samples_weight, len(samples_weight))

target = torch.from_numpy(target)
train_dataset = torch.utils.data.TensorDataset(data, target)
train_loader = data_utils.DataLoader(train_dataset, batch_size=bs, sampler=s)

for data, target in train_loader:
    #print(target)
    print("batch index, 0/1: {}/{}".format(len(np.where(target.numpy() == 0)[0]),len(np.where(target.numpy() == 1)[0])))
    break