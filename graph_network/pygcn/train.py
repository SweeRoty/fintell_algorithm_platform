# -*- coding: utf-8 -*-

import argparse
import time

import numpy as np
import pandas as pd
import torch
import torch.nn.functional as F
import torch.optim as optim

from gcn import GCN
from gcn_data import GCNData

def load_app_graph(node_path, edge_path):
	"""Load the APP graph using pandas IO
		node format: the 1st column should be ID; the last 2 colums should be flag indicating which set the sample belong to, and label
		edge format:
	"""
	x = pd.read_csv(node_path)
	y = x.iloc[:, -2].values.reshape((-1, ))
	phase = x.iloc[:, -1].values.reshape((-1, ))
	x = x.iloc[:, 1:-2].values

	edges = pd.read_csv(edge_path).values

	data_loader = GCNData(x, y, edges)

	idx_train = torch.LongTensor(np.where(phase == 0)[0])
	idx_eval = torch.LongTensor(np.where(phase == 1)[0])
	idx_test = torch.LongTensor(np.where(phase == 2)[0])

	return data_loader.x, data_loader.y, data_loader.adj, idx_train, idx_eval, idx_test

def accuracy(y_hat, y):
	preds = y_hat.max(1)[1].type_as(y)
	comps = preds.eq(y).double()
	comps = comps.sum()
	return comps / len(y)

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	#parser.add_argument('--seed', type=int, default=10, help='Random seed.')
	parser.add_argument('--node_path', type=str, default='../dl_data/app_nodes_2.csv', help='File path of nodes (features).')
	parser.add_argument('--edge_path', type=str, default='../dl_data/app_edges.csv', help='File path of edges.')
	parser.add_argument('--epochs', type=int, default=200, help='Number of epochs to train.')
	parser.add_argument('--lr', type=float, default=3e-2, help='Initial learning rate of Adam.')
	parser.add_argument('--weight_decay', type=float, default=1e-4, help='Weight decay (L2 loss on parameters).')
	parser.add_argument('--verbose', type=int, default=1, help='Frequency to print validation performance.')
	parser.add_argument('--is_testing', dest='mode', action='store_true', default=False, help='Output the performance on the testing set.')
	parser.add_argument('--is_saving', action='store_true', default=False, help='Save the model locally.')
	parser.add_argument('--model_path', type=str, default='./app_gcn', help='File path of the saved model.')
	args = parser.parse_args()

	# Load data
	x, y, adj, idx_train, idx_eval, idx_test = load_app_graph(args.node_path, args.edge_path)

	# Initialize model
	np.random.seed(3)
	torch.manual_seed(10)
	layers = [8, 8]
	model = GCN(n_in=x.shape[1], n_out=y.max().item()+1, n_hids=layers)

	# Train model
	optimizer = optim.Adam(model.parameters(), lr=args.lr, weight_decay=args.weight_decay)
	start_time = time.time()
	epoch_time = start_time
	for i in range(args.epochs):
		# Step: training
		model.train()
		optimizer.zero_grad()
		y_hat = model(x, adj)
		loss_train = F.nll_loss(y_hat[idx_train], y[idx_train], weight=torch.tensor([1.0, 69.0]))
		acc_train = accuracy(y_hat[idx_train], y[idx_train])
		loss_train.backward()
		optimizer.step()

		# Step: validation
		if (i%args.verbose != 0) and (i != args.epochs-1):
			continue
		else:
			model.eval()
			with torch.no_grad():
				#y_hat = model(x, adj)
				loss_eval = F.nll_loss(y_hat[idx_eval], y[idx_eval], weight=torch.tensor([1.0, 69.0]))
				acc_eval = accuracy(y_hat[idx_eval], y[idx_eval])
			curr_time = time.time()
			print('Epoch: {:04d}'.format(i+1),
					'loss_train: {:.4f}'.format(loss_train.item()),
					'acc_train: {:.4f}'.format(acc_train.item()),
					'loss_eval: {:.4f}'.format(loss_eval.item()),
					'acc_eval: {:.4f}'.format(acc_eval.item()),
					'duration: {:.4f}s'.format(curr_time-epoch_time))
			epoch_time = curr_time

	if args.mode:
		model.eval()
		with torch.no_grad():
			#y_hat = model(x, adj)
			loss_test = F.nll_loss(y_hat[idx_test], y[idx_test])
			acc_test = accuracy(y_hat[idx_test], y[idx_test])
			print("Test set results:",
					"loss={:.4f}".format(loss_test.item()),
					"accuracy={:.4f}".format(acc_test.item()))

	if args.is_saving:
		torch.save(model.state_dict(), '{}_L{}.pth'.format(args.model_path, '-'.join([str(l) for l in layers])))