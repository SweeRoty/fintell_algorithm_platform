# -*- coding: utf-8 -*-

import numpy as np
import scipy.sparse as sp
import torch

class GCNData():

	def __init__(self, x, y, edges, is_undirected=True):
		"""
			edge format: a n*3 numpy array with columns representing fr, to and weight between fr and to
		"""
		self.y = torch.LongTensor(np.where(self.one_hot_encode(y))[1])

		x = sp.csr_matrix(x, dtype=np.float32)
		self.x = torch.FloatTensor(np.array(self.normalize(x).todense()))

		n = x.shape[0]
		adj = sp.coo_matrix((edges[:, 2], (edges[:, 0], edges[:, 1])), shape=(n, n), dtype=np.float32)
		if is_undirected:
			adj = adj + adj.T.multiply(adj.T > adj) - adj.multiply(adj.T > adj)
		adj = self.normalize(adj+sp.eye(adj.shape[0]))
		self.adj = self.to_torch_sparse_tensor(adj)
		print('-----Data has been processed')
	
	def one_hot_encode(self, labels):
		"""One-hot encode categorical labels
		"""
		classes = set(labels)
		class_dict = {c:i for i, c in enumerate(classes)}
		label_indices = np.array(list(map(class_dict.get, labels)), dtype=np.int32)
		encoded_labels = np.zeros((len(label_indices), label_indices.max()+1))
		encoded_labels[np.arange(len(label_indices)), label_indices] = 1
		return encoded_labels

	def normalize(self, mat):
		"""Row-normalize sparse matrix
		"""
		row_sum = np.array(mat.sum(1))
		row_sum_inv = np.power(row_sum, -1).flatten()
		row_sum_inv[np.isinf(row_sum_inv)] = 0.
		diag = sp.diags(row_sum_inv)
		mat = diag.dot(mat)
		return mat

	def to_torch_sparse_tensor(self, sparse_mat):
		"""Convert a scipy sparse matrix to a torch sparse tensor
		"""
		sparse_mat = sparse_mat.tocoo().astype(np.float32)
		indices = torch.from_numpy(np.vstack((sparse_mat.row, sparse_mat.col)).astype(np.int64))
		values = torch.from_numpy(sparse_mat.data)
		shape = torch.Size(sparse_mat.shape)
		return torch.sparse.FloatTensor(indices, values, shape)	