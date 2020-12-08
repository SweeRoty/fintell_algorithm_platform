# -*- coding: utf-8 -*-

import torch.nn as nn
import torch.nn.functional as F

from graph_convolutional_layer import GraphConvolutionalLayer

class GCN(nn.Module):

	def __init__(self, n_in, n_out, n_hids, dropout=0.25):
		super(GCN, self).__init__()

		self.layers = []
		n_hids.insert(0, n_in)
		n_hids.append(n_out)
		for i in range(len(n_hids)-1):
			self.layers.append(GraphConvolutionalLayer(n_hids[i], n_hids[i+1]))
		self.layers = nn.ModuleList(self.layers)
		self.dropout = dropout

	def forward(self, x, adj):
		for i, layer in enumerate(self.layers):
			x = F.relu(layer(x, adj))
			if i < len(self.layers)-1:
				x = F.dropout(x, self.dropout, training=self.training)
		return F.log_softmax(x, dim=1)

	def __repr__(self):
		return self.__class__.__name__ + ' [' \
     			+ ' connects with '.join([str(layer) for layer in self.layers]) + ']'