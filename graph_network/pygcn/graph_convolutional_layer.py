# -*- coding: utf-8 -*-

import math

from torch.nn.modules.module import Module
from torch.nn.parameter import Parameter
import torch
import torch.nn.init as init

class GraphConvolutionalLayer(Module):

	def __init__(self, in_features, out_features, use_bias=True):
		super(GraphConvolutionalLayer, self).__init__()
		self.in_features = in_features
		self.out_features = out_features
		self.w = Parameter(torch.FloatTensor(in_features, out_features))
		if use_bias:
			self.bias = Parameter(torch.FloatTensor(out_features))
		else:
			self.register_parameter('bias', None)
		self.reset_parameters()

	def reset_parameters(self):
		init.xavier_uniform_(self.w, init.calculate_gain('relu'))
		if self.bias is not None:
			std = 1. / math.sqrt(self.bias.size(0))
			self.bias.data.uniform_(-std, std)

	def forward(self, x, adj):
		h = torch.mm(x, self.w)
		h = torch.spmm(adj, h)
		if self.bias is not None:
			return h+self.bias
		else:
			return h

	def __repr__(self):
		return self.__class__.__name__ + ' (' \
				+ str(self.in_features) + ' -> ' \
				+ str(self.out_features) + ')'