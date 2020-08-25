# -*- coding: utf-8 -*-

from sklearn.isotonic import IsotonicRegression
import pandas as pd

if __name__ == '__main__':
	t = pd.read_csv('./dense_decomposition_phase1.csv')
	t.sort_values('x', inplace=True)
	ir = IsotonicRegression()
	ir.fit(t.x.values, t.y.values)
	t['y_hat'] = ir.predict(t.x.values)
	t.to_csv('./local_dense_decomposition.csv', index=False)