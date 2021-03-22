from setuptools import setup
from setuptools import find_packages

setup(name='pygcn',
      version='0.1',
      description='Graph Convolutional Networks in PyTorch',
      author='Thomas Kipf / Geyang Han',
      author_email='sweeroty@gmail.com',
      #url='https://tkipf.github.io',
      #download_url='https://github.com/tkipf/pygcn',
      license='MIT',
      install_requires=['numpy',
                        'pandas'，
                        'scipy',
                        'torch'
                        ],
      #package_data={'pygcn': ['README.md']},
      packages=find_packages())