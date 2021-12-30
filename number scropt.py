# -*- coding: utf-8 -*-
"""
Created on Tue Dec 28 15:59:47 2021

@author: nugad
"""
def num1(x):
   def num2(y):
      return x * (y**2)
   return num2

res = num1(2)

print(res(5))

#reading yaml

with open(spec_path) as f:
    spec = yaml.load(f)
    
rel_meta_path = spec.get("meta", {}).get("path", "")
meta_path = os.path.join(building, rel_meta_path)

print(meta_path) 