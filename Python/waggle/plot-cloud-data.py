#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul  2 16:25:51 2024

@author: bhupendra
"""

import sage_data_client
import seaborn as sns

import matplotlib.pyplot as plt

df = sage_data_client.query(
    start="-30m", 
    filter={
        "plugin": "registry.sagecontinuum.org/bhupendraraut/cloud-motion:1.23.07.12",
        "vsn": "W096"
    }
)


df = df.set_index('timestamp')
df = df[(df['meta.seg_rank']=='1') | (df['meta.seg_rank']=='2')]

df = df[(df['name']=='cmv.mean.dir.degn') | (df['name']=='cmv.mean.mag.pxpm')]



sns.relplot(df, kind='line', x='timestamp', y='value', col='name', 
            row='meta.seg_rank', height=6, aspect=3)

plt.savefig("/Users/bhupendra/Desktop/plot.pdf")

