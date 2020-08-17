# -*- coding: utf-8 -*-
"""
Created on Sat Aug 15 11:18:46 2020

@author: SNC6SI: Shen, Chenghao <snc6si@gmail.com>
"""

import os
import sys
from glob import glob
import pandas as pd
from gbt32960parser2tbl import gbt32960parser as gb3p


if __name__ == "__main__":
    print('preparing...')
    vins = ['LB9AB2AC8H0LDN156',
            'LB9AB2AC0H0LDN264',
            'LB9AB2AC2H0LDN136']

    directories = [r'D:\OBS',
                   r'D:\OBS_2345',
                   r'D:\OBS_6',
                   r'D:\OBS_7']

    # find files
    file_dict = {}
    for vin in vins:
        file_dict[vin] = []
        for dir_rc in directories:
            rc = os.path.join(dir_rc, '*', vin, '*.gz')
            file_rc = glob(rc)
            file_dict[vin].extend(file_rc)

    output_root_dir = r'D:\OBS_corrosion_IR_low'

    print('parsing...')
    # parse
    dfs  = {}
    for vin, file_list in file_dict.items():
        dfs[vin] = []
        for file in file_list:
            obj = gb3p(file)
            obj.conv2array()
            obj.validCheck()
            obj.dataPreProc()
            obj.dataOutput()
            dfs[vin] += [obj.df]

    # concat and write
    for vin, df_list in dfs.items():
        print('concating...')
        df2write = pd.concat(df_list, axis=0, ignore_index=True)
        df2write.sort_values(by='time', ignore_index=True, inplace=True)
        # write path
        filepath = os.path.join(output_root_dir, vin)
        if not os.path.exists(filepath):
            os.makedirs(filepath)
        file2write = os.path.join(filepath, 'data.gz')
        print('writing...')
        df2write.to_csv(file2write, compression='gzip', index=False)
