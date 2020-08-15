import os
import sys
import multiprocessing as mp
from gbt32960parser2tbl import gbt32960parser as gb3p

def func(src_des_paar):
    file2parse,file2write = src_des_paar
    if not os.path.exists(file2write):
        obj = gb3p(file2parse)
        obj.conv2array()
        obj.validCheck()
        obj.dataPreProc()
        obj.dataOutput()

        desPath = os.path.split(file2write)[0]
        if not os.path.exists(desPath):
            os.makedirs(desPath)
        print(file2write)
        print('\n')
        obj.df.to_csv(file2write, index=False, compression='gzip')


if __name__ == "__main__":

    m = '202007'
    src_des_paar_param = ['/home/snc6si/OBS_7/data'+m, '/home/snc6si/OBS_7p/data'+m]

    #with open('/home/snc6si/remaining_0616.txt', 'rt') as f:
    #    remaining_file = f.read().split('\n')

    src_des_list_paar = []
    for root_,dir_,file_ in os.walk(src_des_paar_param[0]):
        for fileitem in file_:
            if os.path.splitext(fileitem)[-1]=='.gz':
                srcFileRC = os.path.normpath(os.path.join(root_, fileitem))
                #if '/'.join(srcFileRC.split('/')[-3:]) in remaining_file:
                desFileRC = srcFileRC.replace(src_des_paar_param[0],src_des_paar_param[1])
                src_des_list_paar.append([srcFileRC, desFileRC])


    pool = mp.Pool(4)
    for item in src_des_list_paar:
        try:
            pool.apply_async(func, args=(item,))
        except:
            print('error' + item[0])

    pool.close()
    pool.join()
