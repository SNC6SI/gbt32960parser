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
    mlist = ['201809', '201810', '201811', '201812',
             '201901', '201902', '201903', '201904',
             '201905', '201906', '201907', '201908',
             '201909', '201910', '201911', '201912',
             '202001']
    for m in mlist:
        src_des_paar_param = [r'D:\OBS\data'+m, r'D:\83_OBS\data'+m]
    
        remainingFileInfo = os.path.join(os.path.split(sys.argv[0])[0],'remaining_file.txt')
        with open(remainingFileInfo, 'rt') as f:
            remainingFile = set(f.read().split('\n'))
        

        src_des_list_paar = []
        for root_,dir_,file_ in os.walk(src_des_paar_param[0]):
            for fileitem in file_:
                if os.path.splitext(fileitem)[-1]=='.gz':
                    srcFileRC = os.path.normpath(os.path.join(root_, fileitem))
                    if('\\'.join(srcFileRC.split('\\')[-3:]) in remainingFile):
                        desFileRC = srcFileRC.replace(src_des_paar_param[0],src_des_paar_param[1])
                        src_des_list_paar.append([srcFileRC, desFileRC])
        
        usableCPU = int(mp.cpu_count()*0.75)
        pool = mp.Pool(usableCPU)
        for item in src_des_list_paar:
            try:
                pool.apply_async(func, args=(item,))
            except:
                print('error' + item[0])
                
        pool.close()
        pool.join()