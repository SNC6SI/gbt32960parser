import os
import multiprocessing as mp
import numpy as np
import pandas as pd

def func(file_full_path_list_, des_full_filename_path_):
    if not file_full_path_list_:
            print('NODATA: ' + car)
    else:
        for j,fileitem in enumerate(file_full_path_list_):
            if j==0:
                df2write = pd.read_csv(fileitem, compression='gzip')
            else:
                df_tmp = pd.read_csv(fileitem, compression='gzip')
                try:
                    df_tmp2 = df2write.append(df_tmp)
                    df2write = df_tmp2
                except:
                    print('APPEND: ' + str(j) + ' ' + fileitem)
                    continue
            
        
        if not os.path.exists(des_full_filename_path_):
            os.makedirs(des_full_filename_path_)
        else:
            print(des_full_filename_path_ + 'already exists!')
            return
        filename2write = os.path.join(des_full_filename_path_, 'data.gz')
        try:
            df2write.to_csv(filename2write, index=False, compression='gzip')
        except:
            print('ERROR: ' + filename2write + 'by writing!')
        print(filename2write + 'complete!')
            
if __name__ == '__main__':

    df_car_info = pd.read_csv('cartype.csv')
    carvin_list = list(df_car_info['VIN'])
    
    src_path = r'D:\80_OBS_ALL'
    des_path = r'G:\88_OBS_VIN'
    
    usableCPU = int(mp.cpu_count()*0.75)
    pool = mp.Pool(usableCPU)
    
    dir_list = os.walk(src_path).__next__()[1]
    
    car_list_all = []
    car_list_all_with_path = []
    for month in dir_list:
        car_in_month = os.walk(os.path.join(src_path,month)).__next__()[1]
        car_in_month_full = [month + '\\' + vinstr for vinstr in car_in_month]
        car_list_all += car_in_month
        car_list_all_with_path += car_in_month_full
        
    car_list_all_np = np.array(car_list_all)
    car_list_all_with_path_np = np.array(car_list_all_with_path)
    
    for i,car in enumerate(carvin_list):
        idx = np.where(car_list_all_np == car)
        path_list = car_list_all_with_path_np[idx]
        full_path_list = [os.path.join(src_path, path_suffix) for path_suffix in path_list]
        file_full_path_list = []
        for full_path in full_path_list:
            file_full_path_list += [full_path + '\\' + filename for filename in os.walk(full_path).__next__()[2]]
        des_full_filename_path = os.path.join(des_path, car)
        pool.apply_async(func, args=(file_full_path_list, des_full_filename_path))
        
    pool.close()
    pool.join()
    
    
