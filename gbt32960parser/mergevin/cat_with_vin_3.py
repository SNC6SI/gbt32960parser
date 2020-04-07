import os
import numpy as np
import pandas as pd
import multiprocessing as mp

def func(car, src_path, des_path, car_list_all_np, car_list_all_with_path_np):
    idx = np.where(car_list_all_np == car)
    path_list = car_list_all_with_path_np[idx]
    full_path_list = [os.path.join(src_path, path_suffix) for path_suffix in path_list]
    file_full_path_list = []
    for full_path in full_path_list:
        file_full_path_list += [full_path + '\\' + filename for filename in os.walk(full_path).__next__()[2]]
    #print(file_full_path_list)
    if not file_full_path_list:
        # logstr = logstr + 'NODATA: ' + car + '\n\n'
        print('NODATA: ' + car)
    else:
        for j,fileitem in enumerate(file_full_path_list):
            if j==0:
                df2write = pd.read_csv(fileitem, compression='gzip')
            else:
                df_tmp = pd.read_csv(fileitem, compression='gzip')
                try:
                    df_tmp2 = df2write.append(df_tmp)
                    df2write = df_tmp2
                except:
                    print('APPEND: ' + str(j) + ' ' + fileitem)
                    # logstr = logstr + 'APPEND: ' + str(j) + ' ' + fileitem + '\n\n'
                    continue
            
        des_full_filename_path = os.path.join(des_path, car)
        if not os.path.exists(des_full_filename_path):
            os.makedirs(des_full_filename_path)
        else:
            # logstr += des_full_filename_path + 'already exists!\n\n'
            # continue
            pass
        filename2write = os.path.join(des_full_filename_path, 'data.gz')
        try:
            df2write.to_csv(filename2write, index=False, compression='gzip')
        except:
            print('ERROR: ' + filename2write + 'by writing!')
            # logstr = logstr + 'ERROR: ' + filename2write + 'by writing!\n\n'
        # logstr = logstr + str(i) + 'complete!\n\n'
        # print(str(i) + 'complete!')
            
if __name__ == '__main__':

    with open('vinlist.txt', 'rt') as f:
        vin_str = f.read()
    vin_list = vin_str.split('\n')
        
    ###############################################################################
    carvin_list = vin_list[3800:4017]
    ###############################################################################
    
    src_path = r'D:\85_obs'
    des_path = r'D:\88_OBS_VIN_probook'
    # logstr = ''
    
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

    pool = mp.Pool(3)
    
    for car in carvin_list:
        try:
            pool.apply_async(func, args=(car,src_path, des_path,car_list_all_np,car_list_all_with_path_np))
        except:
            print('error: ' + car)
            