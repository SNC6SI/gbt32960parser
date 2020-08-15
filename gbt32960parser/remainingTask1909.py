'''
1. need txt files, in which file list exists
2. compare them, retrieve remaining file
'''
import os

def return_file_list(filename_in):
    if os.path.exists(filename_in):
        with open(filename_in, 'rt') as f:
            txt = f.read()
            file_list = txt.split('\n')
            return(file_list)
    else:
        return([])


if __name__ == '__main__':
    all_file = 'File_raw111201.txt'
    #finished_file = ['File_JX.txt', 'File_SCH.txt', 'File_TSJ.txt', 'File_TY.txt', 'File_YJW.txt', 'File_ADDT.txt', 'File_ADDT2.txt']
    finished_file = ['File_finish1911_1.txt']
    
    file_list_all = return_file_list(all_file)
    file_list_all_set = set(file_list_all)
    
    finished_file_list = []
    for file_item in finished_file:
        tmp_file_list = return_file_list(file_item)
        tmp_file_set = set(tmp_file_list)   
        finished_file_list.append(tmp_file_set)
        
    finish_file_set = set()
    for file_set in finished_file_list:
        finish_file_set = set.union(finish_file_set, file_set)
        
    remaining_file_set = set.difference(file_list_all_set, finish_file_set)
    remaining_file_list = list(remaining_file_set)
    remaining_file_txt = '\n'.join(remaining_file_list)
    
    with open('remaining_file.txt', 'wt') as f:
        f.write(remaining_file_txt)