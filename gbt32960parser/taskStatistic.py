import os

def genFilesInFolder(itemFolder):
    files = []
    for root_,dir_,file_ in os.walk(itemFolder):
        for fileitem in file_:
            if os.path.splitext(fileitem)[-1]=='.gz':
                files.append(os.path.join(root_, fileitem))
    return files

if __name__ == '__main__':
    srcFolder = r'D:\98_OBS\data201911'
    srcStat = 'File_finish1911_1.txt'
    srcFolder = os.path.normpath(srcFolder)
    
    monthList = os.walk(srcFolder).__next__()[1]

    srcFiles = genFilesInFolder(srcFolder)

    srcFilesNormList = []
    for fileitem in srcFiles:
        srcFilesNormList.append('\\'.join(fileitem.split('\\')[-3:]))
    srcFilesNormSet = set(srcFilesNormList)

# only used for statistic purpose
    # statRawList = len(monthList)*[0]
    # for item in srcFilesNormSet:
    #     statRawList[monthList.index(item.split('\\')[0])] += 1
# only used for statistic purpose
        

    srcFilesStr = '\n'.join(srcFilesNormList)
    with open(srcStat, 'wt') as f:
        f.write(srcFilesStr)