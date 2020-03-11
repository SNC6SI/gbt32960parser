import os
import time
import numpy as np

class gbt32960parser:

    # raw txt
    def __init__(self, filename):
        self.filename = filename
        self.rawtxtlist = []
        self.loglist = []
        self.loglist.append(time.strftime("%Y%m%d_%H%M%S", time.localtime()) + '\n')
        with open(filename, 'r',encoding = "utf-8") as f:
            for line in f:
                msg = line.split(',')[6]
                rawtxt = msg.lstrip('报文内容: ')
                self.rawtxtlist.append(rawtxt)



    # raw txt to array
    def conv2array(self):
        self.ndarraylist = []
        for item in self.rawtxtlist:
            onearray = np.array(bytearray.fromhex(item), dtype = 'uint8')
            self.ndarraylist.append(onearray)



    # log
    def printLog(self):
        filename = ''.join([os.path.splitext(self.filename)[0], '.log'])
        with open(filename, 'a') as f:
            self.loglist.append('End of one parser.\n\n')
            f.writelines(self.loglist)



    # valid check
    # matrix generate
    def dataPreProc(self):
        for i, item in enumerate(self.ndarraylist):

            # start character check
            if not (item[0]==0x23 and item[1]==0x23):
                self.loglist.append('Line {0}: start character error.\n'.format(i+1))
                continue

            # bbc check
            bbc = 0
            for byte in item[2:(len(item)-1)]:
                bbc = bbc ^ byte
            if bbc != item[len(item)-1]:
                self.loglist.append('Line {0}: block check character error. 0x{1:X} is calculated but 0x{2:X} is there in message.\n'.format(i+1, bbc, item[len(item)-1]))
                continue

            # msg byte number check
            totalByteInMsg = (item[22].astype('uint16') << 8) | (item[23].astype('uint16'))
            if totalByteInMsg != (len(item) - 25):
                self.loglist.append('Line {0}: data length check error. {1}(0x{1:0>4X}) is counted but {2}(0x{2:0>4X}) is there in message.\n'.format(i+1, len(item) - 25, totalByteInMsg))
                continue

            # case: login
            if item[2] == 0x01:
                pass
            elif item[2] == 0x02 or item[2] == 0x03:
                pass
            else:
                self.loglist.append('Line {0}: no relevant script for command id: {1:0>2X}.\n'.format(i+1, item[2]))
                continue


if __name__ == '__main__':
    a = gbt32960parser(r'C:\Users\shing\Desktop\GBT 32960\LA9AB2AC0H0LDN066.csv')
    a.conv2array()
    #a.loglist.append('asfdasf\n')
    #a.loglist.append('adfasdfasdf\n')
    a.dataPreProc()
    a.printLog()
    b= 1