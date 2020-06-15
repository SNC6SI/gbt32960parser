import numpy as np
import pandas as pd
import gzip
import datetime

class gbt32960parser:

    # raw txt
    def __init__(self, filename):
        self.filename = filename
        self.rawtxtlist = []
        self.msgArray = []

        self.loglist = []
        # self.loglist.append(time.strftime("%Y%m%d_%H%M%S", time.localtime()) + '\n')
        self.loglist.append(filename + '\n')
        self.loginLine = -1
        self.logInfo = []

        # data relevant
        self.carlist = []
        self.currentVIN = []

        # Num
        self.cellUNum = 0
        self.cellTNum = 0

#        with gzip.open(filename, 'r') as f:
#            for line in f:
#                line = line.decode()
#                msg = line.split(',')[2]
#                self.rawtxtlist.append(msg)
        print(filename)
        try:
            msg = pd.read_csv(filename, compression='gzip', header=None)
            #msg = pd.read_csv(filename, header=None)
            self.rawtxtlist = list(msg[2])
        except:
            with gzip.open(filename,'r') as f:
                tmptxt = f.read()
                if len(tmptxt)==0:
                    self.loglist.append('File problem here.\n')


    # raw txt to array
    def conv2array(self):
        for item in self.rawtxtlist:
            onearray = np.array(bytearray.fromhex(item), dtype = 'uint8')
            self.msgArray.append(onearray)
        self.msgArray = np.array(self.msgArray)



    # log
    def printLog(self, logfilename):
        #filename = ''.join([os.path.splitext(self.filename)[0], '.log'])
        with open(logfilename, 'a') as f:
            #self.loglist.append('End of one parser.\n\n')
            self.loglist.append('\n')
            f.writelines(self.loglist)


    # start character
    # bbc
    # msg number
    def validCheck(self):
        for i, item in enumerate(self.msgArray):

            # start character check
            if not (item[0]==0x23 and item[1]==0x23):
                self.loglist.append('Line {0:<6d}:  start character error.\n'.format(i+1))
                continue

            # bbc check
            bbc = 0
            for byte in item[2:(len(item)-1)]:
                bbc = bbc ^ byte
            if bbc != item[len(item)-1]:
                self.loglist.append('Line {0:<6d}:  block check character error. 0x{1:X} is calculated but 0x{2:X} is there in message.\n'.format(i+1, bbc, item[len(item)-1]))
                continue

            # msg byte number check
            totalByteInMsg = (item[22].astype('uint16') << 8) | (item[23].astype('uint16'))
            if totalByteInMsg != (len(item) - 25):
                self.loglist.append('Line {0:<6d}:  data length check error. {1}(0x{1:0>4X}) is counted but {2}(0x{2:0>4X}) is there in message.\n'.format(i+1, len(item) - 25, totalByteInMsg))
                continue


    def commandIdCheck(self):
        for i, item in enumerate(self.msgArray):
            # case: login
            if item[2] == 0x01:
                if self.loginLine != -1:
                        self.loglist.append('last login in line {0} is not logged out.\n'.format(self.loginLine))
                self.loginLine = i + 1
                pass

            # case normal & additional
            elif item[2] == 0x02 or item[2] == 0x03:
                if self.loginLine == -1:
                    self.loglist.append('Line {0:<6d}:  data message without login.\n'.format(i+1))
                pass
            
            # case logout
            elif item[2] == 0x04:
                if self.loginLine == -1:
                    self.loglist.append('Line {0:<6d}:  logged out before logged in.\n'.format(i+1))
                self.loginLine = -1
                pass

            # case other
            else:
                self.loglist.append('Line {0:<6d}:  no relevant script for command id: {1:0>2X}.\n'.format(i+1, item[2]))
                continue

        if self.loginLine != -1:
            self.loglist.append('last login in line {0} is not logged out.\n'.format(self.loginLine))



    # filter by vin
    def filterByVin(self):
        vinMat = np.array([])
        for i, item in enumerate(self.msgArray):
            vinMat = np.append(vinMat, item[4:21])
            vinMat = vinMat.reshape(int(len(vinMat)/17), 17)
            self.vinUnique, self.vinIndex, self.vinInvIndex = \
                np.unique(vinMat, axis = 0, return_index = True, return_inverse = True)



    # data extraction
    def dataPreProc(self):
        # TIME
        self.Info_0 = np.full((self.msgArray.shape[0],6), np.nan, dtype = 'uint32')
        # VEH
        self.Info_1 = np.full((self.msgArray.shape[0],11),np.nan, dtype = 'uint32')
        # TM
        self.Info_2 = np.full((self.msgArray.shape[0],9), np.nan, dtype = 'float32')
        # POS
        self.Info_3 = np.full((self.msgArray.shape[0],3), np.nan, dtype = 'uint32')
        # EXTREMUM
        self.Info_4 = np.full((self.msgArray.shape[0],12),np.nan, dtype = 'float32')
        # WARN
        self.Info_5 = np.full((self.msgArray.shape[0],6), np.nan, dtype = 'uint32')
        # CELL U
        self.Info_6 = np.full((self.msgArray.shape[0],144),np.nan, dtype = 'float32') #92 create a bigger array first and resize in first loop
        # CELL T
        self.Info_7 = np.full((self.msgArray.shape[0],72),np.nan, dtype = 'float32') #20 create a bigger array first and resize in first loop


        #for i in range(self.vinIndex.size):
        #    idx = np.where(self.vinInvIndex==i)
        #    tmpArray = self.msgArray[idx]
        i = 0
        cellUflg = False
        cellTflg = False
        for item in self.msgArray:
            if not ((item[2] == 0x02) or (item[2] == 0x03)):
                continue
            else:
                idxHead = 0
                skipcnt = 0
                while (idxHead < (item.size - 1) and skipcnt < 5):
                    # TIME
                    if idxHead == 0:
                        self.Info_0[i, :] = item[24:30]
                        idxHead = 30
                    # VEH
                    elif item[idxHead]==0x01:
                        self.Info_1[i, 0]  = item[idxHead+1] # veh st
                        self.Info_1[i, 1]  = item[idxHead+2] # chrg st
                        self.Info_1[i, 2]  = item[idxHead+3] # mode
                        self.Info_1[i, 3]  = item[idxHead+4]<<8 | item[idxHead+5] # vel
                        self.Info_1[i, 4]  = item[idxHead+6]<<24 | item[idxHead+7]<<16 | item[idxHead+8]<<8 | item[idxHead+9] # odo
                        self.Info_1[i, 5]  = item[idxHead+10]<<8 | item[idxHead+11] # pack U
                        self.Info_1[i, 6]  = item[idxHead+12]<<8 | item[idxHead+13] # pack I
                        self.Info_1[i, 7]  = item[idxHead+14] # soc
                        self.Info_1[i, 8]  = item[idxHead+15] # dc st
                        self.Info_1[i, 9]  = item[idxHead+16] # gear
                        self.Info_1[i, 10] = item[idxHead+17]<<8 | item[idxHead+18] # isoR
                        idxHead = min(item.size-1, idxHead+21)
                    # TM
                    elif item[idxHead]==0x02:
                        self.Info_2[i, 0]  = item[idxHead+1] # num
                        self.Info_2[i, 1]  = item[idxHead+2] # SN
                        self.Info_2[i, 2]  = item[idxHead+3] # TM st
                        self.Info_2[i, 3]  = item[idxHead+4] # MCU T
                        self.Info_2[i, 4]  = item[idxHead+5]<<8 | item[idxHead+6] # TM n
                        self.Info_2[i, 5]  = item[idxHead+7]<<8 | item[idxHead+8] # TM trq
                        self.Info_2[i, 6]  = item[idxHead+9] # TM T
                        self.Info_2[i, 7]  = item[idxHead+10]<<8 | item[idxHead+11] # MCU ip U
                        self.Info_2[i, 8]  = item[idxHead+12]<<8 | item[idxHead+13] # MCU dc I
                        idxHead = min(item.size-1, idxHead+14)
                    # POS
                    elif item[idxHead]==0x05:
                        self.Info_3[i, 0]  = item[idxHead+1] # pos st
                        self.Info_3[i, 1]  = item[idxHead+2]<<24 | item[idxHead+3]<<16 | item[idxHead+4]<<8 | item[idxHead+5] # longitude
                        self.Info_3[i, 2]  = item[idxHead+6]<<24 | item[idxHead+7]<<16 | item[idxHead+8]<<8 | item[idxHead+9] # latitude
                        idxHead = min(item.size-1, idxHead+10)
                    # EXTREMUM
                    elif item[idxHead]==0x06:
                        self.Info_4[i, 0]   = item[idxHead+1] # cell U highest subsystem num
                        self.Info_4[i, 1]   = item[idxHead+2] # cell U highest num
                        self.Info_4[i, 2]   = item[idxHead+3]<<8 | item[idxHead+4] # cell U highest value
                        self.Info_4[i, 3]   = item[idxHead+5] # cell U lowest subsystem num
                        self.Info_4[i, 4]   = item[idxHead+6] # cell U lowest num
                        self.Info_4[i, 5]   = item[idxHead+7]<<8 | item[idxHead+8] # cell U lowest value
                        self.Info_4[i, 6]   = item[idxHead+9] # cell T highest subsystem num
                        self.Info_4[i, 7]   = item[idxHead+10] # cell T highest num
                        self.Info_4[i, 8]   = item[idxHead+11] # cell T highest value
                        self.Info_4[i, 9]   = item[idxHead+12] # cell T lowest subsystem num
                        self.Info_4[i, 10]  = item[idxHead+13] # cell T lowest num
                        self.Info_4[i, 11]  = item[idxHead+14] # cell T lowest value
                        idxHead = min(item.size-1, idxHead+15)
                    # WARN
                    elif item[idxHead]==0x07:
                        self.Info_5[i, 0]   = item[idxHead+1] # highest fault lvl
                        self.Info_5[i, 1]   = item[idxHead+2]<<24 | item[idxHead+3]<<16 | item[idxHead+4]<<8 | item[idxHead+5] # fault flag collection
                        self.Info_5[i, 2]   = item[idxHead+6] # total fault num of pack
                        tmp = self.Info_5[i, 2] * 4
                        self.Info_5[i, 3]   = item[idxHead + 7 + tmp] # total fault num of TM
                        tmp = tmp + self.Info_5[i, 3] * 4
                        self.Info_5[i, 4]   = item[idxHead + 8 + tmp] # total fault num of engine, shall always be 0 for BEV
                        tmp = tmp + self.Info_5[i, 4] * 4
                        self.Info_5[i, 5]   = item[idxHead + 9 + tmp] # total fault num of other
                        tmp = tmp + self.Info_5[i, 5] * 4
                        idxHead = min(item.size-1, idxHead+10+tmp)
                    # CELL U
                    elif item[idxHead]==0x08:
                        if item[idxHead+1]!=0x00: # there exists cases, that cell U number is zero
                            cellUNumRC = item[idxHead+11]
                            if cellUNumRC > self.cellUNum:
                                self.cellUNum = item[idxHead+11]
                                cellUflg = False
                            if not cellUflg:
                                if self.cellUNum < 144:
                                    self.Info_6 = np.resize(self.Info_6, (self.msgArray.shape[0], self.cellUNum))
                                    self.loglist.append("cellUNum changed from 144 to {0}.\n".format(self.cellUNum))
                            for j in range(self.cellUNum):
                                try:
                                    self.Info_6[i, j] = item[idxHead+12+2*j]<<8 | item[idxHead+13+2*j]
                                except:
                                    print('Line {0:<6d}: problem with cellUnum {1}.'.format(i+1, j))
                            idxHead = min(item.size-1, idxHead+12+2*self.cellUNum)
                            cellUflg = True
                        else:
                            idxHead = min(item.size-1, idxHead+2)
                    # CELL T
                    elif item[idxHead]==0x09:
                        if item[idxHead+1]!=0x00: # there may be also cases, that cell T number is zero
                            cellTNumRC = (item[idxHead+3]<<8 | item[idxHead+4])
                            if cellTNumRC > self.cellTNum:
                                self.cellTNum = cellTNumRC
                                cellTflg = False
                            if not cellTflg:
                                if self.cellTNum < 72:
                                    self.Info_7 = np.resize(self.Info_7, (self.msgArray.shape[0], self.cellTNum))
                                    self.loglist.append("cellTNum changed from 72 to {0}.\n".format(self.cellTNum))
                            for j in range(self.cellTNum):
                                try:
                                    self.Info_7[i, j] = item[idxHead+5+j]-40
                                except:
                                    print('Line {0:<6d}: problem with cellTnum {1}.'.format(i+1, j))
                            idxHead = min(item.size-1, idxHead+5+self.cellTNum)
                            cellTflg = True
                        else:
                            idxHead = min(item.size-1, idxHead+2)
                    # custom data defined by dearcc beginning with A0
                    elif item[idxHead]==0xA0:
                        idxHead = item.size-1
                        if i==0:
                            self.loglist.append('There ara custom data defined by dearcc beginning with A0 in this file.(from 1st line)\n')
                    else:
                        skipcnt+=1
                        if skipcnt>=5:
                            self.loglist.append('msgAry {0:<4d}:  skip counter reached, maybe problem with raw msg.\n'.format(i))
                i+=1
                
    def dataOutput(self):
        # time
        df_time_ = pd.DataFrame({'year':self.Info_0[:,0]+2000, 'month':self.Info_0[:,1],
                                'day':self.Info_0[:,2], 'hour':self.Info_0[:,3],
                                'minute':self.Info_0[:,4], 'second':self.Info_0[:,5]})
        df_time = pd.DataFrame(pd.to_datetime(df_time_),columns=['time'])
        
                
        # ================================================================================
        df_vehst = pd.DataFrame(self.Info_1[:,0],columns=['vehst'])
        df_chrgst = pd.DataFrame(self.Info_1[:,1],columns=['chrgst'])
        df_vel = pd.DataFrame(self.Info_1[:,3].astype(np.float32)/10.0,columns=['vel'])
        df_odo = pd.DataFrame(self.Info_1[:,4].astype(np.float32)/10.0,columns=['odo'])
        df_packU = pd.DataFrame(self.Info_1[:,5].astype(np.float32)/10.0,columns=['packU'])
        df_packI = pd.DataFrame(self.Info_1[:,6].astype(np.float32)/10.0-1000.0,columns=['packI'])
        
        df_TMst = pd.DataFrame(self.Info_2[:,2],columns=['TMst'])
        df_TMn = pd.DataFrame(self.Info_2[:,4]-20000,columns=['TMn'])
        df_TMtrq = pd.DataFrame(self.Info_2[:,5]/10.0-2000,columns=['TMtrq'])
        df_MCUT = pd.DataFrame(self.Info_2[:,3]-40,columns=['MCUT'])
        df_TMT = pd.DataFrame(self.Info_2[:,6]-40,columns=['TMT'])
        
        df_posLong = pd.DataFrame(self.Info_3[:,1],columns=['posLong'])
        df_posLati = pd.DataFrame(self.Info_3[:,2],columns=['posLati'])
        
        df_U_Hi = pd.DataFrame(self.Info_4[:,2],columns=['U_Hi'])
        df_U_Lo = pd.DataFrame(self.Info_4[:,5],columns=['U_Lo'])
        df_U_HiN = pd.DataFrame(self.Info_4[:,1],columns=['U_HiN'])
        df_U_LoN = pd.DataFrame(self.Info_4[:,4],columns=['U_LoN'])
        df_T_Hi = pd.DataFrame(self.Info_4[:,8]-40,columns=['T_Hi'])
        df_T_Lo = pd.DataFrame(self.Info_4[:,11]-40,columns=['T_Lo'])
        df_T_HiN = pd.DataFrame(self.Info_4[:,7],columns=['T_HiN'])
        df_T_LoN = pd.DataFrame(self.Info_4[:,10],columns=['T_LoN'])
        # ================================================================================
        
        # soc
        df_soc = pd.DataFrame(self.Info_1[:,7],columns=['soc'])
        
        # cell U
        cellUListName = []
        #if not hasattr(self, 'cellUNum'):
        if self.cellUNum == 0:
            self.cellUNum = 144
        for i in range(self.cellUNum):
            cellUListName.append('U' + str(i))
        df_cellU = pd.DataFrame(self.Info_6, columns=cellUListName)
        
        
        # cell T
        cellTListName = []
        #if not hasattr(self, 'cellTNum'):
        if self.cellTNum == 0:
            self.cellTNum = 72
        for i in range(self.cellTNum):
            cellTListName.append('T' + str(i))
        df_cellT = pd.DataFrame(self.Info_7, columns=cellTListName)
        
        # concat
        self.df = pd.concat([df_time, 
                             df_vehst,df_chrgst,df_vel,df_odo,df_packU,df_packI,
                             df_TMst,df_TMn,df_TMtrq,df_MCUT,df_TMT,
                             df_posLong,df_posLati,
                             df_U_Hi,df_U_Lo,df_U_HiN,df_U_LoN,
                             df_T_Hi,df_T_Lo,df_T_HiN,df_T_LoN,                             
                             df_soc, df_cellU, df_cellT], axis=1)
        self.df.sort_values(by='time', inplace=True, ignore_index=True)
        # self.df.reset_index(drop=True, inplace=True)
        

if __name__ == '__main__':

    #a = gbt32960parser(r'D:\OBS\data201903\LA9AB2AC6H0LDN511\detail_data-r-00000.gz')
    a = gbt32960parser(r'D:\OBS\data201810\LB9AB2AC8H0LDN156\test.txt')

    a.conv2array()

    a.validCheck()

    #a.commandIdCheck() # login and logout msgs are already filtered by TSP

    #a.filterByVin() # confirmed that one car in one file

    a.dataPreProc()

    a.printLog('dataReadLog_' + datetime.datetime.now().strftime("%Y%m%d_%H%M%S") + '.log')

    a.dataOutput()
    
#    a.df.to_csv('abc.gz', compression='gzip', index=False)
    a.df.to_csv('abc.csv', index=False)