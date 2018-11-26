# -*- coding: utf-8 -*-
from socket import *
import os, sys
import struct
import time, threading
import select
import Queue

HOST = '127.0.0.1'          #主机名
PORT =  13050                #端口号 与服务器一致
BUFSIZE = 1024              #缓冲区大小1K
HEAD_STRUCT = '=128sII'
CONTENT_STRUCT = "=IIi??Li1024s" #CONTENT_STRUCT, pktNum, acpNum, windowSize, ACK, FIN, CRC, contentSize, content
RECV_WINDOW_SIZE = 500
ADDR = (HOST,PORT)
ACK = 'A'
CORRUPT = 'C'

CRC_key = 0xffffffff
CRC_table = []

def crc_init():
    for i in range(0, 256):
        c = i
        for j in range(0, 8):
            if c & 1:
                c = 0xedb88320 ^ (c >> 1)
            else:
                c = c >> 1
        CRC_table.append(c)

def crc_generate(crc, buffer):
    for buf in buffer:
        crc = CRC_table[(crc ^ ord(buf)) & 0xff] ^ (crc >> 8)
    return crc

def get_file_info(file_path):
    file_name = os.path.basename(file_path)
    file_name_len = len(file_name)
    file_size = os.path.getsize(file_path)
    return file_name, file_name_len, file_size

def Min(a, b):
    return a if a < b else b

class ResendFileType:
    def __init__(self, p, c):
        self.priority = p
        self.content = c

    def __cmp__(self, other):
        return cmp(self.priority, other.priority)

    def getPro(self):
        return self.priority

    def getCon(self):
        return self.content

acpList = Queue.Queue()
resendList = Queue.PriorityQueue()
resendListRec = {}
rcvw = []
rcvwSize = RECV_WINDOW_SIZE
congSize = 1
threshold = RECV_WINDOW_SIZE / 2
timeoutInterval = 1
timerList = []
isFinish = False
old_pkt_num = 0
new_pkt_num = 0
isLossPkt = False

def timeoutOperate(pktNum):
    global rcvw, resendList, threshold, timeoutInterval, congSize, isLossPkt
    for i in rcvw:
        n, p = struct.unpack("=I1046s", i)
        if n == pktNum and not resendListRec.has_key(n):
            resendList.put(ResendFileType(n, p))
            resendListRec[n] = 1
            isLossPkt = True
            break
    timerList[0].cancel()
    del timerList[0]

def changeWindowSize():
    global RECV_WINDOW_SIZE, congSize, timeoutInterval
    global new_pkt_num, old_pkt_num, isFinish, threshold, isLossPkt
    while not isFinish:
        print "send ", new_pkt_num - old_pkt_num, "pkts in a RTT = ", timeoutInterval, "s"
        old_pkt_num = new_pkt_num
        if isLossPkt == True:
            threshold = threshold / 2
            congSize = 1
            timeoutInterval += 1
        else:
            if congSize < threshold:
                congSize = congSize * 2
            else:
                congSize = congSize + 1 if congSize <= RECV_WINDOW_SIZE else RECV_WINDOW_SIZE
        isLossPkt = False
        time.sleep(timeoutInterval)


time_RTT = threading.Timer(0, changeWindowSize)

def send_file(udpCliSock, file_path):
    file_name, file_name_len, file_size = get_file_info(file_path)
    file_head = struct.pack(HEAD_STRUCT, file_name, file_name_len, file_size)

    try:
        udpCliSock.sendto(file_head, ADDR)            #发送文件头数据
        data, addr = udpCliSock.recvfrom(BUFSIZE)
        t, n = struct.unpack('cI', data)

        if t == ACK:
            sendFileSeq(udpCliSock, file_name, file_size)

    except Exception,e:
        print 'Error: ', e
    finally:
        udpCliSock.close()          #关闭客户端

def sendFileSeq(udpCliSendSock, file_name, file_size):
    udpCliSendSock.connect(ADDR)
    udpCliSendSock.setblocking(False)
    sent_size = 0
    packageNum = 0

    lastSendPkt = -1
    lastAckPkt = -1
    msgDict = [0] * (file_size / BUFSIZE + 1)
    lossList = {}

    global acpList, resendList, rcvw, rcvwSize, timeoutInterval, congSize, timerList, new_pkt_num, isFinish, isLossPkt, time_RTT
    time_RTT.start()
    print "start"
    with open(file_name, "rb") as f:
        try:
            while sent_size < file_size:
                try:
                    t = udpCliSendSock.recvfrom(1500)
                    acpList.put(t)
                except Exception, e:
                    if not resendList.empty():
                        if rcvwSize > 0:
                            p = resendList.get()
                            s0, a0, b0, c0, d0, e0, f0, g0 = struct.unpack(CONTENT_STRUCT, p.getCon())
                            resendListRec.pop(p.getPro())
                            udpCliSendSock.sendto(p.getCon(), ADDR)
                            timeT = threading.Timer(timeoutInterval, timeoutOperate, (s0, ))
                            timeT.start()
                            timerList.append(timeT)
                            new_pkt_num += 1
                            time.sleep(0.015)
                    else:
                        #print "window size:",Min(congSize, rcvwSize)
                        if lastSendPkt - lastAckPkt <= Min(congSize, rcvwSize):
                            remained_size = file_size - sent_size
                            send_size = BUFSIZE if remained_size > BUFSIZE else remained_size
                            s_file = f.read(send_size)
                            sent_size += send_size
                            s_pkt = struct.pack(CONTENT_STRUCT, packageNum, 0, 1, True, False, crc_generate(CRC_key, s_file), send_size, s_file)
                            rcvw.append(struct.pack("=I1046s", packageNum, s_pkt))
                            udpCliSendSock.sendto(s_pkt, ADDR)
                            timeT = threading.Timer(timeoutInterval, timeoutOperate, (packageNum, ))
                            timeT.start()
                            timerList.append(timeT)
                            packageNum += 1
                            lastSendPkt += 1
                            new_pkt_num += 1
                        else:
                            continue
                        time.sleep(0.015)
                finally:
                    if not acpList.empty():
                        d = acpList.get()
                        sendpkt, nextpkt, winSize, ack, loss, crc, cLen, content = struct.unpack(CONTENT_STRUCT, d[0])
                        rcvwSize = winSize
                        if loss:
                            if lossList.has_key(nextpkt):
                                lossList[nextpkt] += 1
                            else:
                                lossList[nextpkt] = 1
                        else:
                            if content[0] == 'R':
                                for i in rcvw:
                                    n, r_content = struct.unpack("=I1046s", i)
                                    if n == sendpkt:
                                        rcvw.remove(i)
                                        break
                            else:
                                lossList[sendpkt] = -1000
                                for i in rcvw:
                                    n, r_content = struct.unpack("=I1046s", i)
                                    if n == sendpkt:
                                        rcvw.remove(i)
                                        break

                        sort_lossList = sorted(lossList)
                        flag = 0
                        for key in sort_lossList:
                            if lossList[key] < 0:
                                lossList.pop(key)
                                lastAckPkt = key
                                continue
                            if lossList[key] >= 3 and flag == 0:
                                for i in rcvw:
                                    n, r_content = struct.unpack("=I1046s", i)
                                    if n == key and not resendListRec.has_key(key):
                                        flag = 1
                                        resendList.put(ResendFileType(n, r_content))
                                        resendListRec[key] = 1
                                        break
                        if flag == 1:
                            isLossPkt = True
            for e in timerList:
                e.cancel()
            isFinish = True
        except Exception, e:
            print "Error: ", e
        finally:
            print file_name,": transfer sucessfully"
            time_RTT.cancel()
            udpCliSendSock.close()

def recv_file(udpCliSock, file_name):
    try:
        print file_name, len(file_name)
        req = struct.pack("128sI", file_name, len(file_name))
        udpCliSock.sendto(req, ADDR)             #发送文件头数据
        res, addr = udpCliSock.recvfrom(1500)
        file_name, file_name_len, file_size = struct.unpack(HEAD_STRUCT, res)
        print file_name[0:file_name_len], file_name_len, file_size
        pktrec = [0] * (file_size/BUFSIZE+1)

        recvd_size = 0
        recvWindow = 500
        lastAckPkt = 0
        recvFileList = Queue.Queue()
        recvDatagram = Queue.Queue()
        res = struct.pack('cI', ACK, recvd_size)
        udpCliSock.sendto(res, ADDR)
        udpCliSock.setblocking(False)

        with open(file_name[0:file_name_len], 'wb+') as f:
            while recvd_size < file_size:
                try:
                    dataContent, addr = udpCliSock.recvfrom(1500)
                    recvDatagram.put(dataContent)
                except Exception, e:
                    if not recvFileList.empty():
                        print "accept", recvd_size, file_size
                        n, m, cs, c = struct.unpack("=III1024s", recvFileList.get())
                        f.write(c[0:cs])
                        recvWindow += 1
                        res = struct.pack(CONTENT_STRUCT, n, m, recvWindow, True, False, 10, 1, "Y")
                        udpCliSock.sendto(res, ADDR)
                        recvd_size = recvd_size + cs
                        time.sleep(0.015)
                    else:
                        continue
                finally:
                    while not recvDatagram.empty():
                        pktNum, lrp, winSize, a, fin, crc_code, content_size, content = struct.unpack(CONTENT_STRUCT, recvDatagram.get())
                        if crc_code == crc_generate(CRC_key, content[0:content_size]):
                            if pktrec[pktNum] == 0:
                                pktrec[pktNum] = 1
                                if pktrec[lastAckPkt] != 0:
                                    recvWindow -= 1
                                    lastAckPkt += 1
                                    recvFileList.put(struct.pack("=III1024s", pktNum, lastAckPkt, content_size, content[0:content_size]))
                                else:
                                    pktrec[pktNum] = 0
                                    recvWindow -= 1
                                    time.sleep(0.015)
                                    res = struct.pack(CONTENT_STRUCT, pktNum, lastAckPkt, recvWindow, True, True, 10, 1, "Y")
                                    udpCliSock.sendto(res, ADDR)
                                    recvWindow += 1
                            else:
                                recvWindow -= 1
                                res = struct.pack(CONTENT_STRUCT, pktNum, pktNum+1, recvWindow, True, False, 10, 1, "R")
                                time.sleep(0.015)
                                udpCliSock.sendto(res, ADDR)
                                recvWindow += 1
                        else:
                            continue
            while not recvFileList.empty():
                cs, c = struct.unpack("=I1024s", recvFileList.get())
                f.write(c[0:cs])
            print file_name[0:file_name_len],": transfer sucessfully"
    except Exception,e:
        print 'Error: ', e
    finally:
        udpCliSock.close()          #关闭客户端

def setup_connection():
    try:
        udpCliSock = socket(AF_INET, SOCK_DGRAM)
        udpCliSock.connect(ADDR)
        udpCliSock.settimeout(3)
        udpCliSock.sendto(command, ADDR)
        d, addr = udpCliSock.recvfrom(BUFSIZE)
        udpCliSock.close()
        return d
    except Exception,e:
        return 'Error: CONNECTION ERROR'

if __name__ == '__main__':
    command = sys.argv[1]
    server = sys.argv[2]
    filename = sys.argv[3]
    HOST = server
    ADDR = (HOST,PORT)
    crc_init()
    d = setup_connection()
    state, port = struct.unpack("=3sI", d)
    ADDR = (HOST, port)
    udpCliSock = socket(AF_INET, SOCK_DGRAM)
    udpCliSock.connect((HOST, port))
    if state == 'ACK':
        if command == 'lsend':
            send_file(udpCliSock, filename)
        else:
            recv_file(udpCliSock, filename)
    else:
        print d
