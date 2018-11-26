# -*- coding: utf-8 -*-
from socket import *
import os, sys
import struct
import time, threading
import select
import Queue


HOST = '127.0.0.1'         #主机名
PORT =  13050              #端口号
BUFSIZE = 1024             #缓冲区大小1K
HEAD_STRUCT = '=128sII'
CONTENT_STRUCT = "=IIi??Li1024s"
ADDR = (HOST,PORT)
ACK = 'A'
CORRUPT = 'C'

CRC_key = 0xffffffff
CRC_table = []
linkList = []
portList = [1] + [0] * 99

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


class SendClass:
    RECV_WINDOW_SIZE = 500
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

    def __init__(self):
        pass

    def __del__(self):
        pass

    def send_file(self, udpSerSock):
        try:
            req, addr = udpSerSock.recvfrom(BUFSIZE)
            filename, filename_len = struct.unpack("128sI", req)
            file_name, file_name_len, file_size = get_file_info(filename[0:filename_len])
            file_head = struct.pack(HEAD_STRUCT, file_name, file_name_len, file_size)
            udpSerSock.sendto(file_head, addr)
            data, addr = udpSerSock.recvfrom(BUFSIZE)
            t, num = struct.unpack('cI', data)

            if t == ACK:
                self.sendFileSeq(udpSerSock, file_name, file_size, addr)
        except Exception,e:
            print 'Error: ', e
        finally:
            udpSerSock.close()          #关闭客户端

    def sendFileSeq(self, udpSerSock, file_name, file_size, addr):
        print addr
        udpSerSock.setblocking(False)
        sent_size = 0
        packageNum = 0

        lastSendPkt = -1
        lastAckPkt = -1

        msgDict = [0] * (file_size / BUFSIZE + 1)
        lossList = {}
        time_RTT = threading.Timer(0, self.changeWindowSize)
        time_RTT.start()
        print "start"
        with open(file_name, "rb") as f:
            try:
                while sent_size < file_size:
                    try:
                        t = udpSerSock.recvfrom(1500)
                        self.acpList.put(t)
                    except Exception, e:
                        if not self.resendList.empty():
                            if self.rcvwSize > 0:
                                p = self.resendList.get()
                                s0, a0, b0, c0, d0, e0, f0, g0 = struct.unpack(CONTENT_STRUCT, p.getCon())
                                self.resendListRec.pop(p.getPro())
                                udpSerSock.sendto(p.getCon(), addr)
                                timeT = threading.Timer(self.timeoutInterval, self.timeoutOperate, (s0, ))
                                timeT.start()
                                self.timerList.append(timeT)
                                self.new_pkt_num += 1
                                time.sleep(0.015)
                        else:
                            if lastSendPkt - lastAckPkt <= Min(self.congSize, self.rcvwSize):
                                remained_size = file_size - sent_size
                                send_size = BUFSIZE if remained_size > BUFSIZE else remained_size
                                s_file = f.read(send_size)
                                sent_size += send_size
                                s_pkt = struct.pack(CONTENT_STRUCT, packageNum, 0, 1, True, False, crc_generate(CRC_key, s_file), send_size, s_file)
                                self.rcvw.append(struct.pack("=I1046s", packageNum, s_pkt))
                                udpSerSock.sendto(s_pkt, addr)
                                timeT = threading.Timer(self.timeoutInterval, self.timeoutOperate, (packageNum, ))
                                timeT.start()
                                self.timerList.append(timeT)
                                packageNum += 1
                                lastSendPkt += 1
                                self.new_pkt_num += 1
                            else:
                                continue
                            time.sleep(0.015)
                    finally:
                        if not self.acpList.empty():
                            d = self.acpList.get()
                            sendpkt, nextpkt, winSize, ack, loss, crc, cLen, content = struct.unpack(CONTENT_STRUCT, d[0])
                            self.rcvwSize = winSize
                            if loss:
                                if lossList.has_key(nextpkt):
                                    lossList[nextpkt] += 1
                                else:
                                    lossList[nextpkt] = 1
                            else:
                                if content[0] == 'R':
                                    for i in self.rcvw:
                                        n, r_content = struct.unpack("=I1046s", i)
                                        if n == sendpkt:
                                            self.rcvw.remove(i)
                                            break
                                else:
                                    lossList[sendpkt] = -1000
                                    for i in self.rcvw:
                                        n, r_content = struct.unpack("=I1046s", i)
                                        if n == sendpkt:
                                            self.rcvw.remove(i)
                                            break
                        sort_lossList = sorted(lossList)
                        flag = 0
                        for key in sort_lossList:
                            if lossList[key] < 0:
                                lossList.pop(key)
                                lastAckPkt = key
                                continue
                            if lossList[key] >= 3 and flag == 0:
                                for i in self.rcvw:
                                    n, r_content = struct.unpack("=I1046s", i)
                                    if n == key and not self.resendListRec.has_key(key):
                                        flag = 1
                                        self.resendList.put(ResendFileType(n, r_content))
                                        self.resendListRec[key] = 1
                                        break
                        if flag == 1:
                            self.isLossPkt = True
                for e in self.timerList:
                    e.cancel()
                self.isFinish = True
            except Exception, e:
                print "Error: ", e
            finally:
                global portList
                print file_name,": transfer sucessfully"
                print udpSerSock.getsockname()[1]
                portList[udpSerSock.getsockname()[1]-PORT] = 0
                time_RTT.cancel()
                udpSerSock.close()

    def timeoutOperate(self, pktNum):
        for i in self.rcvw:
            n, p = struct.unpack("=I1046s", i)
            if n == pktNum and not self.resendListRec.has_key(n):
                self.resendList.put(ResendFileType(n, p))
                self.resendListRec[n] = 1
                self.isLossPkt = True
                break
        self.timerList[0].cancel();
        del self.timerList[0]

    def changeWindowSize(self):
        while not self.isFinish:
            print "send", self.new_pkt_num - self.old_pkt_num, "pkts in a RTT = ", self.timeoutInterval,"s"
            self.old_pkt_num = self.new_pkt_num
            if self.isLossPkt :
                self.threshold = self.threshold / 2
                self.congSize = 1
                self.timeoutInterval += 1
            else:
                if self.congSize < self.threshold:
                    self.congSize = self.congSize * 2
                else:
                    self.congSize = self.congSize + 1 if self.congSize <= self.RECV_WINDOW_SIZE else self.RECV_WINDOW_SIZE
            self.isLossPkt = False
            time.sleep(self.timeoutInterval)

class RecvClass:
    def  __init__(self):
        pass

    def __del__(self):
        pass

    def recv_file(self, udpSerSock):
        try:
            dataHead, addr = udpSerSock.recvfrom(BUFSIZE)
            file_name, filename_len, file_size = struct.unpack(HEAD_STRUCT, dataHead)
            file_name = file_name[0:filename_len]

            print file_name[0:filename_len], filename_len, file_size

            pktrec = [0] * (file_size/BUFSIZE+1)
            recvd_size = 0
            recvWindow = 500
            lastAckPkt = 0
            recvFileList = Queue.Queue()
            recvDatagram = Queue.Queue()

            res = struct.pack('cI', ACK, recvWindow)
            udpSerSock.sendto(res, addr)
            udpSerSock.setblocking(False)

            with open(file_name[0:filename_len], 'wb+') as f:
                while recvd_size < file_size:
                    try:
                        dataContent, addr = udpSerSock.recvfrom(1500)
                        recvDatagram.put(dataContent)
                    except Exception, e:
                        if not recvFileList.empty():
                            print "accept", recvd_size, file_size
                            n, m, cs, c = struct.unpack("=III1024s", recvFileList.get())
                            f.write(c[0:cs])
                            recvWindow += 1
                            res = struct.pack(CONTENT_STRUCT, n, m, recvWindow, True, False, 10, 1, "Y")
                            udpSerSock.sendto(res, addr)
                            recvd_size = recvd_size + cs
                            time.sleep(0.015) #防止发送方阻塞
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
                                        time.sleep(0.015) #防止发送方阻塞
                                        res = struct.pack(CONTENT_STRUCT, pktNum, lastAckPkt, recvWindow, True, True, 10, 1, "Y")
                                        udpSerSock.sendto(res, addr)
                                        recvWindow += 1
                                else:
                                    recvWindow -= 1
                                    time.sleep(0.015) #防止发送方阻塞
                                    res = struct.pack(CONTENT_STRUCT, pktNum, pktNum+1, recvWindow, True, False, 10, 1, "R")
                                    udpSerSock.sendto(res, addr)
                                    recvWindow += 1
                            else:
                                continue
                while not recvFileList.empty():
                    n, m, cs, c = struct.unpack("=III1024s", recvFileList.get())
                    f.write(c[0:cs])
                print file_name[0:filename_len],": transfer sucessfully"
        except Exception, e:
            print "Socket error: %s" % str(e)
        finally:
            global portList
            print udpSerSock.getsockname()[1]
            portList[udpSerSock.getsockname()[1]-PORT] = 0
            udpSerSock.close()

def setup_connection():
    global portList
    udpSerSock = socket(AF_INET, SOCK_DGRAM)     #UDP协议
    udpSerSock.bind(ADDR)
    udpSerSock.settimeout(5)
    try:
        d, addr = udpSerSock.recvfrom(BUFSIZE)
        newP = -1
        for p in range(1, 100):
            if portList[p] == 0:
                newP = p
                break
        if newP >= 1:
            udpSerSock.sendto(struct.pack("=3sI", 'ACK', PORT+newP), addr)
            portList[newP] = 1
        else:
            udpSerSock.sendto(struct.pack("=3sI", 'ERR', -1), addr)
        if newP >= 1:
            return (d, addr, PORT+newP)
        else:
            return (d, addr, -1)
    except Exception, e:
        print 'No new link in 5 sec'
        return ('Error: CONNECTION ERROR', 0, 0)
    finally:
        udpSerSock.close()

def fileOperate(mode, host, port):
    udpSerSock = socket(AF_INET, SOCK_DGRAM)     #UDP协议
    udpSerSock.bind((host, port))                        #绑定地址到套接字
    if mode == 'lsend':
        r = RecvClass()
        r.recv_file(udpSerSock)
    else:
        s = SendClass()
        s.send_file(udpSerSock)

def belongs(list, e):
    for i in len(list):
        if cmp(e, list[i]) == 0:
            return i
    return -1

if __name__ == '__main__':
    crc_init()
    while True:
        d = setup_connection()
        if d[2] != 0:
            if d[2] == -1:
                continue
            else:
                p = threading.Thread(target=fileOperate, args=[d[0], HOST, d[2],])
                p.setDaemon(True)
                p.start()
        else:
            continue
