package com.alibaba.search.swift.perf;

import com.alibaba.search.swift.SwiftAdminAdaptor;
import com.alibaba.search.swift.SwiftClient;
import com.alibaba.search.swift.SwiftReader;
import com.alibaba.search.swift.SwiftWriter;
import com.alibaba.search.swift.exception.SwiftException;
import com.alibaba.search.swift.protocol.AdminRequestResponse;
import com.alibaba.search.swift.protocol.ErrCode;
import com.alibaba.search.swift.protocol.SwiftMessage;
import com.google.protobuf.ByteString;

import java.util.Date;

/**
 * Created by sdd on 15-11-18.
 */
public class ReadPerf {
    public static void main(String[] args) {
        String clientConfig = "zkPath=zfs://10.101.170.89:12181,10.101.170.90:12181,10.101.170.91:12181,10.101.170.92:12181,10.101.170.93:12181/swift/swift_service"
                + ";logConfigFile=./swift_alog.conf;useFollowerAdmin=false";
        if (args.length >= 1) {
            clientConfig = args[0];
            System.out.println("client config:" + clientConfig);
        }
        String readerConfig = "topicName=sdd_test";
        if (args.length >= 2) {
            readerConfig = args[1];
            System.out.println("reader config:" +readerConfig);
        }
        int msgCount = 1000000;
        if (args.length >= 3) {
            msgCount = Integer.decode(args[2]).intValue();
            System.out.println(msgCount);
        }
        boolean batchRead= true;
        if (args.length >= 4) {
            if (args[4] == "false") {
                batchRead = false;
            }
        }
        SwiftClient testClient = new SwiftClient();
        SwiftReader testReader = null;
        try {
            testClient.init(clientConfig);
            testReader = testClient.createReader(readerConfig);
            long startTime = System.currentTimeMillis();
            long readCnt = 0;
            long dataSize = 0;
            if (batchRead) {
                while (readCnt < msgCount) {
                    try {
                        SwiftMessage.Messages readMsgs;
                        readMsgs = testReader.reads();
                        for (int i = 0; i < readMsgs.getMsgsCount(); i++) {
                            dataSize += readMsgs.getMsgs(i).getData().size();
                        }
                        readCnt += readMsgs.getMsgsCount();
                    } catch (SwiftException e) {
                        e.printStackTrace();
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        }
                    }
                }
            } else {
                while (readCnt < msgCount) {
                    try {
                        SwiftMessage.Message readMsg;
                        readMsg = testReader.read();
                        dataSize += readMsg.getData().size();
                        readCnt++;
                    } catch (SwiftException e) {
                        e.printStackTrace();
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        }
                    }
                }

            }

            long readUseTime = System.currentTimeMillis() - startTime;
            long readSpeed = readCnt * 1000 / readUseTime;
            double readBandwidth = dataSize / 1024.0 / 1024.0;
            System.out.printf("read msg count [%d], use time [%d] ms, speed [%d] Msg/s, bandwidth [%f] MByte/s.\n", readCnt, readUseTime,
                    readSpeed, readBandwidth);
            testReader.close();
            testReader = null;
        } catch (SwiftException e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
        } finally {
            if (testReader != null) {
                testReader.close();
            }
            testClient.close();
        }
    }
}
