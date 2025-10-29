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
public class SwiftClientWriteReadPerf {
    public static void main(String[] args) {
        String configStr = "zkPath=zfs://10.101.170.89:12181,10.101.170.90:12181,10.101.170.91:12181,10.101.170.92:12181,10.101.170.93:12181/swift/swift_service"
                + ";logConfigFile=./swift_alog.conf;useFollowerAdmin=false";
        int msgSize = 1024;
        long sendMsgCnt = 1000 * 10;
        String readerConfig = "";
        String writerConfig = "";
        if (args.length > 0) {
            configStr = args[0];
        }
        if (args.length > 1) {
            msgSize = Integer.decode(args[1]).intValue();
        }
        if (args.length > 2) {
            readerConfig = args[2];
        }
        if (args.length > 3) {
            writerConfig = args[3];
        }
        SwiftClient testClient = new SwiftClient();
        SwiftAdminAdaptor testSwiftAdmin = null;
        SwiftWriter testWriter = null;
        SwiftReader testReader = null;
        String topicName = "swift_java_client_test_topic_name";
        System.out.println(new Date().toString());
        try {
            //create topic and wait topic ready
            AdminRequestResponse.TopicCreationRequest.Builder request = AdminRequestResponse.TopicCreationRequest
                    .newBuilder();
            request.setTopicName(topicName);
            request.setPartitionCount(8);
            request.setPartitionMaxBufferSize(5120);
            request.setResource(10);
            request.setDeleteTopicData(true);
            request.setPartitionLimit(8);
            testClient.init(configStr);
            testSwiftAdmin = testClient.getAdminAdapter();
            testSwiftAdmin.createTopic(request.build());
            if (testSwiftAdmin.waitTopicReady(topicName, 10)) {
                System.out.printf("topic [%s] is running!\n", topicName);
            } else {
                throw new SwiftException(ErrCode.ErrorCode.ERROR_UNKNOWN, "WaitTopicReady failed");
            }
            // write data
            if (writerConfig.isEmpty()) {
                writerConfig = "topicName=" + topicName + ";functionChain=HASH,hashId2partId";
            }
            byte[] bytes = new byte[msgSize];
            for (int i = 0; i < bytes.length; i++) {
                bytes[i] = (byte) ('a' + i % 26);
            }
            testWriter = testClient.createWriter(writerConfig);
            SwiftMessage.WriteMessageInfo.Builder msg = SwiftMessage.WriteMessageInfo.newBuilder();
            msg.setData(ByteString.copyFrom(bytes));
            long startTime = System.currentTimeMillis();
            int message_size = msg.build().toByteArray().length;
            for (long i = 0; i < sendMsgCnt; i++) {
                msg.setHashStr(ByteString.copyFrom(new Long(i).toString().getBytes()));
                try {
                    testWriter.write(msg.build());
                } catch (SwiftException e) {
                    if (e.getEc() != ErrCode.ErrorCode.ERROR_CLIENT_SEND_BUFFER_FULL) {
                        throw e;
                    }
                    i--;
                    System.out.printf("send buffer is full, send [%d]\n", i);
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException ee) {
                    }
                }
            }
            testWriter.waitFinished(20 * 1000 * 1000);
            testWriter.close();
            testWriter = null;
            long writeUseTime = System.currentTimeMillis() - startTime;
            long writeSpeed = sendMsgCnt * 1000 / writeUseTime;
            double writeBandwidth = writeSpeed * message_size / 1024.0 / 1024.0;
            System.out.printf("write msg count [%d], msg size [%d] byte, data size [%d] byte, use time [%d] ms, speed [%d] Msg/s, bandwidth [%f] MByte/s.\n",
                        sendMsgCnt, message_size, msgSize, writeUseTime, writeSpeed, writeBandwidth);
            // read the message
            if (readerConfig.isEmpty()) {
                readerConfig = "topicName=" + topicName;
            }
            testReader = testClient.createReader(readerConfig);
            startTime = System.currentTimeMillis();
            long readCnt = 0;
	    while (readCnt < sendMsgCnt) {
		try {
                    SwiftMessage.Message readMsg;
                    readMsg = testReader.read();
                    readCnt++;
		} catch (SwiftException e) {
		    e.printStackTrace();
		}
	    }
            long readUseTime = System.currentTimeMillis() - startTime;
            long readSpeed = readCnt * 1000 / readUseTime;
            double readBandwidth = readSpeed * message_size / 1024.0 / 1024.0;
            System.out.printf("read msg count [%d], use time [%d] ms, speed [%d] Msg/s, bandwidth [%f] MByte/s.\n", readCnt, readUseTime,
                    readSpeed, readBandwidth);
            testReader.close();
            testReader = null;
        } catch (SwiftException e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
        } finally {
            if (testSwiftAdmin != null) {
                try {
                    testSwiftAdmin.deleteTopic(topicName);
                } catch (SwiftException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            if (testWriter != null) {
                testWriter.close();
            }
            if (testReader != null) {
                testReader.close();
            }
            testClient.close();
        }
    }
}
