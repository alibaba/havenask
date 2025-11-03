package com.alibaba.search.swift;

import com.alibaba.search.swift.SwiftAdminAdaptor;
import com.alibaba.search.swift.SwiftClient;
import com.alibaba.search.swift.SwiftReader;
import com.alibaba.search.swift.SwiftWriter;
import com.alibaba.search.swift.exception.SwiftException;
import com.alibaba.search.swift.protocol.AdminRequestResponse;
import com.alibaba.search.swift.protocol.ErrCode;
import com.alibaba.search.swift.protocol.SwiftMessage;
import com.google.protobuf.ByteString;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * SwiftWriter Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>十一月 18, 2015</pre>
 */
public class SwiftMultiWriterTest2 {
    private SwiftClient testClient = null;
    private SwiftAdminAdaptor testSwiftAdmin = null;
    private String [] topicNames = {"swift_java_client_test_topic_name_multi_writer", "swift_java_client_test_topic_name_multi_writer1" };
    private String zkPath = "zfs://10.101.170.89:12181/swift/swift_service";
    private String zkPath1 = "zfs://10.101.170.90:12181/swift/swift_service";

    @Before
    public void before() throws Exception {
        String configStr = "zkPath=" + zkPath + ";logConfigFile=./src/main/resources/linux-x86-64/swift_alog.conf;useFollowerAdmin=false"
                 +"##zkPath=" + zkPath1 + ";logConfigFile=./src/main/resources/linux-x86-64/swift_alog.conf;useFollowerAdmin=false";
        testClient = new SwiftClient();
        try {
            testClient.init(configStr);
            for (int i = 0; i < topicNames.length; i++) {
                topicNames[i] = topicNames[i] + "_" + System.currentTimeMillis();
                AdminRequestResponse.TopicCreationRequest.Builder request = AdminRequestResponse.TopicCreationRequest
                        .newBuilder();
                request.setTopicName(topicNames[i]);
                request.setPartitionCount(2);
                request.setPartitionMaxBufferSize(512);
                request.setResource(10);
                request.setDeleteTopicData(true);
                testSwiftAdmin = testClient.getAdminAdapter(zkPath);
                testSwiftAdmin.createTopic(request.build());
                assertTrue(testSwiftAdmin.waitTopicReady(topicNames[i], 20));
            }
        } catch (SwiftException e) {
            e.printStackTrace();
            fail(e.toString());
        }
    }

    @After
    public void after() throws Exception {
        if (testSwiftAdmin != null) {
            for (int i = 0; i < topicNames.length; i++) {
                testSwiftAdmin.deleteTopic(topicNames[i]);
            }
            testSwiftAdmin.close();
        }
        if (testClient != null) {
            testClient.close();
        }
    }

    /**
     * Method: write(SwiftMessage.WriteMessageInfo msg)
     */
    @Test
    public void testMultiWrite2() throws Exception {
        String writeConfig = "zkPath=" + zkPath + ";topicName=" + topicNames[0] + "##topicName=" +topicNames[1] + ";zkPath=" +zkPath1;
        SwiftWriter testWriter = null;
        SwiftReader testReader = null;
        SwiftReader testReader1 = null;
        int msgCount = 50;
        try {
            testWriter = testClient.createWriter(writeConfig);
            for (int i = 0; i < msgCount; i++) {
                SwiftMessage.WriteMessageInfo.Builder msg = SwiftMessage.WriteMessageInfo.newBuilder();
                msg.setData(ByteString.copyFrom(Integer.toString(i).getBytes()));
                msg.setCheckpointId(i * 100);
                testWriter.write(msg.build());
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                }
            }
            testWriter.waitFinished();

            String readConfig = "zkPath="+ zkPath +";topicName=" + topicNames[0];
            testReader = testClient.createReader(readConfig);

            for (int i = 0; i < msgCount; i++) {
                testReader.read();
            }
            try {
                testReader.read();
            } catch (SwiftException e) {
                assertEquals(ErrCode.ErrorCode.ERROR_CLIENT_NO_MORE_MESSAGE, e.getEc());
            }


            String readConfig1 = "zkPath=" +zkPath1 + ";topicName=" + topicNames[1];
            testReader1 = testClient.createReader(readConfig1);

            for (int i = 0; i < msgCount; i++) {
                testReader1.read();
            }
            try {
                testReader1.read();
            } catch (SwiftException e) {
                assertEquals(ErrCode.ErrorCode.ERROR_CLIENT_NO_MORE_MESSAGE, e.getEc());
            }
        } catch (SwiftException e) {
            e.printStackTrace();
            fail(e.toString());
        } finally {
            if (testWriter != null) {
                testWriter.close();
            }
            if (testReader != null) {
                testReader.close();
            }
            if (testReader1 != null) {
                testReader1.close();
            }
        }
    }

}
