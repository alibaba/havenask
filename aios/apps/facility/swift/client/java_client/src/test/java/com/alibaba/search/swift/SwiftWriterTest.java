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
public class SwiftWriterTest {
    private SwiftClient testClient = null;
    private SwiftAdminAdaptor testSwiftAdmin = null;
    private String topicName = "swift_java_client_test_topic_name_writer";

    @Before
    public void before() throws Exception {
        String configStr = SwiftTestConf.getSwiftConfig();
        topicName = topicName + "_" + System.currentTimeMillis();
        testClient = new SwiftClient();
        try {
            testClient.init(configStr);
            AdminRequestResponse.TopicCreationRequest.Builder request = AdminRequestResponse.TopicCreationRequest
                    .newBuilder();
            request.setTopicName(topicName);
            request.setPartitionCount(2);
            request.setPartitionMaxBufferSize(512);
            request.setResource(10);
            request.setDeleteTopicData(true);
            testSwiftAdmin = testClient.getAdminAdapter();
            testSwiftAdmin.createTopic(request.build());
            assertTrue(testSwiftAdmin.waitTopicReady(topicName, 20));
        } catch (SwiftException e) {
            e.printStackTrace();
            fail(e.toString());
        }
    }

    @After
    public void after() throws Exception {
        if (testSwiftAdmin != null && !testSwiftAdmin.isClosed()) {
            testSwiftAdmin.deleteTopic(topicName);
            testSwiftAdmin.close();
        }
        if (testClient != null && !testClient.isClosed()) {
            testClient.close();
        }
    }

    /**
     * Method: write(SwiftMessage.WriteMessageInfo msg)
     */
    @Test
    public void testWrite() throws Exception {
        String writeConfig = "topicName=" + topicName;
        SwiftWriter testWriter = null;
        SwiftReader testReader = null;
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
            String readConfig = "topicName=" + topicName;
            testReader = testClient.createReader(readConfig);

            for (int i = 0; i < msgCount; i++) {
                testReader.read();
            }
            try {
                testReader.read();
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
        }
    }

    @Test
    public void testWriteWhenClientClose() throws Exception {
        String writeConfig = "topicName=" + topicName;
        SwiftWriter testWriter = null;
        int msgCount = 50;
        try {
            testWriter = testClient.createWriter(writeConfig);
            SwiftMessage.WriteMessageInfo.Builder msg = SwiftMessage.WriteMessageInfo.newBuilder();
            msg.setData(ByteString.copyFrom(Integer.toString(10).getBytes()));
            testWriter.write(msg.build());
            testClient.close();
            boolean hasError = false;
            try {
                testWriter.write(msg.build());
            } catch (SwiftException e) {
                hasError = true;
            }
            assertTrue(hasError);
        } catch (SwiftException e) {
            e.printStackTrace();
            fail(e.toString());
        } finally {
            if (testWriter != null) {
                testWriter.close();
            }
        }
    }
    
    /**
     * Method: write(SwiftMessage.WriteMessageInfo msg)
     */
    @Test
    public void testSyncWrite() throws Exception {
        String writeConfig = "topicName=" + topicName +";mode=sync";
        SwiftWriter testWriter = null;
        SwiftReader testReader = null;
        int msgCount = 500;
        try {
            testWriter = testClient.createWriter(writeConfig);
            for (int i = 0; i < msgCount; i++) {
                SwiftMessage.WriteMessageInfo.Builder msg = SwiftMessage.WriteMessageInfo.newBuilder();
                msg.setData(ByteString.copyFrom(Integer.toString(i).getBytes()));
                msg.setCheckpointId(i * 100);
                testWriter.write(msg.build());
            }
            String readConfig = "topicName=" + topicName;
            testReader = testClient.createReader(readConfig);

            for (int i = 0; i < msgCount; i++) {
                testReader.read();
            }
            try {
                testReader.read();
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
        }
    }
    /**
     * Method: write(SwiftMessage.WriteMessageInfoVec msg, bool waitSent)
     */
    @Test
    public void testWriteBatch() throws Exception {
        String writeConfig = "topicName=" + topicName +";mode=async|safe;functionChain=HASH,hashId2partId";
        SwiftWriter testWriter = null;
        SwiftReader testReader = null;
        int msgCount = 500;
        try {
            testWriter = testClient.createWriter(writeConfig);
            int step = 10;
            for (int i = 0; i < msgCount; i = i + step) {
                SwiftMessage.WriteMessageInfoVec.Builder msgVec = SwiftMessage.WriteMessageInfoVec.newBuilder();
                for (int j = 0; j < step; j++) {
                    SwiftMessage.WriteMessageInfo.Builder msg = SwiftMessage.WriteMessageInfo.newBuilder();
                    msg.setData(ByteString.copyFrom(Integer.toString(i).getBytes()));
                    msg.setCheckpointId(i * 100);
                    msg.setHashStr(ByteString.copyFromUtf8(Long.toString(i)));
                    msgVec.addMessageInfoVec(msg.build());
                }
                testWriter.write(msgVec.build(), true);
            }
            String readConfig = "topicName=" + topicName;
            testReader = testClient.createReader(readConfig);

            for (int i = 0; i < msgCount; i++) {
                testReader.read();
            }
            try {
                testReader.read();
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
        }
    }

    /**
     * Method: write(SwiftMessage.WriteMessageInfoVec msg, bool waitSent)
     */
    @Test
    public void testWriteBatchWaitSent() throws Exception {
        String writeConfig = "topicName=" + topicName +";mode=async|safe;functionChain=HASH,hashId2partId";
        SwiftWriter testWriter = null;
        SwiftReader testReader = null;
        int msgCount = 500;
        try {
            testWriter = testClient.createWriter(writeConfig);
            int step = 10;
            for (int i = 0; i < msgCount; i = i + step) {
                SwiftMessage.WriteMessageInfoVec.Builder msgVec = SwiftMessage.WriteMessageInfoVec.newBuilder();
                for (int j = 0; j < step; j++) {
                    SwiftMessage.WriteMessageInfo.Builder msg = SwiftMessage.WriteMessageInfo.newBuilder();
                    msg.setData(ByteString.copyFrom(Integer.toString(i).getBytes()));
                    msg.setCheckpointId(i * 100);
                    msg.setHashStr(ByteString.copyFromUtf8(Long.toString(i)));
                    msgVec.addMessageInfoVec(msg.build());
                }
                testWriter.write(msgVec.build(), false);
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                }
            }

            testWriter.waitSent();

            String readConfig = "topicName=" + topicName;
            testReader = testClient.createReader(readConfig);

            for (int i = 0; i < msgCount; i++) {
                testReader.read();
            }
            try {
                testReader.read();
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
        }
    }

    @Test
    public void testGetCommittedCheckpointId() throws Exception {
        String writeConfig = "topicName=" + topicName + ";mode=async|unsafe";
        SwiftWriter testWriter = null;
        try {
            testWriter = testClient.createWriter(writeConfig);
            for (int i = 0; i < 5; i++) {
                SwiftMessage.WriteMessageInfo.Builder msg = SwiftMessage.WriteMessageInfo.newBuilder();
                msg.setData(ByteString.copyFrom(Integer.toString(i).getBytes()));
                msg.setCheckpointId(i * 100);
                testWriter.write(msg.build());
                testWriter.waitSent();
                assertEquals(i * 100, testWriter.getCommittedCheckpointId());
            }
        } catch (SwiftException e) {
            e.printStackTrace();
            fail(e.toString());
        } finally {
            if (testWriter != null) {
                testWriter.close();
            }
        }
    }


}
