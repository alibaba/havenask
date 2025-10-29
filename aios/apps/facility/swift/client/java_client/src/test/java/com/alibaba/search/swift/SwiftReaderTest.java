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

import java.nio.LongBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.util.Vector;

public class SwiftReaderTest {
    private SwiftClient testClient = null;
    private SwiftAdminAdaptor testSwiftAdmin = null;
    private String topicName = "swift_java_client_test_topic_name_reader";

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
            request.setPartitionCount(1);
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
     * Method: read()
     */
    @Test
    public void testReadWhenClientClosed() throws Exception {
        int msgCount = 5;
        sendMsg(msgCount);
        SwiftReader testReader = null;
        SwiftReader testReader2 = null;
        long[] times = new long[msgCount];
        try {
            String readConfig = "topicName=" + topicName;
            testReader = testClient.createReader(readConfig);
            SwiftMessage.Message msg;
            msg = testReader.read();
            assertEquals(new String(msg.getData().toByteArray()), Integer.toString(0));
            assertEquals(0, msg.getMsgId());
            testClient.close();
            boolean hasError = false;
            try {
                msg = testReader.read();
            } catch(SwiftException e) {
                hasError = true;
            }
            assertTrue(hasError);
            hasError = false;
            try {
                testReader2 = testClient.createReader(readConfig);
            } catch(SwiftException e) {
                hasError = true;
            }
            assertTrue(hasError);
            assertTrue(testReader2 == null);
        } catch (SwiftException e) {
            e.printStackTrace();
            fail(e.toString());
        } finally {
            if (testReader != null) {
                testReader.close();
            }
            if (testReader2 != null) {
                testReader2.close();
            }
        }
    }

    /**
     * Method: read()
     */
    @Test
    public void testRead() throws Exception {
        int msgCount = 5;
        sendMsg(msgCount);
        SwiftReader testReader = null;
        SwiftReader testReader2 = null;
        long[] times = new long[msgCount];
        try {
            String readConfig = "topicName=" + topicName;
            testReader = testClient.createReader(readConfig);
            SwiftMessage.Message msg;
            for (int i = 0; i < msgCount; i++) {
                msg = testReader.read();
                assertEquals(new String(msg.getData().toByteArray()), Integer.toString(i));
                assertEquals(i, msg.getMsgId());
                times[i] = msg.getTimestamp();
            }
            testReader2 = testClient.createReader(readConfig);
            LongBuffer buffer = LongBuffer.allocate(1);
            for (int i = 0; i < msgCount; i++) {
                msg = testReader2.read(buffer);
                assertEquals(new String(msg.getData().toByteArray()), Integer.toString(i));
                assertTrue(msg.getTimestamp() < buffer.get(0));
                assertTrue(buffer.get(0) != 0);
                assertEquals(i, msg.getMsgId());
                buffer.clear();
            }
        } catch (SwiftException e) {
            e.printStackTrace();
            fail(e.toString());
        } finally {
            if (testReader != null) {
                testReader.close();
            }
            if (testReader2 != null) {
                testReader2.close();
            }
        }
    }

    /**
     * Method: read()
     */
    @Test
    public void testReads() throws Exception {
        int msgCount = 5;
        sendMsg(msgCount);
        SwiftReader testReader = null;
        SwiftReader testReader2 = null;
        SwiftReader testReader3 = null;
        long[] times = new long[msgCount];
        try {
            String readConfig = "topicName=" + topicName +";batchReadCount=5";
            testReader = testClient.createReader(readConfig);
            SwiftMessage.Messages msgs = testReader.reads();
            assertEquals(5, msgs.getMsgsCount());
            for (int i = 0; i < msgs.getMsgsCount(); i++) {
                SwiftMessage.Message msg = msgs.getMsgs(i);
                assertEquals(new String(msg.getData().toByteArray()), Integer.toString(i));
                assertEquals(i, msg.getMsgId());
                times[i] = msg.getTimestamp();
            }

            testReader2 = testClient.createReader(readConfig);
            LongBuffer buffer = LongBuffer.allocate(1);
            for (int i = 0; i < msgCount; i++) {
                SwiftMessage.Message msg = testReader2.read(buffer);
                assertEquals(new String(msg.getData().toByteArray()), Integer.toString(i));
                assertTrue(msg.getTimestamp() < buffer.get(0));
                assertTrue(buffer.get(0) != 0);
                assertEquals(i, msg.getMsgId());
                buffer.clear();
            }

            testReader3 = testClient.createReader(readConfig);
            msgs = testReader3.reads(buffer);
            assertEquals(5, msgs.getMsgsCount());
            assertTrue(times[msgCount - 1] < buffer.get(0));
        } catch (SwiftException e) {
            e.printStackTrace();
            fail(e.toString());
        } finally {
            if (testReader != null) {
                testReader.close();
            }
            if (testReader2 != null) {
                testReader2.close();
            }
            if (testReader3 != null) {
                testReader3.close();
            }
        }
    }

    class MyThread extends Thread{
        public void run() {
	    int msgCount = 50;
            for (int k = 0; k <100 ; k++) {
		SwiftReader testReader = null;
		long[] times = new long[msgCount];
		try {
		    String readConfig = "topicName=" + topicName;
		    testReader = testClient.createReader(readConfig);
		    SwiftMessage.Message msg;
		    for (int i = 0; i < msgCount; i++) {
			msg = testReader.read();
			assertEquals(new String(msg.getData().toByteArray()), Integer.toString(i));
			assertEquals(i, msg.getMsgId());
			times[i] = msg.getTimestamp();
		    }
		} catch (SwiftException e) {
		    e.printStackTrace();
		    fail(e.toString());
		} finally {
		    if (testReader != null) {
			testReader.close();
		    }
		}
	    }
	}
    }
    /**
     * Method: multi_read()
     */
    @Test
    public void testMultiRead() throws Exception {
        int msgCount = 50;
        sendMsg(msgCount);
	int threadNum = 20;
        Vector<Thread> list = new Vector<Thread>();
        for (int i = 0; i < threadNum; i++) {
            MyThread thread = new MyThread();
            thread.start();
            Thread.sleep(100);
            list.add(thread);
        }
        for (int i = 0; i < list.size(); i++) {
            list.get(i).join();
        }

    }
    /**
     * Method: getPartitionStatus()
     */
    @Test
    public void testGetPartitionStatus() throws Exception {
        int msgCount = 5;
        sendMsg(msgCount);
        SwiftReader testReader = null;
        try {
            String readConfig = "topicName=" + topicName;
            testReader = testClient.createReader(readConfig);
            int time = 10;
            while(time-- > 0) {
                LongBuffer refTime = LongBuffer.allocate(1);
                LongBuffer maxId = LongBuffer.allocate(1);
                LongBuffer maxTime = LongBuffer.allocate(1);
                testReader.getPartitionStatus(refTime, maxId, maxTime);
                if(maxId.get(0)>= msgCount -1) {
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
            assertTrue(time > 0);
        } catch (SwiftException e) {
            e.printStackTrace();
            fail(e.toString());
        } finally {
            if (testReader != null) {
                testReader.close();
            }
        }
    }
    /**
     * Method: seekByTimestamp(long timeStamp, boolean force)
     */
    @Test
    public void testSeekByTimestamp() throws Exception {
        int msgCount = 5;
        sendMsg(msgCount);
        SwiftReader testReader = null;
        long[] times = new long[msgCount];
        try {
            String readConfig = "topicName=" + topicName;
            testReader = testClient.createReader(readConfig);
            SwiftMessage.Message msg;
            for (int i = 0; i < msgCount; i++) {
                msg = testReader.read();
                times[i] = msg.getTimestamp();
                assertEquals(new String(msg.getData().toByteArray()), Integer.toString(i));
                assertEquals(i, msg.getMsgId());
            }
            //normal seek
            testReader.seekByTimestamp(times[2], false);
            try {
                testReader.read();
            } catch (SwiftException e) {
                assertEquals(ErrCode.ErrorCode.ERROR_CLIENT_NO_MORE_MESSAGE, e.getEc());
            }
            // force seek
            testReader.seekByTimestamp(times[2], true);
            for (int i = 2; i < msgCount; i++) {
                msg = testReader.read();
                times[i] = msg.getTimestamp();
                assertEquals(new String(msg.getData().toByteArray()), Integer.toString(i));
                assertEquals(i, msg.getMsgId());
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
            if (testReader != null) {
                testReader.close();
            }
        }
    }


    /**
     * Method: seekByMessageId(long msgId)
     */
    @Test
    public void testSeekByMessageId() throws Exception {
        int msgCount = 5;
        sendMsg(msgCount);
        SwiftReader testReader = null;
        long seekMsgId = 3;
        try {
            String readConfig = "topicName=" + topicName;
            testReader = testClient.createReader(readConfig);
            SwiftMessage.Message msg;
            testReader.seekByMessageId(seekMsgId);
            for (long i = seekMsgId; i < msgCount; i++) {
                msg = testReader.read();
                assertEquals(new String(msg.getData().toByteArray()), Long.toString(i));
                assertEquals(i, msg.getMsgId());
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
            if (testReader != null) {
                testReader.close();
            }
        }
    }

    /**
     * Method: setTimestampLimit(long timeLimit, Long acceptTimestamp)
     */
    @Test
    public void testSetTimestampLimit() throws Exception {
        int msgCount = 5;
        sendMsg(msgCount);
        SwiftReader testReader = null;
        long[] times = new long[msgCount];
        try {
            String readConfig = "topicName=" + topicName;
            testReader = testClient.createReader(readConfig);
            SwiftMessage.Message msg;
            for (int i = 0; i < msgCount; i++) {
                msg = testReader.read();
                times[i] = msg.getTimestamp();
                assertEquals(new String(msg.getData().toByteArray()), Integer.toString(i));
                assertEquals(i, msg.getMsgId());
            }
            LongBuffer acceptTime = LongBuffer.allocate(1);
            testReader.setTimestampLimit(times[msgCount - 1], acceptTime);
            assertEquals(times[msgCount - 1], acceptTime.get(0));

            try {
                testReader.read();
            } catch (SwiftException e) {
                assertEquals(ErrCode.ErrorCode.ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT, e.getEc());
            }
            // force seek
            testReader.seekByTimestamp(times[1], true);
            acceptTime.clear();
            testReader.setTimestampLimit(times[msgCount - 2], acceptTime);
            assertEquals(times[msgCount - 2], acceptTime.get(0));
            for (int i = 1; i < msgCount - 1; i++) {
                msg = testReader.read();
                times[i] = msg.getTimestamp();
                assertEquals(new String(msg.getData().toByteArray()), Integer.toString(i));
                assertEquals(i, msg.getMsgId());
            }
            try {
                testReader.read();
            } catch (SwiftException e) {
                assertEquals(ErrCode.ErrorCode.ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT, e.getEc());
            }
        } catch (SwiftException e) {
            e.printStackTrace();
            fail(e.toString());
        } finally {
            if (testReader != null) {
                testReader.close();
            }
        }
    }

    private void sendMsg(int msgCount) {
        String writeConfig = "topicName=" + topicName;
        SwiftWriter testWriter = null;
        try {
            testWriter = testClient.createWriter(writeConfig);
            for (int i = 0; i < msgCount; i++) {
                SwiftMessage.WriteMessageInfo.Builder msg = SwiftMessage.WriteMessageInfo.newBuilder();
                msg.setData(ByteString.copyFrom(Integer.toString(i).getBytes()));
                testWriter.write(msg.build());
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                }
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
