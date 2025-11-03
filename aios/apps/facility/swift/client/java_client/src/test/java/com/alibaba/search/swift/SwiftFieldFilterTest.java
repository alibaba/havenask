package com.alibaba.search.swift;

import com.alibaba.search.swift.SwiftAdminAdaptor;
import com.alibaba.search.swift.SwiftClient;
import com.alibaba.search.swift.SwiftReader;
import com.alibaba.search.swift.SwiftWriter;
import com.alibaba.search.swift.exception.SwiftException;
import com.alibaba.search.swift.protocol.AdminRequestResponse;
import com.alibaba.search.swift.protocol.ErrCode;
import com.alibaba.search.swift.protocol.SwiftMessage;
import com.alibaba.search.swift.util.FieldGroupReader;
import com.alibaba.search.swift.util.FieldGroupWriter;
import com.google.protobuf.ByteString;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import org.omg.Messaging.SYNC_WITH_TRANSPORT;

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
public class SwiftFieldFilterTest {
    private SwiftClient testClient = null;
    private SwiftAdminAdaptor testSwiftAdmin = null;
    private String topicName = "swift_java_client_test_topic_name_field_filter";
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
            request.setNeedFieldFilter(true);
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
        if (testSwiftAdmin != null) {
            testSwiftAdmin.deleteTopic(topicName);
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
    public void testWriteAndRead() throws Exception {
        String writeConfig = "topicName=" + topicName;
        SwiftWriter testWriter = null;
        SwiftReader testReader = null;
        String [] filedNames = {"a","b", "c","d","e"};
        int msgCount = 50;
        try {
            FieldGroupWriter groupWriter = new FieldGroupWriter();
            testWriter = testClient.createWriter(writeConfig);
            for (int i = 0; i < msgCount; i++) {
                for (int j =0; j < 5; j++) { //5 field
                    groupWriter.addProductionField(filedNames[j], Integer.toString(i + j), true);
                }
                SwiftMessage.WriteMessageInfo.Builder msg = SwiftMessage.WriteMessageInfo.newBuilder();
                msg.setData(ByteString.copyFrom(groupWriter.toBytes()));
                testWriter.write(msg.build());
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                }
                groupWriter.reset();

            }

            testWriter.waitFinished();

            FieldGroupReader groupReader = new FieldGroupReader();
            String readConfig = "topicName=" + topicName +";requiredFields=a,c,e;fieldFilterDesc=a IN 1|15|49";
            testReader = testClient.createReader(readConfig);

            SwiftMessage.Message msg = testReader.read();
            assertEquals(msg.getMsgId(), 1);
            groupReader.fromConsumptionString(msg.getData().toByteArray());
            assertEquals(groupReader.getFieldSize(), 3);
            assertEquals(groupReader.getField(0).name, null);
            assertEquals(groupReader.getField(0).value, "1");
            assertEquals(groupReader.getField(0).isUpdated, true);
            assertEquals(groupReader.getField(0).isExisted, true);
            assertEquals(groupReader.getField(1).name, null);
            assertEquals(groupReader.getField(1).value, "3");
            assertEquals(groupReader.getField(1).isUpdated, true);
            assertEquals(groupReader.getField(1).isExisted, true);
            assertEquals(groupReader.getField(2).name, null);
            assertEquals(groupReader.getField(2).value, "5");
            assertEquals(groupReader.getField(2).isUpdated, true);
            assertEquals(groupReader.getField(2).isExisted, true);


            msg = testReader.read();
            assertEquals(msg.getMsgId(), 15);
            groupReader.fromConsumptionString(msg.getData().toByteArray());
            assertEquals(groupReader.getFieldSize(), 3);
            assertEquals(groupReader.getField(0).value, "15");
            assertEquals(groupReader.getField(0).isUpdated, true);
            assertEquals(groupReader.getField(0).isExisted, true);
            assertEquals(groupReader.getField(1).value, "17");
            assertEquals(groupReader.getField(1).isUpdated, true);
            assertEquals(groupReader.getField(1).isExisted, true);
            assertEquals(groupReader.getField(2).value, "19");
            assertEquals(groupReader.getField(2).isUpdated, true);
            assertEquals(groupReader.getField(2).isExisted, true);

            msg = testReader.read();
            assertEquals(msg.getMsgId(), 49);
            groupReader.fromConsumptionString(msg.getData().toByteArray());
            assertEquals(groupReader.getFieldSize(), 3);
            assertEquals(groupReader.getField(0).value, "49");
            assertEquals(groupReader.getField(0).isUpdated, true);
            assertEquals(groupReader.getField(0).isExisted, true);
            assertEquals(groupReader.getField(1).value, "51");
            assertEquals(groupReader.getField(1).isUpdated, true);
            assertEquals(groupReader.getField(1).isExisted, true);
            assertEquals(groupReader.getField(2).value, "53");
            assertEquals(groupReader.getField(2).isUpdated, true);
            assertEquals(groupReader.getField(2).isExisted, true);
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

}
