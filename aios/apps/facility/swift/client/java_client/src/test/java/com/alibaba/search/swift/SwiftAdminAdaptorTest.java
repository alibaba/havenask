package com.alibaba.search.swift;

import com.alibaba.search.swift.SwiftAdminAdaptor;
import com.alibaba.search.swift.SwiftClient;
import com.alibaba.search.swift.exception.SwiftException;
import com.alibaba.search.swift.protocol.AdminRequestResponse;
import com.alibaba.search.swift.protocol.Common;
import com.alibaba.search.swift.protocol.ErrCode;
import com.alibaba.search.swift.SwiftTestConf;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SwiftAdminAdaptorTest {
    private SwiftClient testClient = null;

    @Before
    public void before() throws Exception {
        String configStr = SwiftTestConf.getSwiftConfig();
        testClient = new SwiftClient();
        try {
            testClient.init(configStr);
        } catch (SwiftException e) {
            e.printStackTrace();
            fail(e.toString());
        }
    }

    @After
    public void after() throws Exception {
        if (testClient != null) {
            testClient.close();
        }
    }

    @Test
    public void testSimpleProcess() throws Exception {
        //create topic and wait topic ready
        SwiftAdminAdaptor testSwiftAdmin = testClient.getAdminAdapter();
        String topicName = "test_swift_topic_1111";
        try {
            // create topic
            AdminRequestResponse.TopicCreationRequest.Builder request = AdminRequestResponse.TopicCreationRequest
                    .newBuilder();
            request.setTopicName(topicName);
            request.setPartitionCount(5);
            request.setPartitionMaxBufferSize(512);
            request.setResource(10);
            request.setDeleteTopicData(true);
            testSwiftAdmin.createTopic(request.build());
            // wait topic ready
            assertTrue(testSwiftAdmin.waitTopicReady(topicName, 20));
            //get topic info
            AdminRequestResponse.TopicInfoResponse response = testSwiftAdmin.getTopicInfo(topicName);
            Common.TopicInfo topicInfo = response.getTopicInfo();
            assertEquals(topicName, topicInfo.getName());
            assertEquals(5, topicInfo.getPartitionCount());
            assertEquals(512, topicInfo.getPartitionMaxBufferSize());
            assertEquals(10, topicInfo.getResource());
            //get partition count
            int partCount = testSwiftAdmin.getPartitionCount(topicName);
            assertEquals(5, partCount);
            // get broker address and partition info
            for (int i = 0; i < partCount; i++) {
                AdminRequestResponse.PartitionInfoResponse partInfo = testSwiftAdmin.getPartitionInfo(topicName, i);
                String brokerAddress1 = partInfo.getPartitionInfos(0).getBrokerAddress();
                String brokerAddress2 = testSwiftAdmin.getBrokerAddress(topicName, i);
                assertEquals(brokerAddress1, brokerAddress2);
                assertTrue(brokerAddress1.indexOf(':') != -1);
            }
            //get all topic info
            AdminRequestResponse.AllTopicInfoResponse allTopicInfo = testSwiftAdmin.getAllTopicInfo();
            int count = allTopicInfo.getAllTopicInfoCount();
            assertTrue(count >= 1);
            // delete topic
            testSwiftAdmin.deleteTopic(topicName);
            try {
                testSwiftAdmin.getTopicInfo(topicName);
            } catch (SwiftException e) {
                assertEquals(ErrCode.ErrorCode.ERROR_ADMIN_TOPIC_NOT_EXISTED, e.getEc());
            }
        } catch (SwiftException e) {
            e.printStackTrace();
            fail(e.toString());
        } finally {
            if (testSwiftAdmin != null) {
                try {
                    testSwiftAdmin.deleteTopic(topicName);
                } catch (SwiftException e) {
                }
            }
            testSwiftAdmin.close();
        }
    }


} 
