package com.alibaba.search.swift;

import com.alibaba.search.swift.SwiftAdminAdaptor;
import com.alibaba.search.swift.SwiftClient;
import com.alibaba.search.swift.SwiftReader;
import com.alibaba.search.swift.SwiftWriter;
import com.alibaba.search.swift.exception.SwiftException;
import com.alibaba.search.swift.protocol.AdminRequestResponse;
import com.alibaba.search.swift.protocol.ErrCode;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import java.util.Vector;

public class SwiftClientTest {
    private String configStr = SwiftTestConf.getSwiftConfig();
    class MyThread extends Thread{
        public void run() {
            for (int i = 0; i <20 ; i++) {
                System.out.println(i);
                SwiftClient testClient = new SwiftClient();
                try {
                    testClient.init(configStr);
                    String writeConfig = "topicName=not_exist_writer_1111";
                    try {
                        testClient.createWriter(writeConfig);
                    } catch (SwiftException e) {
                        assertEquals(ErrCode.ErrorCode.ERROR_ADMIN_TOPIC_NOT_EXISTED, e.getEc());
                    }
                } catch (SwiftException e) {
                    e.printStackTrace();
                    fail(e.toString());
                } finally {
                    testClient.close();
                }
            }
        }
    }
    class MyThreadCreateWriter extends Thread{
        private  SwiftClient _testClient = null;
        public MyThreadCreateWriter(SwiftClient testClient) {
            _testClient = testClient;
        }
        public void run() {
            for (int i = 0; i <10 ; i++) {
                try {
                    String writeConfig = "topicName=sdd_test";
                    SwiftWriter writer = _testClient.createWriter(writeConfig);
                    writer.close();
                } catch (SwiftException e) {
                    e.printStackTrace();
                    fail(e.toString());
                }
            }
        }
    }

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    @Test
    public void testInit() throws Exception {
        SwiftClient testClient = new SwiftClient();
        try {
            testClient.init(configStr);
        } catch (SwiftException e) {
            e.printStackTrace();
            fail(e.toString());
        } finally {
            testClient.close();
        }
    }

    @Test
    public void testInitWithMultiThread() throws Exception {
        int threadNum = 10;
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

    @Test
    public void testCreateReader() throws Exception {
        SwiftClient testClient = new SwiftClient();
        String topicName = "swift_java_test_create_reader_1111";
        try {
            testClient.init(configStr);
            String readConfig = "topicName=not_exist_reader_11";
            try {
                testClient.createReader(readConfig);
            } catch (SwiftException e) {
                assertEquals(ErrCode.ErrorCode.ERROR_ADMIN_TOPIC_NOT_EXISTED, e.getEc());
            }
            boolean ret = createTopic(topicName, testClient);
            assertTrue(ret);
            readConfig = "topicName=" + topicName;
            SwiftReader testReader = testClient.createReader(readConfig);
            assertTrue(testReader.getReaderPtr().longValue() != 0);
        } catch (SwiftException e) {
            e.printStackTrace();
            fail(e.toString());
        } finally {
            deleteTopic(topicName, testClient);
            testClient.close();
        }
    }

    @Test
    public void testCreateWriter() throws Exception {
        SwiftClient testClient = new SwiftClient();
        String topicName = "swift_java_test_create_writer_1111";
        try {
            testClient.init(configStr);
            String writeConfig = "topicName=not_exist_writer_1111";
            try {
                testClient.createWriter(writeConfig);
            } catch (SwiftException e) {
                assertEquals(ErrCode.ErrorCode.ERROR_ADMIN_TOPIC_NOT_EXISTED, e.getEc());
            }
            boolean ret = createTopic(topicName, testClient);
            assertTrue(ret);
            writeConfig = "topicName=" + topicName;
            SwiftWriter testWriter = testClient.createWriter(writeConfig);
            assertTrue(testWriter.getWriterPtr().longValue() != 0);
        } catch (SwiftException e) {
            e.printStackTrace();
            fail(e.toString());
        } finally {
            deleteTopic(topicName, testClient);
            testClient.close();
        }
    }

    @Test
    public void testCreateWriterMultiThread() throws Exception {
        SwiftClient testClient = new SwiftClient();
        try {
            testClient.init(configStr);
            int threadNum = 100;
            Vector<Thread> list = new Vector<Thread>();
            for (int i = 0; i < threadNum; i++) {
                MyThreadCreateWriter thread = new MyThreadCreateWriter(testClient);
                thread.start();
                //Thread.sleep(100);
                list.add(thread);
            }
            for (int i = 0; i < list.size(); i++) {
                list.get(i).join();
            }
        } catch (SwiftException e) {
            e.printStackTrace();
            fail(e.toString());
        } finally {
            testClient.close();
        }
    }

    /**
     * Method: getAdminAdapter()
     */
    @Test
    public void testGetAdminAdapter() throws Exception {
        SwiftClient testClient = new SwiftClient();
        SwiftAdminAdaptor testSwiftAdmin = null;
        try {
            testClient.init(configStr);
            testSwiftAdmin = testClient.getAdminAdapter();
            assertTrue(testSwiftAdmin.getAdminPtr().longValue() != 0);
        } catch (SwiftException e) {
            e.printStackTrace();
            fail(e.toString());
        } finally {
            if (testSwiftAdmin != null) {
                testSwiftAdmin.close();
            }
            testClient.close();
        }

    }


    /**
     * Method: extractBinary(String[] libs)
     */
    @Test
    public void testExtractBinary() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = SwiftClient.getClass().getMethod("extractBinary", String[].class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    private boolean createTopic(String topicName, SwiftClient testClient) {
        try {
            SwiftAdminAdaptor testSwiftAdmin = testClient.getAdminAdapter();
            AdminRequestResponse.TopicCreationRequest.Builder request = AdminRequestResponse.TopicCreationRequest
                    .newBuilder();
            request.setTopicName(topicName);
            request.setPartitionCount(1);
            request.setResource(1);
            testSwiftAdmin.createTopic(request.build());
            assertTrue(testSwiftAdmin.waitTopicReady(topicName, 20));
        } catch (SwiftException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    private boolean deleteTopic(String topicName, SwiftClient testClient) {
        try {
            testClient.getAdminAdapter().deleteTopic(topicName);
        } catch (SwiftException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

} 
