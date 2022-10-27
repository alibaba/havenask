#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/service/ThreadPoolManager.h>

using namespace std;
using namespace autil;
BEGIN_HA3_NAMESPACE(service);

class ThreadPoolManagerTest : public TESTBASE {
public:
    ThreadPoolManagerTest();
    ~ThreadPoolManagerTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(service, ThreadPoolManagerTest);


ThreadPoolManagerTest::ThreadPoolManagerTest() {
}

ThreadPoolManagerTest::~ThreadPoolManagerTest() {
}

void ThreadPoolManagerTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
}

void ThreadPoolManagerTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(ThreadPoolManagerTest, testAddThreadPool1) {
    HA3_LOG(DEBUG, "Begin Test!");
    {
        ThreadPoolManager poolManager;
        string configStr;
        bool ret = poolManager.addThreadPool(configStr);
        ASSERT_TRUE(ret);
        ASSERT_TRUE(poolManager._pools.empty());
        ASSERT_EQ(size_t(0), poolManager.getItemCount());
        ASSERT_EQ(size_t(0), poolManager.getTotalThreadNum());
        ASSERT_EQ(size_t(0), poolManager.getTotalQueueSize());
        ASSERT_TRUE(poolManager.start());
    }
    {
        ThreadPoolManager poolManager;
        string configStr = "search_queue|1|2;summary_queue|3|4";
        bool ret = poolManager.addThreadPool(configStr);
        ASSERT_TRUE(ret);
        ASSERT_EQ(size_t(2), poolManager._pools.size());
        ThreadPool *threadPool = poolManager.getThreadPool("search_queue");
        ASSERT_TRUE(threadPool);
        ASSERT_EQ(size_t(2), threadPool->getThreadNum());
        ASSERT_EQ(size_t(1), threadPool->getQueueSize());

        threadPool = poolManager.getThreadPool("summary_queue");
        ASSERT_TRUE(threadPool);
        ASSERT_EQ(size_t(4), threadPool->getThreadNum());
        ASSERT_EQ(size_t(3), threadPool->getQueueSize());

        ASSERT_EQ(size_t(0), poolManager.getItemCount());
        ASSERT_EQ(size_t(6), poolManager.getTotalThreadNum());
        ASSERT_EQ(size_t(4), poolManager.getTotalQueueSize());

        threadPool = poolManager.getThreadPool("not_exist_queue");
        ASSERT_TRUE(!threadPool);
    }
    {
        ThreadPoolManager poolManager;
        string configStr = "extra_searcher_task_queues = search_queue|500|024\n";
        bool ret = poolManager.addThreadPool(configStr);
        ASSERT_TRUE(!ret);
        ASSERT_TRUE(poolManager._pools.empty());
    }
}

TEST_F(ThreadPoolManagerTest, testAddThreadPool2) {
    ThreadPoolManager poolManager;
    ASSERT_TRUE(poolManager._pools.empty());
    bool ret = poolManager.addThreadPool("pool1", 1, 2);
    ASSERT_TRUE(ret);
    ASSERT_EQ(size_t(1), poolManager._pools.size());
    ThreadPool* threadPool = poolManager.getThreadPool("pool1");
    ASSERT_TRUE(threadPool);
    ASSERT_EQ(size_t(2), threadPool->getThreadNum());
    ASSERT_EQ(size_t(1), threadPool->getQueueSize());

    ret = poolManager.addThreadPool("pool1", 3, 4);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(size_t(1), poolManager._pools.size());
    threadPool = poolManager.getThreadPool("pool1");
    ASSERT_TRUE(threadPool);

    ret = poolManager.addThreadPool("pool2", 3, 4);
    ASSERT_TRUE(ret);
    ASSERT_EQ(size_t(2), poolManager._pools.size());
    threadPool = poolManager.getThreadPool("pool1");
    ASSERT_TRUE(threadPool);
    threadPool = poolManager.getThreadPool("pool2");
    ASSERT_TRUE(threadPool);
    ASSERT_EQ(size_t(4), threadPool->getThreadNum());
    ASSERT_EQ(size_t(3), threadPool->getQueueSize());
}

class FakeWorkItem : public WorkItem {
public:
    FakeWorkItem() {}
    virtual ~FakeWorkItem() {}

public:
    void process() {};
    void destroy() {delete this;};
    void drop() {destroy();};
};

TEST_F(ThreadPoolManagerTest, testPushWorkItem) {
    ThreadPoolManager poolManager;
    string configStr = "search_queue|1|2;summary_queue|3|4";
    bool ret = poolManager.addThreadPool(configStr);
    ASSERT_TRUE(ret);

    ThreadPool* searchPool = poolManager.getThreadPool("search_queue");
    ASSERT_TRUE(searchPool);
    FakeWorkItem *workItem1 = new FakeWorkItem;
    ThreadPool::ERROR_TYPE ec = searchPool->pushWorkItem(workItem1);
    ASSERT_TRUE(ec == ThreadPool::ERROR_NONE);

    ThreadPool* summaryPool = poolManager.getThreadPool("summary_queue");
    ASSERT_TRUE(summaryPool);
    FakeWorkItem *workItem2 = new FakeWorkItem;
    ec = summaryPool->pushWorkItem(workItem2);
    ASSERT_TRUE(ec == ThreadPool::ERROR_NONE);

    ASSERT_EQ(size_t(2), poolManager.getItemCount());

    ASSERT_TRUE(poolManager.start());
    usleep(200000);
    ASSERT_EQ(size_t(0), poolManager.getItemCount());

    poolManager.stop();
    FakeWorkItem *workItem3 = new FakeWorkItem;
    ec = searchPool->pushWorkItem(workItem3);
    ASSERT_TRUE(ec == ThreadPool::ERROR_POOL_HAS_STOP);
    ec = summaryPool->pushWorkItem(workItem3);
    ASSERT_TRUE(ec == ThreadPool::ERROR_POOL_HAS_STOP);
    delete workItem3;
}

END_HA3_NAMESPACE(service);
