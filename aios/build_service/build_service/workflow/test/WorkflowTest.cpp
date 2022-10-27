#include "build_service/test/unittest.h"
#include "build_service/workflow/Workflow.h"
#include "build_service/common/Locator.h"
#include "build_service/workflow/test/MockConsumer.h"
#include "build_service/workflow/test/MockProducer.h"
#include <autil/Lock.h>

using namespace std;
using namespace testing;

namespace build_service {
namespace workflow {

class AutoFinishProducer : public Producer<int> {
public:
    AutoFinishProducer(int finishId, int64_t sleepTime = 0)
        : _idNow(0)
        , _finishId(finishId)
        , _sleepTime(sleepTime)
        , _stopped(false)
    {
    }
    ~AutoFinishProducer() {
    }
public:
    /* override */ FlowError produce(int &id) {
        if (_idNow >= _finishId || _stopped) {
            return FE_EOF;
        }
        if (_sleepTime > 0) {
            usleep(_sleepTime);
        }
        id = ++_idNow;
        return FE_OK;
    }
    /* override */ bool seek(const common::Locator &locator) {
        return false;
    }
    /* override */ bool stop() {
        _stopped = true;
        return true;
    }
    void setFinishId(int finishId) {
        _finishId = finishId;
    }
private:
    int _idNow;
    int _finishId;
    int64_t _sleepTime;
    bool _stopped;
};

class CounterConsumer : public Consumer<int> {
public:
    CounterConsumer(std::vector<int> &output)
        : _output(output)
    {
    }
    ~CounterConsumer() {
    }
public:
    /* override */ FlowError consume(const int &id) {
        _output.push_back(id);
        return FE_OK;
    }
    /* override */ bool getLocator(common::Locator &locator) const {
        return true;
    }
    /* override */ bool stop(FlowError lastError) {
        return true;
    }
public:
    vector<int>& _output;
};

class WorkflowTest : public BUILD_SERVICE_TESTBASE {
protected:
    void checkCounter(const std::vector<int>& ids, int counter) {
        EXPECT_EQ(size_t(counter), ids.size());
        for (size_t i = 0; i < ids.size(); i++) {
            EXPECT_EQ(int(i+1), ids[i]);
        }
    }
};

// job, local build
TEST_F(WorkflowTest, testAutoFinishProducer) {
    std::vector<int> ids;
    AutoFinishProducer *producer = new AutoFinishProducer(10);
    CounterConsumer *consumer = new CounterConsumer(ids);
    Workflow<int> workflow(producer, consumer);
    workflow.start(JOB);
    while (!workflow.isFinished() && !workflow.hasFatalError()) usleep(1);
    checkCounter(ids, 10);
    EXPECT_FALSE(workflow.hasFatalError());
    EXPECT_TRUE(workflow.isFinished());
}

TEST_F(WorkflowTest, testGetStopLocator) {
    {
        MockConsumer<int> *consumer = new StrictMock<MockConsumer<int> >;
        MockProducer<int> *producer = new StrictMock<MockProducer<int> >;
        Workflow<int> flow(producer, consumer);
        EXPECT_CALL(*consumer, getLocator(_)).WillOnce(DoAll(
                        SetArgReferee<0>(common::Locator(100,200)), Return(true)));
        EXPECT_CALL(*producer, produce(_))
            .WillOnce(Return(FE_OK))
            .WillOnce(Return(FE_EOF));
        EXPECT_CALL(*consumer, consume(_)).Times(1);
        EXPECT_CALL(*consumer, stop(_)).WillOnce(Return(true));
        EXPECT_CALL(*producer, stop()).WillOnce(Return(true));
        flow.start(SERVICE);
        while (flow.isRunning()) usleep(1);
        sleep(1);
        EXPECT_FALSE(flow.hasFatalError());
        EXPECT_EQ(common::Locator(100, 200), flow.getStopLocator());
    }
    {
        MockConsumer<int> *consumer = new StrictMock<MockConsumer<int> >;
        MockProducer<int> *producer = new StrictMock<MockProducer<int> >;
        Workflow<int> flow(producer, consumer);
        EXPECT_CALL(*consumer, getLocator(_)).WillOnce(Return(false));
        EXPECT_CALL(*producer, produce(_))
            .WillOnce(Return(FE_OK))
            .WillOnce(Return(FE_EOF));
        EXPECT_CALL(*consumer, consume(_)).Times(1);
        EXPECT_CALL(*consumer, stop(_)).WillOnce(Return(true));
        EXPECT_CALL(*producer, stop()).WillOnce(Return(true));
        flow.start(SERVICE);
        while (flow.isRunning()) usleep(1);
        sleep(1);
        EXPECT_TRUE(flow.hasFatalError());
    }
}

TEST_F(WorkflowTest, testHandleError) {
    {
        MockConsumer<int> *consumer1 = new StrictMock<MockConsumer<int> >;
        MockProducer<int> *producer1 = new StrictMock<MockProducer<int> >;
        Workflow<int> flow1(producer1, consumer1);
        EXPECT_CALL(*producer1, stop()).Times(1);
        EXPECT_CALL(*consumer1, stop(_)).Times(1);
        EXPECT_CALL(*producer1, produce(_))
            .WillOnce(Return(FE_OK))
            .WillOnce(Return(FE_EXCEPTION))
            .WillOnce(Return(FE_OK))
            .WillOnce(Return(FE_WAIT))
            .WillOnce(Return(FE_OK))
            .WillOnce(Return(FE_SKIP))
            .WillOnce(Return(FE_EOF));
        EXPECT_CALL(*consumer1, consume(_)).Times(3);
        EXPECT_CALL(*consumer1, getLocator(_)).Times(1);
        flow1.start(SERVICE);
        while (flow1.isRunning()) usleep(1);
        EXPECT_FALSE(flow1.hasFatalError());
    }
    {
        MockConsumer<int> *consumer1 = new StrictMock<MockConsumer<int> >;
        MockProducer<int> *producer1 = new StrictMock<MockProducer<int> >;
        Workflow<int> flow1(producer1, consumer1);
        EXPECT_CALL(*producer1, stop()).Times(1);
        EXPECT_CALL(*consumer1, stop(_)).Times(1);
        EXPECT_CALL(*producer1, produce(_))
            .WillOnce(Return(FE_OK))
            .WillOnce(Return(FE_WAIT))
            .WillOnce(Return(FE_OK))
            .WillOnce(Return(FE_EOF));
        EXPECT_CALL(*consumer1, consume(_)).Times(2);
        EXPECT_CALL(*consumer1, getLocator(_)).Times(1);
        flow1.start(JOB);
        while (flow1.isRunning()) usleep(1);
        EXPECT_FALSE(flow1.hasFatalError());
    }
    {
        MockConsumer<int> *consumer1 = new StrictMock<MockConsumer<int> >;
        MockProducer<int> *producer1 = new StrictMock<MockProducer<int> >;
        Workflow<int> flow1(producer1, consumer1);
        EXPECT_CALL(*producer1, stop()).Times(1);
        EXPECT_CALL(*consumer1, stop(_)).Times(1);
        EXPECT_CALL(*producer1, produce(_))
            .WillOnce(Return(FE_OK))
            .WillOnce(Return(FE_WAIT))
            .WillOnce(Return(FE_OK))
            .WillOnce(Return(FE_EXCEPTION));
        EXPECT_CALL(*consumer1, consume(_)).Times(2);
        flow1.start(JOB);
        while (flow1.isRunning()) usleep(1);
        EXPECT_TRUE(flow1.hasFatalError());
    }
}

TEST_F(WorkflowTest, testServiceModeNotAutoStop) {
    std::vector<int> ids;
    // produce 1 number by 500ms
    AutoFinishProducer *producer = new AutoFinishProducer(10, 10 * 1000); // 10ms
    CounterConsumer *consumer = new CounterConsumer(ids);
    Workflow<int> workflow(producer, consumer);
    workflow.start(SERVICE);

    usleep(1000 * 1000);  // 1s
    checkCounter(ids, 10);

    producer->setFinishId(20);

    usleep(1000 * 1000);  // 1s
    checkCounter(ids, 10);

    workflow.stop();
}

TEST_F(WorkflowTest, testSuspendAndResume) {
    std::vector<int> ids;
    AutoFinishProducer *producer = new AutoFinishProducer(10, 500 * 1000);
    CounterConsumer *consumer = new CounterConsumer(ids);
    Workflow<int> workflow(producer, consumer);
    workflow.suspend();
    workflow.start(SERVICE);
    // sleep for 2.2s
    usleep(2200 * 1000);
    // checkSuspend and workflow don't work
    checkCounter(ids, 0);

    workflow.resume();
    // sleep for 2.1s
    usleep(2100 * 1000);

    checkCounter(ids, 4);

    // sleep for 2.1s
    workflow.suspend();
    usleep(2100 * 1000);

    checkCounter(ids, 5); //one item is producing when suspend. so ids count inc to 5.
    workflow.stop();
}

TEST_F(WorkflowTest, testStopFailed) {
    {
        NiceMockRawDocProducer *producer = new NiceMockRawDocProducer;
        NiceMockRawDocConsumer *consumer = new NiceMockRawDocConsumer;
        EXPECT_CALL(*producer, produce(_)).WillOnce(Return(FE_EOF));
        EXPECT_CALL(*consumer, stop(_)).WillOnce(Return(false));
        Workflow<document::RawDocumentPtr> workflow(producer, consumer);
        workflow.start();
        while (workflow.isRunning()) usleep(1);
        while (!workflow.hasFatalError()) usleep(1);
        EXPECT_TRUE(workflow.hasFatalError());
        EXPECT_FALSE(workflow.isFinished());
    }
    {
        NiceMockRawDocProducer *producer = new NiceMockRawDocProducer;
        NiceMockRawDocConsumer *consumer = new NiceMockRawDocConsumer;
        EXPECT_CALL(*producer, produce(_)).WillOnce(Return(FE_EXCEPTION));
        Workflow<document::RawDocumentPtr> workflow(producer, consumer);
        workflow.start(JOB);
        while (workflow.isRunning()) usleep(1);
        while (!workflow.hasFatalError()) usleep(1);
        EXPECT_TRUE(workflow.hasFatalError());
        EXPECT_FALSE(workflow.isFinished());
        EXPECT_FALSE(workflow.isRunning());
    }
}

TEST_F(WorkflowTest, testDropWithLastError) {
    {
        NiceMockRawDocProducer *producer = new NiceMockRawDocProducer;
        NiceMockRawDocConsumer *consumer = new NiceMockRawDocConsumer;
        EXPECT_CALL(*producer, produce(_))
            . WillOnce(Return(FE_OK))
            . WillOnce(Return(FE_EOF));
        EXPECT_CALL(*consumer, consume(_)).WillOnce(Return(FE_OK));
        EXPECT_CALL(*consumer, stop(FE_EOF)).WillOnce(Return(true));
        Workflow<document::RawDocumentPtr> workflow(producer, consumer);
        workflow.start();
        while (!workflow.isFinished()) usleep(1);
        EXPECT_FALSE(workflow.hasFatalError());
    }
    {
        NiceMockRawDocProducer *producer = new NiceMockRawDocProducer;
        NiceMockRawDocConsumer *consumer = new NiceMockRawDocConsumer;
        EXPECT_CALL(*producer, produce(_))
            . WillOnce(Return(FE_OK))
            . WillOnce(Return(FE_FATAL));
        EXPECT_CALL(*consumer, consume(_)).WillOnce(Return(FE_OK));
        EXPECT_CALL(*consumer, stop(FE_FATAL)).WillOnce(Return(true));
        Workflow<document::RawDocumentPtr> workflow(producer, consumer);
        workflow.start();
        while (workflow.isRunning()) usleep(1);
        EXPECT_TRUE(workflow.hasFatalError());
    }
}

}
}
