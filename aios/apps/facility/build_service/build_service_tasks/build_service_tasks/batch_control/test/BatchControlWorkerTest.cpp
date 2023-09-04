#include "build_service_tasks/batch_control/BatchControlWorker.h"

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service_tasks/batch_control/BatchReporter.h"
#include "build_service_tasks/batch_control/test/MockBatchControlWorker.h"
#include "build_service_tasks/test/unittest.h"
#include "fslib/util/FileUtil.h"

using namespace std;
using namespace autil;
using namespace testing;
using namespace build_service::util;
using namespace build_service::config;

namespace build_service { namespace task_base {

typedef BatchControlWorker::BatchOp BatchOp;
typedef BatchControlWorker::BatchInfo BatchInfo;
typedef swift::protocol::ErrorCode ErrorCode;
typedef swift::protocol::Message Message;

class FakeBatchControlWorker : public BatchControlWorker
{
public:
    FakeBatchControlWorker(bool async = true) : BatchControlWorker(async), exit(false) {}

protected:
    void handleError(const string& msg) override { exit = true; }

public:
    bool exit;
};

class BatchControlWorkerTest : public BUILD_SERVICE_TASKS_TESTBASE
{
public:
    void setUp();
    void tearDown();

public:
    ErrorCode generateMsg(int64_t& ts, Message& message, int64_t timeout = 3 * 1000000)
    {
        ts = TimeUtility::currentTimeInSeconds();
        return swift::protocol::ERROR_NONE;
    };

private:
    vector<BatchInfo> _batchInfos;
};

void BatchControlWorkerTest::setUp() {}

void BatchControlWorkerTest::tearDown() { _batchInfos.clear(); }

TEST_F(BatchControlWorkerTest, testProcess)
{
    BatchControlWorker worker(false);
    worker._startTimestamp = 1234;

    // will fill up begin op
    worker.process(BatchInfo(BatchOp::end, -1, 123, 1));
    ASSERT_EQ(2, worker._batchQueue.size());
    ASSERT_EQ(worker._startTimestamp, worker._batchQueue[0].beginTime);
    ASSERT_EQ(BatchOp::begin, worker._batchQueue[0].operation);
    ASSERT_EQ(1, worker._batchQueue[0].batchId);
    ASSERT_EQ(1234, worker._batchQueue[1].endTime);
    ASSERT_EQ(BatchOp::end, worker._batchQueue[1].operation);
    ASSERT_EQ(1, worker._batchQueue[1].batchId);

    // add begin op, batch 4
    worker.process(BatchInfo(BatchOp::begin, 4, -1, 4));
    ASSERT_EQ(3, worker._batchQueue.size());
    ASSERT_EQ(4, worker._batchQueue[2].beginTime);
    ASSERT_EQ(4, worker._batchQueue[2].batchId);
    ASSERT_EQ(BatchOp::begin, worker._batchQueue[2].operation);
    // add begin op, batch 5, will fill up end op for batch 4
    worker.process(BatchInfo(BatchOp::begin, 5, -1, 5));
    ASSERT_EQ(5, worker._batchQueue.size());
    ASSERT_EQ(5, worker._batchQueue[3].endTime);
    ASSERT_EQ(4, worker._batchQueue[3].batchId);
    ASSERT_EQ(BatchOp::end, worker._batchQueue[3].operation);

    ASSERT_EQ(5, worker._batchQueue[4].beginTime);
    ASSERT_EQ(5, worker._batchQueue[4].batchId);
    ASSERT_EQ(BatchOp::begin, worker._batchQueue[4].operation);
    // add begin op for batch 5, will ommit
    worker.process(BatchInfo(BatchOp::begin, 123, -1, 5));
    ASSERT_EQ(5, worker._batchQueue.size());

    // add end op, batch 3
    // will fill up begin op for batch 3
    // fill up end op for batch 5
    worker.process(BatchInfo(BatchOp::end, -1, 8, 3));
    ASSERT_EQ(8, worker._batchQueue.size());
    ASSERT_EQ(8, worker._batchQueue[5].endTime);
    ASSERT_EQ(5, worker._batchQueue[5].batchId);
    ASSERT_EQ(BatchOp::end, worker._batchQueue[5].operation);

    ASSERT_EQ(5, worker._batchQueue[6].beginTime);
    ASSERT_EQ(3, worker._batchQueue[6].batchId);
    ASSERT_EQ(BatchOp::begin, worker._batchQueue[6].operation);

    ASSERT_EQ(8, worker._batchQueue[7].endTime);
    ASSERT_EQ(3, worker._batchQueue[7].batchId);
    ASSERT_EQ(BatchOp::end, worker._batchQueue[7].operation);

    // add begin op for batch 3, will ommit
    worker.process(BatchInfo(BatchOp::begin, 123, -1, 3));
    ASSERT_EQ(8, worker._batchQueue.size());

    // add end op for batch 10, will fill up begin op for batch 10
    worker.process(BatchInfo(BatchOp::end, -1, 19, 10));
    ASSERT_EQ(10, worker._batchQueue.size());
    ASSERT_EQ(8, worker._batchQueue[8].beginTime);
    ASSERT_EQ(10, worker._batchQueue[8].batchId);
    ASSERT_EQ(BatchOp::begin, worker._batchQueue[8].operation);

    ASSERT_EQ(19, worker._batchQueue[9].endTime);
    ASSERT_EQ(10, worker._batchQueue[9].batchId);
    ASSERT_EQ(BatchOp::end, worker._batchQueue[9].operation);

    // abnormal case
    {
        BatchControlWorker worker(false);
        worker._startTimestamp = 1234;

        // will fill up end op
        worker.process(BatchInfo(BatchOp::begin, 33, -1, 1));
        worker.process(BatchInfo(BatchOp::begin, 22, -1, 2));

        ASSERT_EQ(3, worker._batchQueue.size());
        ASSERT_EQ(33, worker._batchQueue[0].beginTime);
        ASSERT_EQ(33, worker._batchQueue[1].endTime);
        ASSERT_EQ(22, worker._batchQueue[2].beginTime);

        worker.process(BatchInfo(BatchOp::end, -1, 23, 3));
        ASSERT_EQ(23, worker._batchQueue[3].endTime);
        ASSERT_EQ(22, worker._batchQueue[4].beginTime);
        ASSERT_EQ(23, worker._batchQueue[5].endTime);

        worker.process(BatchInfo(BatchOp::end, -1, 21, 4));
        ASSERT_EQ(23, worker._batchQueue[6].beginTime);
        ASSERT_EQ(23, worker._batchQueue[7].endTime);

        worker.process(BatchInfo(BatchOp::end, -1, 26, 5));
        ASSERT_EQ(23, worker._batchQueue[8].beginTime);
        ASSERT_EQ(26, worker._batchQueue[9].endTime);

        worker.process(BatchInfo(BatchOp::begin, 28, -1, 6));
        ASSERT_EQ(28, worker._batchQueue[10].beginTime);

        worker.process(BatchInfo(BatchOp::end, -1, 30, 7));
        ASSERT_EQ(30, worker._batchQueue[11].endTime);
        ASSERT_EQ(28, worker._batchQueue[12].beginTime);
        ASSERT_EQ(30, worker._batchQueue[13].endTime);

        worker.process(BatchInfo(BatchOp::begin, 32, -1, 8));
        ASSERT_EQ(32, worker._batchQueue[14].beginTime);

        worker.process(BatchInfo(BatchOp::end, -1, 30, 9));
        ASSERT_EQ(32, worker._batchQueue[15].endTime);
        ASSERT_EQ(32, worker._batchQueue[16].beginTime);
        ASSERT_EQ(32, worker._batchQueue[17].endTime);

        ASSERT_EQ(18, worker._batchQueue.size());
        worker.process(BatchInfo(BatchOp::begin, 32, -1, 8));
        ASSERT_EQ(18, worker._batchQueue.size());
        worker.process(BatchInfo(BatchOp::begin, 320, -1, 8));
        ASSERT_EQ(19, worker._batchQueue.size());
    }
}

TEST_F(BatchControlWorkerTest, testValidate)
{
    BatchControlWorker worker(false);
    ASSERT_FALSE(worker.validate(BatchInfo(BatchOp::unknown, -1, -1, 2)));
    ASSERT_FALSE(worker.validate(BatchInfo(BatchOp::begin, -1, 120, 2)));
    ASSERT_TRUE(worker.validate(BatchInfo(BatchOp::begin, 123, 120, 2)));
    ASSERT_FALSE(worker.validate(BatchInfo(BatchOp::end, 123, -1, 2)));
    ASSERT_TRUE(worker.validate(BatchInfo(BatchOp::end, 123, 123, 2)));
}

TEST_F(BatchControlWorkerTest, testParse)
{
    BatchControlWorker worker(false);
    {
        swift::protocol::Message msg;
        msg.set_data("batch=1\n \n operation = begin\n");
        msg.set_timestamp(1234);
        BatchInfo batch;
        worker.parse(msg, batch);
        ASSERT_EQ(BatchOp::begin, batch.operation);
        ASSERT_EQ(1, batch.batchId);
        ASSERT_EQ(1234, batch.beginTime);
        ASSERT_EQ(-1, batch.endTime);
    }
    {
        swift::protocol::Message msg;
        msg.set_data("batch=2\n operation = end\n");
        msg.set_timestamp(1234);
        BatchInfo batch;
        worker.parse(msg, batch);
        ASSERT_EQ(BatchOp::end, batch.operation);
        ASSERT_EQ(2, batch.batchId);
        ASSERT_EQ(1234, batch.endTime);
        ASSERT_EQ(-1, batch.beginTime);
    }
    {
        swift::protocol::Message msg;
        msg.set_data("batch = 2\n operation = test\n");
        msg.set_timestamp(1234);
        BatchInfo batch;
        ASSERT_FALSE(worker.parse(msg, batch));
    }
    {
        swift::protocol::Message msg;
        msg.set_data("batch = abc\n operation = begin\n");
        msg.set_timestamp(1234);
        BatchInfo batch;
        ASSERT_FALSE(worker.parse(msg, batch));
    }
}

TEST_F(BatchControlWorkerTest, testReportBatch)
{
    setenv(BatchControlWorker::BS_CALL_GRAPH_FAILED_TIME.c_str(), "4", true);
    MockBatchReporter* reporter = new ::testing::StrictMock<MockBatchReporter>;
    FakeBatchControlWorker worker(false);
    int64_t ts = worker._successReportTs;
    ASSERT_EQ(0, worker._batchCursor);
    worker._reporter.reset(reporter);
    // test no more operation
    worker.reportBatch();
    worker._batchQueue.push_back(BatchInfo(BatchOp::begin, 123, 123, 1));
    worker._batchQueue.push_back(BatchInfo(BatchOp::begin, 123, 123, 2));
    worker._batchQueue.push_back(BatchInfo(BatchOp::end, 123, 123, 2));
    worker._batchQueue.push_back(BatchInfo(BatchOp::begin, 123, 123, 4));
    worker._batchCursor = 1;
    worker._globalId = 10;

    // report operation
    EXPECT_CALL(*reporter, report(_, 10, -1)).WillOnce(Return(false)).WillOnce(Return(true));
    worker.reportBatch();
    ASSERT_EQ(1, worker._batchCursor);
    ASSERT_EQ(10, worker._globalId);
    ASSERT_EQ(ts, worker._successReportTs); // report failed, will not update ts
    ASSERT_FALSE(worker.exit);
    sleep(1);
    worker.reportBatch();
    ASSERT_EQ(2, worker._batchCursor);
    ASSERT_GT(worker._successReportTs, ts); // report success, will update ts
    // only report end operation, will globalId++
    ASSERT_EQ(10, worker._globalId);

    EXPECT_CALL(*reporter, report(_, 10, 9)).WillOnce(Return(false)).WillOnce(Return(true));
    sleep(5);
    // call graph failed over DEFAULT_CALL_GRAPH_FAIL_TIME
    worker.reportBatch();
    ASSERT_TRUE(worker.exit);

    worker.exit = false;
    worker.reportBatch();
    ASSERT_EQ(3, worker._batchCursor);
    ASSERT_EQ(11, worker._globalId);
    ASSERT_FALSE(worker.exit);

    EXPECT_CALL(*reporter, report(_, 11, 10)).WillOnce(Return(true));
    worker.reportBatch();
    ASSERT_EQ(4, worker._batchCursor);
    ASSERT_EQ(11, worker._globalId);

    // no more operation
    worker.reportBatch();
    ASSERT_EQ(4, worker._batchCursor);
    ASSERT_EQ(11, worker._globalId);
}

TEST_F(BatchControlWorkerTest, testInitAndRecover)
{
    BuildServiceConfig buildServiceConfig;
    buildServiceConfig.zkRoot = "zfs://127.0.0.1:1234/";
    string content = ToJsonString(buildServiceConfig);
    string configRoot = GET_TEMP_DATA_PATH() + "/batch_control_test/config";
    ASSERT_TRUE(fslib::util::FileUtil::atomicCopy(GET_TEST_DATA_PATH() + "/batch_control_test/config", configRoot));
    ASSERT_TRUE(fslib::util::FileUtil::writeFile(configRoot + "/build_app.json", content));
    Task::TaskInitParam initParam;
    ResourceReaderPtr reader = ResourceReaderManager::getResourceReader(configRoot);
    initParam.resourceReader = reader;
    initParam.buildId.appName = "igraph";
    initParam.buildId.dataTable = "simple";
    initParam.buildId.generationId = 1234;

    BatchInfo batch1(BatchOp::begin, 123, 123, 1);
    BatchInfo batch2(BatchOp::begin, 123, 123, 2);
    string ckpStr;
    auto saveCkp = [&ckpStr](const string& content) {
        ckpStr = content;
        return WorkerState::ErrorCode::EC_OK;
    };
    auto readCkp = [&ckpStr](string& content) {
        content = ckpStr;
        return WorkerState::ErrorCode::EC_OK;
    };
    {
        MockBatchControlWorker* worker = new ::testing::StrictMock<MockBatchControlWorker>;
        cm_basic::ZkWrapper* zk = new cm_basic::ZkWrapper("127.0.0.1:1234", 10);
        MockZkState* zkstate = new ::testing::StrictMock<MockZkState>(zk, "");
        EXPECT_CALL(*zkstate, write(_)).WillOnce(Invoke(saveCkp));
        EXPECT_CALL(*zkstate, read(_)).WillOnce(Return(WorkerState::ErrorCode::EC_NOT_EXIST));

        worker->_zkState.reset(zkstate);
        EXPECT_CALL(*worker, initZkState(_, _)).WillOnce(Return(true));
        ASSERT_TRUE(worker->init(initParam));
        ASSERT_TRUE(worker->_reporter);
        ASSERT_EQ("[\n  \"cluster1\",\n  \"cluster2\"\n]", worker->_reporter->_clusterNames);
        worker->_batchCursor = 100;
        worker->_locator = 20;
        worker->_batchQueue.push_back(batch1);
        worker->_batchQueue.push_back(batch2);

        worker->syncCheckpoint();
        delete worker;
        delete zk;
    }
    {
        MockBatchControlWorker* worker = new ::testing::StrictMock<MockBatchControlWorker>;
        cm_basic::ZkWrapper* zk = new cm_basic::ZkWrapper("127.0.0.1:1234", 10);
        MockZkState* zkstate = new ::testing::StrictMock<MockZkState>(zk, "");
        EXPECT_CALL(*zkstate, read(_)).WillOnce(Invoke(readCkp));
        worker->_zkState.reset(zkstate);
        EXPECT_CALL(*worker, initZkState(_, _)).WillOnce(Return(true));
        ASSERT_TRUE(worker->init(initParam));
        ASSERT_EQ(100, worker->_batchCursor);
        ASSERT_EQ(20, worker->_locator);
        ASSERT_EQ(batch1, worker->_batchQueue[0]);
        ASSERT_EQ(batch2, worker->_batchQueue[1]);
        delete worker;
        delete zk;
    }
    {
        // recover failed
        MockBatchControlWorker* worker = new ::testing::StrictMock<MockBatchControlWorker>;
        cm_basic::ZkWrapper* zk = new cm_basic::ZkWrapper("127.0.0.1:1234", 10);
        MockZkState* zkstate = new ::testing::StrictMock<MockZkState>(zk, "");
        EXPECT_CALL(*zkstate, read(_)).WillOnce(Return(WorkerState::ErrorCode::EC_FAIL));
        worker->_zkState.reset(zkstate);
        EXPECT_CALL(*worker, initZkState(_, _)).WillOnce(Return(true));
        ASSERT_FALSE(worker->init(initParam));
        delete worker;
        delete zk;
    }
}
}} // namespace build_service::task_base
