#include "suez/drc/LogReplicationPipeline.h"

#include "MockLogRewriter.h"
#include "MockSink.h"
#include "MockSource.h"
#include "MockWorkerState.h"
#include "autil/EnvUtil.h"
#include "autil/HashFuncFactory.h"
#include "suez/drc/LogReader.h"
#include "suez/drc/LogRecordBuilder.h"
#include "suez/drc/LogWriter.h"
#include "unittest/unittest.h"
#include "worker_framework/WorkerState.h"

using namespace ::testing;

namespace suez {

class MockLogReplicator : public LogReplicationPipeline {
public:
    MockLogReplicator() = default;
    MockLogReplicator(std::unique_ptr<LogReader> producer,
                      std::map<std::string, std::unique_ptr<LogWriter>> consumers,
                      const std::shared_ptr<indexlibv2::framework::ITablet> &index,
                      std::unique_ptr<worker_framework::WorkerState> state)
        : LogReplicationPipeline(std::move(producer), std::move(consumers), index, std::move(state)) {}

public:
    MOCK_CONST_METHOD0(getVisibleLogId, int64_t());
    MOCK_CONST_METHOD0(createLogRewriter, LogRewriter *());
};
typedef ::testing::StrictMock<MockLogReplicator> StrictMockLogReplicator;
typedef ::testing::NiceMock<MockLogReplicator> NiceMockLogReplicator;

class LogReplicationPipelineTest : public TESTBASE {
public:
    void setUp() override {
        auto source = std::make_unique<NiceMockSource>();
        _mockSource = source.get();
        auto reader = std::make_unique<LogReader>(std::move(source));

        auto sink = std::make_unique<NiceMockSink>();
        _mockSink = sink.get();
        auto hashFunc = autil::HashFuncFactory::createHashFunc("HASH");
        auto writer =
            std::make_unique<LogWriter>(std::move(sink), std::move(hashFunc), std::vector<std::string>{"key"});
        std::map<std::string, std::unique_ptr<LogWriter>> writers;
        writers["w1"] = std::move(writer);

        auto state = std::make_unique<NiceMockWorkerState>();
        _mockState = state.get();

        auto logRewriter = std::make_unique<NiceMockLogRewriter>();
        _mockLogRewriter = logRewriter.get();

        autil::EnvGuard guard("checkpoint_interval_in_sec", "2");
        _replicator =
            std::make_unique<NiceMockLogReplicator>(std::move(reader), std::move(writers), nullptr, std::move(state));
        _replicator->_logRewriter = std::move(logRewriter);
    }

protected:
    MockSource *_mockSource;
    MockSink *_mockSink;
    MockWorkerState *_mockState;
    MockLogRewriter *_mockLogRewriter;
    std::unique_ptr<NiceMockLogReplicator> _replicator;
};

TEST_F(LogReplicationPipelineTest, testRecover) {
    EXPECT_CALL(*_mockState, read(_)).WillOnce(DoAll(SetArgReferee<0>(""), Return(WorkerState::EC_FAIL)));
    ASSERT_FALSE(_replicator->recover());

    EXPECT_CALL(*_mockState, read(_)).WillOnce(DoAll(SetArgReferee<0>(""), Return(WorkerState::EC_NOT_EXIST)));
    ASSERT_TRUE(_replicator->recover());
    ASSERT_EQ(-1, _replicator->_checkpoint.getPersistedLogId());

    EXPECT_CALL(*_mockState, read(_)).WillOnce(DoAll(SetArgReferee<0>("xx"), Return(WorkerState::EC_OK)));
    ASSERT_FALSE(_replicator->recover());

    std::string content = "{\"persisted_log_id\": 100}";
    EXPECT_CALL(*_mockState, read(_)).WillOnce(DoAll(SetArgReferee<0>(content), Return(WorkerState::EC_OK)));
    ASSERT_TRUE(_replicator->recover());
    ASSERT_EQ(100, _replicator->_checkpoint.getPersistedLogId());
}

TEST_F(LogReplicationPipelineTest, testStart) {
    EXPECT_CALL(*_mockState, read(_)).WillOnce(DoAll(SetArgReferee<0>(""), Return(WorkerState::EC_NOT_EXIST)));
    EXPECT_CALL(*_replicator, createLogRewriter()).WillOnce(Return(new NiceMockLogRewriter()));
    ASSERT_TRUE(_replicator->start(-1));
    ASSERT_EQ(-1, _replicator->_consumedLogId);
    ASSERT_TRUE(_replicator->_running);
    _replicator->_running = false;

    EXPECT_CALL(*_mockState, read(_)).WillOnce(DoAll(SetArgReferee<0>(""), Return(WorkerState::EC_NOT_EXIST)));
    EXPECT_CALL(*_replicator, createLogRewriter()).WillOnce(Return(new NiceMockLogRewriter()));
    ASSERT_TRUE(_replicator->start(0));
    ASSERT_EQ(-1, _replicator->_consumedLogId);
    ASSERT_TRUE(_replicator->_running);
    _replicator->_running = false;

    EXPECT_CALL(*_mockState, read(_)).WillOnce(DoAll(SetArgReferee<0>(""), Return(WorkerState::EC_NOT_EXIST)));
    EXPECT_CALL(*_replicator, createLogRewriter()).WillOnce(Return(new NiceMockLogRewriter()));
    ASSERT_TRUE(_replicator->start(10));
    ASSERT_EQ(9, _replicator->_consumedLogId);
    ASSERT_TRUE(_replicator->_running);
    _replicator->_running = false;

    std::string content = "{\"persisted_log_id\": 100}";
    EXPECT_CALL(*_mockState, read(_)).WillOnce(DoAll(SetArgReferee<0>(content), Return(WorkerState::EC_OK)));
    EXPECT_CALL(*_replicator, createLogRewriter()).WillOnce(Return(new NiceMockLogRewriter()));
    ASSERT_TRUE(_replicator->start(80));
    ASSERT_EQ(100, _replicator->_consumedLogId);
    ASSERT_TRUE(_replicator->_running);
}

TEST_F(LogReplicationPipelineTest, testReplicateLogRange) {
    _replicator->_running = true;

    LogRecord r1(1);
    LogRecord r2(4);
    ASSERT_TRUE(LogRecordBuilder::makeInsert({{"key", "k1"}, {"value", "v1"}}, r1));
    ASSERT_TRUE(LogRecordBuilder::makeInsert({{"key", "k2"}, {"value", "v2"}}, r2));
    {
        int64_t start = 1, end = 2; // [1, 2)
        EXPECT_CALL(*_mockSource, seek(start)).WillOnce(Return(true));
        EXPECT_CALL(*_mockSource, read(_, _))
            .WillOnce(DoAll(SetArgReferee<0>(r1.getData()), SetArgReferee<1>(r1.getLogId()), Return(true)))
            .WillOnce(DoAll(SetArgReferee<0>(r2.getData()), SetArgReferee<1>(r2.getLogId()), Return(true)));
        EXPECT_CALL(*_mockLogRewriter, createSnapshot()).WillOnce(Return(true));
        EXPECT_CALL(*_mockLogRewriter, rewrite(_)).WillOnce(DoAll(SetArgReferee<0>(r1), Return(RC_OK)));
        EXPECT_CALL(*_mockSink, write(_, r1.getData(), r1.getLogId())).WillOnce(Return(true));
        size_t count = 0;
        ASSERT_TRUE(_replicator->replicateLogRange(start, end, count));
        ASSERT_EQ(1, count);
        ASSERT_EQ(1, _replicator->_consumedLogId);
    }
    {
        int64_t start = 2, end = 4; // [2, 4) no validate log
        EXPECT_CALL(*_mockSource, seek(2)).WillOnce(Return(true));
        EXPECT_CALL(*_mockSource, read(_, _))
            .WillOnce(DoAll(SetArgReferee<0>(r2.getData()), SetArgReferee<1>(r2.getLogId()), Return(true)));
        EXPECT_CALL(*_mockLogRewriter, createSnapshot()).WillOnce(Return(true));
        size_t count = 0;
        ASSERT_FALSE(_replicator->replicateLogRange(start, end, count));
        ASSERT_EQ(0, count);
        ASSERT_EQ(1, _replicator->_consumedLogId);
    }
    {
        int64_t start = 2, end = 5; // [2, 5)
        EXPECT_CALL(*_mockSource, seek(2)).WillOnce(Return(true));
        EXPECT_CALL(*_mockSource, read(_, _))
            .WillOnce(DoAll(SetArgReferee<0>(r2.getData()), SetArgReferee<1>(r2.getLogId()), Return(true)))
            .WillOnce(Return(false));
        EXPECT_CALL(*_mockLogRewriter, createSnapshot()).WillOnce(Return(true));
        EXPECT_CALL(*_mockLogRewriter, rewrite(_)).WillOnce(DoAll(SetArgReferee<0>(r2), Return(RC_OK)));
        EXPECT_CALL(*_mockSink, write(_, r2.getData(), r2.getLogId())).WillOnce(Return(true));
        size_t count = 0;
        ASSERT_TRUE(_replicator->replicateLogRange(start, end, count));
        ASSERT_EQ(1, count);
        ASSERT_EQ(4, _replicator->_consumedLogId);
    }
}

TEST_F(LogReplicationPipelineTest, testUpdateCheckpoint) {
    EXPECT_CALL(*_mockSink, getCommittedCheckpoint()).WillOnce(Return(1));
    _replicator->updateCheckpoint();
    ASSERT_EQ(1, _replicator->_checkpoint.getPersistedLogId());

    EXPECT_CALL(*_mockSink, getCommittedCheckpoint()).WillOnce(Return(4));
    _replicator->updateCheckpoint();
    ASSERT_EQ(4, _replicator->_checkpoint.getPersistedLogId());

    EXPECT_CALL(*_mockSink, getCommittedCheckpoint()).WillOnce(Return(2));
    _replicator->updateCheckpoint();
    ASSERT_EQ(4, _replicator->_checkpoint.getPersistedLogId());
}

TEST_F(LogReplicationPipelineTest, testSaveCheckpoint) {
    EXPECT_CALL(*_mockState, write(_)).WillOnce(Return(worker_framework::WorkerState::EC_OK));
    _replicator->saveCheckpoint();
    sleep(1);
    EXPECT_CALL(*_mockState, write(_)).Times(0);
    _replicator->saveCheckpoint();
    sleep(1);
    EXPECT_CALL(*_mockState, write(_)).WillOnce(Return(worker_framework::WorkerState::EC_OK));
    _replicator->saveCheckpoint();
}

} // namespace suez
