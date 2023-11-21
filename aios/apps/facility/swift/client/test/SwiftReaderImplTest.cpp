#include "swift/client/SwiftReaderImpl.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <unistd.h>
#include <utility>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/TimeUtility.h"
#include "swift/client/SwiftPartitionStatus.h"
#include "swift/client/SwiftReaderConfig.h"
#include "swift/client/SwiftSinglePartitionReader.h"
#include "swift/client/TransportClosure.h"
#include "swift/client/fake_client/FakeClientHelper.h"
#include "swift/client/fake_client/FakeSwiftAdminAdapter.h"
#include "swift/client/fake_client/FakeSwiftSinglePartitionReader.h"
#include "swift/client/fake_client/FakeSwiftTransportAdapter.h"
#include "swift/client/fake_client/FakeSwiftTransportClient.h"
#include "swift/common/Common.h"
#include "swift/network/SwiftAdminAdapter.h"
#include "swift/network/SwiftRpcChannelManager.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace swift::protocol;
using namespace swift::common;
using namespace swift::network;

namespace swift {
namespace client {

class SwiftReaderImplTest : public TESTBASE {
public:
    void setUp() {
        _partitionCount = 3;
        FakeSwiftAdminAdapter *fakeAdapter = new FakeSwiftAdminAdapter;
        fakeAdapter->setPartCount(10);
        _topicInfo.set_partitioncount(10);
        _config.topicName = "test_topic";
        _config.batchReadCount = 1;
        _adapter.reset(fakeAdapter);
        _reader = FakeClientHelper::createFakeReader(_adapter, _config, _partitionCount);
    }
    void tearDown() { DELETE_AND_SET_NULL(_reader); }

private:
    void finishAsyncCall(std::vector<FakeSwiftTransportClient *> &fakeClientVec);

private:
    SwiftReaderImpl *_reader;
    uint32_t _partitionCount;
    SwiftAdminAdapterPtr _adapter;
    SwiftReaderConfig _config;
    TopicInfo _topicInfo;
};

TEST_F(SwiftReaderImplTest, testInit) {
    ErrorCode ec = ERROR_NONE;
    {
        SwiftReaderImpl reader(_adapter, SwiftRpcChannelManagerPtr(), _topicInfo);
        SwiftReaderConfig config;
        config.topicName = "topic";
        std::vector<uint32_t> pids;
        pids.push_back(0);
        pids.push_back(100);
        config.readPartVec = pids;
        ec = reader.init(config);
        EXPECT_EQ(ERROR_CLIENT_INVALID_PARTITIONID, ec);
        EXPECT_EQ(size_t(0), reader._readers.size());
    }

    {
        SwiftReaderImpl reader(_adapter, SwiftRpcChannelManagerPtr(), _topicInfo);
        SwiftReaderConfig config;
        config.topicName = "topic";
        std::vector<uint32_t> pids;
        pids.push_back(0);
        pids.push_back(3);
        pids.push_back(5);
        config.readPartVec = pids;
        ec = reader.init(config);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ((size_t)3, reader._readers.size());
    }
    {
        SwiftReaderImpl reader(_adapter, SwiftRpcChannelManagerPtr(), _topicInfo);
        SwiftReaderConfig config;
        config.topicName = "topic";
        std::vector<uint32_t> pids;
        pids.push_back(0);
        pids.push_back(2);
        config.readPartVec = pids;
        vector<string> requiredFields;
        requiredFields.push_back("abc");
        config.requiredFields = requiredFields;
        config.fieldFilterDesc = "abc IN A|B";
        ec = reader.init(config);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ((size_t)2, reader._readers.size());
        vector<string> fields = reader._readers[0]->getRequiredFieldNames();
        EXPECT_EQ((size_t)1, fields.size());
        EXPECT_EQ(string("abc"), fields[0]);
        EXPECT_EQ(string("abc IN A|B"), reader._readers[0]->getFieldFilterDesc());
        fields = reader._readers[1]->getRequiredFieldNames();
        EXPECT_EQ((size_t)1, fields.size());
        EXPECT_EQ(string("abc"), fields[0]);
        EXPECT_EQ(string("abc IN A|B"), reader._readers[1]->getFieldFilterDesc());
    }
}

TEST_F(SwiftReaderImplTest, testSeekBytimestamp) {
    int64_t timestamp = 0;
    ErrorCode ec = _reader->seekByTimestamp(timestamp);
    EXPECT_TRUE(ec == ERROR_NONE);

    FakeSwiftSinglePartitionReader *singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[0]);
    singleReader->setErrorCode(ERROR_UNKNOWN);

    EXPECT_TRUE(_partitionCount >= 1);
    singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[_partitionCount - 1]);
    singleReader->setErrorCode(ERROR_CLIENT_RPC_CONNECT);

    ec = _reader->seekByTimestamp(timestamp);
    EXPECT_TRUE(ec == ERROR_NONE);
    ec = _reader->seekByTimestamp(timestamp + 1);
    EXPECT_TRUE(ec == ERROR_CLIENT_RPC_CONNECT);
}

TEST_F(SwiftReaderImplTest, testSeekBytimestampWithFutureTime) {
    int64_t timestamp = TimeUtility::currentTime() + 10000;
    ErrorCode ec = _reader->seekByTimestamp(timestamp);
    EXPECT_TRUE(ec == ERROR_NONE);

    FakeSwiftSinglePartitionReader *singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[0]);
    EXPECT_TRUE(singleReader->_seekTimestamp < timestamp);
}

TEST_F(SwiftReaderImplTest, testSeekByProgress) {
    ReaderProgress progress;
    ErrorCode ec = _reader->seekByProgress(progress);
    EXPECT_EQ(ERROR_CLIENT_INVALID_PARAMETERS, ec);
    TopicReaderProgress *topicProgress = progress.add_topicprogress();
    topicProgress->set_topicname(_config.topicName);
    PartReaderProgress *partProgress = topicProgress->add_partprogress();
    partProgress->set_from(100);
    partProgress->set_to(65535);
    partProgress->set_timestamp(0);
    ec = _reader->seekByProgress(progress);
    EXPECT_EQ(ERROR_NONE, ec);

    FakeSwiftSinglePartitionReader *singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[0]);
    singleReader->setErrorCode(ERROR_UNKNOWN);

    EXPECT_TRUE(_partitionCount >= 1);
    singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[_partitionCount - 1]);
    singleReader->setErrorCode(ERROR_CLIENT_RPC_CONNECT);

    ec = _reader->seekByProgress(progress);
    EXPECT_EQ(ERROR_NONE, ec);

    partProgress->set_timestamp(1);
    ec = _reader->seekByProgress(progress);
    EXPECT_EQ(ERROR_CLIENT_RPC_CONNECT, ec);

    ec = _reader->seekByProgress(progress, true);
    EXPECT_EQ(ERROR_CLIENT_RPC_CONNECT, ec);
}

TEST_F(SwiftReaderImplTest, testGetFirstMsgTimestamp) {
    int64_t timestamp = 0;
    timestamp = _reader->getFirstMsgTimestamp();
    EXPECT_EQ((int64_t)-1, timestamp);

    FakeSwiftSinglePartitionReader *singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[0]);
    Message msg;
    msg.set_timestamp(101);
    singleReader->push(msg);

    EXPECT_TRUE(_partitionCount >= 1);
    singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[_partitionCount - 1]);
    msg.set_timestamp(1);
    singleReader->push(msg);

    timestamp = _reader->getFirstMsgTimestamp();
    EXPECT_EQ(timestamp, (int64_t)1);
}

TEST_F(SwiftReaderImplTest, testBatchRead) {
    SwiftReaderConfig config;
    config.batchReadCount = 2;
    uint32_t partitionCount = 2;
    SwiftReaderImplPtr reader(FakeClientHelper::createSimpleReader(_adapter, config, partitionCount));

    Messages msgs;
    int64_t timestamp = -1;
    reader->read(timestamp, msgs, 8000);
    EXPECT_EQ(2, msgs.msgs_size());
    EXPECT_EQ(0, msgs.msgs(0).timestamp());
    EXPECT_EQ(0, reader->_messageBuffer.msgs_size());
    EXPECT_EQ(0, reader->_bufferCursor);
    EXPECT_EQ(3, timestamp);

    reader->read(timestamp, msgs, 10);
    EXPECT_EQ(2, msgs.msgs_size());
    EXPECT_EQ(3, msgs.msgs(0).timestamp());
    EXPECT_EQ(6, timestamp);

    reader->read(timestamp, msgs, 10);
    EXPECT_EQ(1, msgs.msgs_size());
    EXPECT_EQ(6, msgs.msgs(0).timestamp());
    EXPECT_EQ(7, timestamp);

    reader->read(timestamp, msgs, 10);
    EXPECT_EQ(1, msgs.msgs_size());
    EXPECT_EQ(9, msgs.msgs(0).timestamp());
    EXPECT_EQ(7, timestamp);

    ErrorCode ec = reader->read(timestamp, msgs, 10);
    EXPECT_EQ(ec, ERROR_CLIENT_NO_MORE_MESSAGE);
}

TEST_F(SwiftReaderImplTest, testBatchAndSingleRead) {
    SwiftReaderConfig config;
    config.batchReadCount = 3;
    uint32_t partitionCount = 2;
    uint32_t messageCount = 4;
    SwiftReaderImplPtr reader(FakeClientHelper::createSimpleReader(_adapter, config, partitionCount, messageCount));
    Message msg;
    int64_t timestamp = -1;
    reader->read(timestamp, msg); // read one
    EXPECT_EQ(0, msg.timestamp());
    EXPECT_EQ(3, reader->_messageBuffer.msgs_size());
    EXPECT_EQ(1, reader->_bufferCursor);
    EXPECT_EQ(3, timestamp);

    Messages msgs;
    reader->read(timestamp, msgs); // batch read
    EXPECT_EQ(2, msgs.msgs_size());
    EXPECT_EQ(3, msgs.msgs(0).timestamp());
    EXPECT_EQ(6, msgs.msgs(1).timestamp());
    EXPECT_EQ(3, timestamp);

    reader->read(timestamp, msg); // read one
    EXPECT_EQ(3, msg.timestamp());
    EXPECT_EQ(3, reader->_messageBuffer.msgs_size());
    EXPECT_EQ(1, reader->_bufferCursor);
    EXPECT_EQ(6, timestamp);

    reader->read(timestamp, msg); // read one
    EXPECT_EQ(6, msg.timestamp());
    EXPECT_EQ(3, reader->_messageBuffer.msgs_size());
    EXPECT_EQ(2, reader->_bufferCursor);
    EXPECT_EQ(9, timestamp);

    reader->read(timestamp, msgs); // batch read
    EXPECT_EQ(1, msgs.msgs_size());
    EXPECT_EQ(9, msgs.msgs(0).timestamp());
    EXPECT_EQ(9, timestamp);

    reader->read(timestamp, msg); // read one
    EXPECT_EQ(9, msg.timestamp());
    EXPECT_EQ(1, reader->_messageBuffer.msgs_size());
    EXPECT_EQ(1, reader->_bufferCursor);
    EXPECT_EQ(10, timestamp);

    reader->read(timestamp, msg);
    EXPECT_EQ(12, msg.timestamp());
    EXPECT_EQ(1, reader->_messageBuffer.msgs_size());
    EXPECT_EQ(1, reader->_bufferCursor);
    EXPECT_EQ(10, timestamp);

    ErrorCode ec = reader->read(timestamp, msgs);
    EXPECT_EQ(ec, ERROR_CLIENT_NO_MORE_MESSAGE);
}

TEST_F(SwiftReaderImplTest, testBatchWithTimestampLimit) {
    SwiftReaderConfig config;
    config.batchReadCount = 2;
    uint32_t partitionCount = 2;
    SwiftReaderImplPtr reader(FakeClientHelper::createSimpleReader(_adapter, config, partitionCount));

    int64_t acceptTimestamp = -1;
    reader->setTimestampLimit(3, acceptTimestamp);
    EXPECT_EQ(3, acceptTimestamp);

    Messages msgs;
    int64_t timestamp;
    reader->read(timestamp, msgs);
    EXPECT_EQ(2, msgs.msgs_size());
    EXPECT_EQ(3, msgs.msgs(1).timestamp());
    reader->read(timestamp, msgs);
    EXPECT_EQ(1, msgs.msgs_size());
    EXPECT_EQ(3, msgs.msgs(0).timestamp());

    ErrorCode ec = reader->read(timestamp, msgs);
    EXPECT_EQ(ec, ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT);
}

void assertSingleProgress(const ReaderProgress &progress,
                          uint32_t index,
                          uint32_t from,
                          uint32_t to,
                          int64_t ts,
                          int32_t offsetInMsg = 0,
                          string logicTopic = string()) {
    const TopicReaderProgress &topicProgresss = progress.topicprogress(0);
    ASSERT_EQ(from, topicProgresss.partprogress(index).from());
    ASSERT_EQ(to, topicProgresss.partprogress(index).to());
    ASSERT_EQ(ts, topicProgresss.partprogress(index).timestamp());
    ASSERT_EQ(offsetInMsg, topicProgresss.partprogress(index).offsetinrawmsg());
    if (!logicTopic.empty()) {
        ASSERT_EQ(logicTopic, topicProgresss.topicname());
    }
}

TEST_F(SwiftReaderImplTest, testBatchWithSeek) {
    SwiftReaderConfig config;
    config.topicName = _config.topicName;
    config.batchReadCount = 3;
    uint32_t partitionCount = 2;
    uint32_t messageCount = 4;
    SwiftReaderImplPtr reader(FakeClientHelper::createSimpleReader(_adapter, config, partitionCount, messageCount));
    reader->_logicTopicName = "logicTopic";

    ReaderProgress progress;
    ErrorCode ec = reader->getReaderProgress(progress);
    EXPECT_EQ(ERROR_NONE, ec);
    ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 0, 0, 32767, 0, 0, "logicTopic"));
    ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 1, 32768, 65535, 0, 0, "logicTopic"));

    Message msg;
    int64_t timestamp = -1;
    reader->read(timestamp, msg); // read one
    ASSERT_EQ(0, msg.timestamp());
    ASSERT_EQ(3, reader->_messageBuffer.msgs_size());
    ASSERT_EQ(1, reader->_bufferCursor);
    ASSERT_EQ(3, timestamp);

    progress.clear_topicprogress();
    ec = reader->getReaderProgress(progress);
    EXPECT_EQ(ERROR_NONE, ec);
    ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 0, 0, 32767, 3, 0, "logicTopic"));
    ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 1, 32768, 65535, 3, 0, "logicTopic"));

    int64_t seekTimestamp = 6;
    reader->seekByTimestamp(seekTimestamp);

    Messages msgs;
    ec = reader->read(timestamp, msgs); // batch read
    ASSERT_EQ(ec, ERROR_NONE);
    ASSERT_EQ(1, msgs.msgs_size());
    ASSERT_EQ(6, msgs.msgs(0).timestamp());
    ASSERT_EQ(3, reader->_messageBuffer.msgs_size());
    ASSERT_EQ(3, reader->_bufferCursor);
    ASSERT_EQ(6, timestamp);
    progress.clear_topicprogress();
    ec = reader->getReaderProgress(progress);
    ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 0, 0, 32767, 9, 0, "logicTopic"));
    ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 1, 32768, 65535, 6, 0, "logicTopic"));

    reader->read(timestamp, msg);
    ASSERT_EQ(9, msg.timestamp()); // for part_1's response not return
    ASSERT_EQ(1, reader->_messageBuffer.msgs_size());
    ASSERT_EQ(1, reader->_bufferCursor);
    ASSERT_EQ(6, timestamp);
    progress.clear_topicprogress();
    ec = reader->getReaderProgress(progress);
    ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 0, 0, 32767, 10));
    ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 1, 32768, 65535, 6));
    usleep(1000 * 1000);

    reader->read(timestamp, msg);
    ASSERT_EQ(6, msg.timestamp()); // part_1's response return
    ASSERT_EQ(9, timestamp);
    progress.clear_topicprogress();
    ec = reader->getReaderProgress(progress);
    ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 0, 0, 32767, 10));
    ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 1, 32768, 65535, 9));

    seekTimestamp = 10;
    reader->seekByTimestamp(seekTimestamp);
    reader->read(timestamp, msg);
    ASSERT_EQ(12, msg.timestamp());
    ASSERT_EQ(10, timestamp);
    progress.clear_topicprogress();
    ec = reader->getReaderProgress(progress);
    ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 0, 0, 32767, 10));
    ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 1, 32768, 65535, 13));

    ec = reader->read(timestamp, msgs);
    ASSERT_EQ(ec, ERROR_CLIENT_NO_MORE_MESSAGE);
    progress.clear_topicprogress();
    ec = reader->getReaderProgress(progress);
    ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 0, 0, 32767, 10));
    ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 1, 32768, 65535, 13));

    // test seekByProgress
    progress.mutable_topicprogress(0)->mutable_partprogress(0)->set_timestamp(5);
    progress.mutable_topicprogress(0)->mutable_partprogress(1)->set_timestamp(9);
    ec = reader->seekByProgress(progress, true);
    ASSERT_EQ(ERROR_NONE, ec);
    ec = reader->read(timestamp, msg);
    ASSERT_EQ(6, msg.timestamp());
    ASSERT_EQ(9, timestamp);
    ec = reader->read(timestamp, msg);
    ASSERT_EQ(9, msg.timestamp());
    ASSERT_EQ(9, timestamp);
    ec = reader->read(timestamp, msg);
    ASSERT_EQ(9, msg.timestamp());
    ASSERT_EQ(10, timestamp);
}

TEST_F(SwiftReaderImplTest, testSeekByProgressPartial) {
    SwiftReaderConfig config;
    config.topicName = _config.topicName;
    config.batchReadCount = 3;
    config.swiftFilter.set_from(100);
    config.swiftFilter.set_to(40000);
    uint32_t partitionCount = 1;
    uint32_t messageCount = 10;
    SwiftReaderImplPtr reader(FakeClientHelper::createSimpleReader(_adapter, config, partitionCount, messageCount));
    reader->_logicTopicName = "logicTopic";
    ReaderProgress progress;
    ErrorCode ec = reader->getReaderProgress(progress);
    EXPECT_EQ(ERROR_NONE, ec);
    ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 0, 100, 40000, 0, 0, "logicTopic"));

    Message msg;
    int64_t timestamp = -1;
    reader->read(timestamp, msg); // read one
    ASSERT_EQ(0, msg.timestamp());
    ASSERT_EQ(3, reader->_messageBuffer.msgs_size());
    ASSERT_EQ(1, reader->_bufferCursor);
    ASSERT_EQ(3, timestamp);
    progress.clear_topicprogress();
    ec = reader->getReaderProgress(progress);
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ(1, progress.topicprogress_size());
    EXPECT_EQ(1, progress.topicprogress(0).partprogress_size());
    ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 0, 100, 40000, 3, 0, "logicTopic"));

    // 往后seek
    progress.mutable_topicprogress(0)->mutable_partprogress(0)->set_timestamp(5);
    ec = reader->seekByProgress(progress);
    ASSERT_EQ(ERROR_NONE, ec);
    ec = reader->read(timestamp, msg);
    EXPECT_EQ(2, msg.msgid());
    EXPECT_EQ(6, msg.timestamp());
    EXPECT_EQ(9, timestamp);
    ec = reader->read(timestamp, msg);
    EXPECT_EQ(3, msg.msgid());
    EXPECT_EQ(9, msg.timestamp());
    EXPECT_EQ(12, timestamp);
    progress.clear_topicprogress();
    ec = reader->getReaderProgress(progress);
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ(1, progress.topicprogress_size());
    EXPECT_EQ(1, progress.topicprogress(0).partprogress_size());
    ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 0, 100, 40000, 12, 0, "logicTopic"));

    // 往前seek, 没设force走不了
    progress.mutable_topicprogress(0)->mutable_partprogress(0)->set_timestamp(5);
    ec = reader->seekByProgress(progress);
    EXPECT_EQ(ERROR_NONE, ec);
    ec = reader->read(timestamp, msg);
    EXPECT_EQ(4, msg.msgid());
    EXPECT_EQ(12, msg.timestamp());
    EXPECT_EQ(15, timestamp);

    // 往前seek, 设force成功
    progress.mutable_topicprogress(0)->mutable_partprogress(0)->set_timestamp(5);
    ec = reader->seekByProgress(progress, true);
    EXPECT_EQ(ERROR_NONE, ec);
    ec = reader->read(timestamp, msg);
    EXPECT_EQ(2, msg.msgid());
    EXPECT_EQ(6, msg.timestamp());
    EXPECT_EQ(9, timestamp);
    progress.clear_topicprogress();
    ec = reader->getReaderProgress(progress);
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ(1, progress.topicprogress_size());
    EXPECT_EQ(1, progress.topicprogress(0).partprogress_size());
    ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 0, 100, 40000, 9, 0, "logicTopic"));
}

TEST_F(SwiftReaderImplTest, testBatchWithForceSeek) {
    SwiftReaderConfig config;
    config.batchReadCount = 3;
    uint32_t partitionCount = 2;
    uint32_t messageCount = 4;
    SwiftReaderImplPtr reader(FakeClientHelper::createSimpleReader(_adapter, config, partitionCount, messageCount));
    Message msg;
    int64_t timestamp = -1;
    reader->read(timestamp, msg); // read one
    ASSERT_EQ(0, msg.timestamp());
    ASSERT_EQ(3, reader->_messageBuffer.msgs_size());
    ASSERT_EQ(1, reader->_bufferCursor);
    ASSERT_EQ(3, timestamp);

    int64_t seekTimestamp = 6;
    reader->seekByTimestamp(seekTimestamp, true);

    reader->read(timestamp, msg); // read one
    ASSERT_EQ(6, msg.timestamp());
    ASSERT_EQ(6, timestamp);

    seekTimestamp = 2;
    reader->seekByTimestamp(seekTimestamp, true);

    reader->read(timestamp, msg);
    ASSERT_EQ(3, msg.timestamp());
    ASSERT_EQ(3, timestamp);

    seekTimestamp = 11;
    reader->seekByTimestamp(seekTimestamp, true);
    reader->read(timestamp, msg);
    ASSERT_EQ(12, msg.timestamp());
    ASSERT_EQ(11, timestamp);

    ErrorCode ec = reader->read(timestamp, msg);
    ASSERT_EQ(ec, ERROR_CLIENT_NO_MORE_MESSAGE);
}

TEST_F(SwiftReaderImplTest, testTryRead) {
    Messages msg2;
    bool ret = _reader->tryRead(msg2);
    EXPECT_TRUE(!ret);
    ASSERT_EQ(int64_t(0), _reader->_checkpointTimestamp);

    Message msg;
    msg.set_timestamp(int64_t(3));
    FakeSwiftSinglePartitionReader *singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[0]);
    singleReader->push(msg);

    msg.set_timestamp(int64_t(1));
    singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[1]);
    singleReader->push(msg);
    msg.set_timestamp(int64_t(4));
    singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[1]);
    singleReader->push(msg);

    msg.set_timestamp(int64_t(2));
    singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[2]);
    singleReader->push(msg);

    ret = _reader->tryRead(msg2);
    EXPECT_TRUE(ret);
    ASSERT_EQ(int32_t(1), msg2.msgs_size());
    ASSERT_EQ((int64_t)1, msg2.msgs(0).timestamp());
    ASSERT_EQ((int64_t)2, _reader->_checkpointTimestamp);

    ret = _reader->tryRead(msg2);
    EXPECT_TRUE(ret);
    ASSERT_EQ(int32_t(1), msg2.msgs_size());
    ASSERT_EQ((int64_t)2, msg2.msgs(0).timestamp());
    ASSERT_EQ((int64_t)3, _reader->_checkpointTimestamp);

    ret = _reader->tryRead(msg2);
    EXPECT_TRUE(ret);
    ASSERT_EQ(int32_t(1), msg2.msgs_size());
    ASSERT_EQ((int64_t)3, msg2.msgs(0).timestamp());
    ASSERT_EQ((int64_t)3, _reader->_checkpointTimestamp);

    ret = _reader->tryRead(msg2);
    EXPECT_TRUE(ret);
    ASSERT_EQ(int32_t(1), msg2.msgs_size());
    ASSERT_EQ((int64_t)4, msg2.msgs(0).timestamp());
    ASSERT_EQ((int64_t)3, _reader->_checkpointTimestamp);

    // no more message
    singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[0]);
    singleReader->setNextTimestamp(10);
    singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[1]);
    singleReader->setNextTimestamp(11);
    singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[2]);
    singleReader->setNextTimestamp(12);
    ret = _reader->tryRead(msg2);
    EXPECT_TRUE(!ret);
    ASSERT_EQ(int64_t(3), _reader->_checkpointTimestamp);
}

TEST_F(SwiftReaderImplTest, testTryFillBuffer) {
    int64_t interval = 0;
    FakeSwiftSinglePartitionReader *singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[0]);
    singleReader->setTryFillBufferInterval(3);
    singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[1]);
    singleReader->setTryFillBufferInterval(1);
    singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[2]);
    singleReader->setTryFillBufferInterval(2);

    interval = _reader->tryFillBuffer();
    ASSERT_EQ(interval, (int64_t)1);
}

TEST_F(SwiftReaderImplTest, testFillBuffer) {
    FakeSwiftSinglePartitionReader *singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[0]);
    singleReader->setFillBufferInterval(8);
    singleReader->setTryFillBufferInterval(10 * 1000);
    singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[1]);
    singleReader->setFillBufferInterval(1);
    singleReader->setTryFillBufferInterval(10 * 1000);
    singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[2]);
    singleReader->setFillBufferInterval(4);
    singleReader->setTryFillBufferInterval(10 * 1000);

    ErrorCode ec = _reader->fillBuffer(500);
    ASSERT_EQ((int64_t)0, _reader->getUnReadMsgCount());
    EXPECT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, ec);

    ec = _reader->fillBuffer(2 * 1000);
    EXPECT_EQ(ERROR_NONE, ec);
    Messages msgs;

    ASSERT_EQ((int64_t)1, _reader->getUnReadMsgCount());
    EXPECT_TRUE(_reader->tryRead(msgs));
    ASSERT_EQ((int64_t)1, msgs.msgs(0).timestamp());

    ec = _reader->fillBuffer(5 * 1000);
    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_EQ((int64_t)1, _reader->getUnReadMsgCount());
    EXPECT_TRUE(_reader->tryRead(msgs));
    ASSERT_EQ((int64_t)4, msgs.msgs(0).timestamp());

    ec = _reader->fillBuffer(5 * 1000);
    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_EQ((int64_t)1, _reader->getUnReadMsgCount());
    EXPECT_TRUE(_reader->tryRead(msgs));
    ASSERT_EQ((int64_t)8, msgs.msgs(0).timestamp());

    ec = _reader->fillBuffer(1 * 1000);
    EXPECT_TRUE(ec == ERROR_CLIENT_NO_MORE_MESSAGE);
    ASSERT_EQ((int64_t)0, _reader->getUnReadMsgCount());
}

TEST_F(SwiftReaderImplTest, testFillBufferWithTryFillBufferReturnImmediately) {
    FakeSwiftSinglePartitionReader *singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[0]);
    singleReader->setFillBufferInterval(-1);
    singleReader->setTryFillBufferInterval(10 * 1000);
    singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[1]);
    singleReader->setFillBufferInterval(-1);
    singleReader->setTryFillBufferInterval(10 * 1000);
    singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[2]);
    singleReader->setFillBufferInterval(-1);
    singleReader->setTryFillBufferInterval(10 * 1000);
    ErrorCode ec = _reader->fillBuffer(500 * 1000);
    EXPECT_TRUE(ec == ERROR_NONE);
    ASSERT_EQ((int64_t)3, _reader->getUnReadMsgCount());
}

TEST_F(SwiftReaderImplTest, testReadWithFatalError) {
    FakeSwiftSinglePartitionReader *singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[0]);
    singleReader->setErrorCode(ERROR_NONE);
    singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[1]);
    singleReader->setErrorCode(ERROR_CLIENT_ARPC_ERROR);
    singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[2]);
    singleReader->setErrorCode(ERROR_CLIENT_SYS_STOPPED);

    Message msg;
    int64_t timestamp;
    ErrorCode ec = _reader->read(timestamp, msg);
    ASSERT_EQ(ec, ERROR_CLIENT_ARPC_ERROR);
}

TEST_F(SwiftReaderImplTest, testReadWithTryReadOnce) {

    Message msg;
    msg.set_timestamp(int64_t(3));
    FakeSwiftSinglePartitionReader *singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[0]);
    singleReader->push(msg);

    Message msg2;
    int64_t timestamp;
    ErrorCode ec = _reader->read(timestamp, msg2);
    ASSERT_EQ(ec, ERROR_NONE);
    ASSERT_EQ((int64_t)3, msg2.timestamp());
    ASSERT_EQ((int64_t)3, _reader->getUnReadMsgCount());
}

TEST_F(SwiftReaderImplTest, testReadWithTryReadTwice) {
    Message msg2;
    int64_t timestamp;
    ErrorCode ec = _reader->read(timestamp, msg2);
    ASSERT_EQ(ec, ERROR_NONE);
    ASSERT_EQ((int64_t)100, msg2.timestamp()); // FakeSwiftSinglePartitionReader will addMsg
    ASSERT_EQ((int64_t)2, _reader->getUnReadMsgCount());
}

TEST_F(SwiftReaderImplTest, testReadWithTryReadFailed) {

    FakeSwiftSinglePartitionReader *singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[0]);
    singleReader->setFillBufferInterval(2);
    singleReader->setTryFillBufferInterval(10 * 1000);
    singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[1]);
    singleReader->setFillBufferInterval(2);
    singleReader->setTryFillBufferInterval(10 * 1000);
    singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[2]);
    singleReader->setFillBufferInterval(2);
    singleReader->setTryFillBufferInterval(10 * 1000);

    Message msg2;
    int64_t timestamp;
    ErrorCode ec = _reader->read(timestamp, msg2, 1000);
    ASSERT_EQ(ec, ERROR_CLIENT_NO_MORE_MESSAGE);
}

TEST_F(SwiftReaderImplTest, testCheckCurrentError) {

    FakeSwiftSinglePartitionReader *singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[0]);
    singleReader->setErrorCode(ERROR_UNKNOWN);
    singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[1]);
    singleReader->setErrorCode(ERROR_UNKNOWN);
    singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[2]);
    singleReader->setErrorCode(ERROR_CLIENT_READ_MESSAGE_TIMEOUT);

    ErrorCode ec = ERROR_NONE;
    string msg;
    _reader->checkCurrentError(ec, msg);
    EXPECT_TRUE(ec == ERROR_CLIENT_READ_MESSAGE_TIMEOUT);
    ASSERT_EQ(string("error msg;error msg;error msg;"), msg);
}

TEST_F(SwiftReaderImplTest, testGetNextMsgTimestamp) {
    int64_t timestamp = 0;
    timestamp = _reader->getNextMsgTimestamp();
    EXPECT_TRUE(timestamp == -1);

    Message msg;
    FakeSwiftSinglePartitionReader *singleReader1 =
        dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[0]);
    FakeSwiftSinglePartitionReader *singleReader2 =
        dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[1]);
    FakeSwiftSinglePartitionReader *singleReader3 =
        dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[2]);
    singleReader1->setAutoAddMsg(false);
    singleReader2->setAutoAddMsg(false);
    singleReader3->setAutoAddMsg(false);
    msg.set_timestamp(int64_t(3));
    singleReader1->push(msg);
    ASSERT_EQ((int64_t)-1, _reader->getNextMsgTimestamp());

    msg.set_timestamp(int64_t(1));
    singleReader2->push(msg);
    ASSERT_EQ((int64_t)-1, _reader->getNextMsgTimestamp());

    msg.set_timestamp(int64_t(2));
    singleReader3->push(msg);
    ASSERT_EQ((int64_t)1, _reader->getNextMsgTimestamp());

    timestamp = -1;
    EXPECT_TRUE(ERROR_NONE == _reader->read(timestamp, msg));
    ASSERT_EQ(int64_t(2), timestamp);
    ASSERT_EQ(int64_t(1), msg.timestamp());
    EXPECT_TRUE(ERROR_NONE == _reader->read(timestamp, msg));
    ASSERT_EQ(int64_t(2), timestamp);
    ASSERT_EQ(int64_t(2), msg.timestamp());
    EXPECT_TRUE(ERROR_NONE == _reader->read(timestamp, msg));
    ASSERT_EQ(int64_t(2), timestamp);
    ASSERT_EQ(int64_t(3), msg.timestamp());
    EXPECT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, _reader->read(timestamp, msg, 10));
}

TEST_F(SwiftReaderImplTest, testGetMaxLastMsgTimestamp) {
    int64_t timestamp = 0;
    timestamp = _reader->getMaxLastMsgTimestamp();
    EXPECT_TRUE(timestamp == -1);

    Message msg;
    FakeSwiftSinglePartitionReader *singleReader1 =
        dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[0]);
    FakeSwiftSinglePartitionReader *singleReader2 =
        dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[1]);
    FakeSwiftSinglePartitionReader *singleReader3 =
        dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[2]);
    singleReader1->setAutoAddMsg(false);
    singleReader2->setAutoAddMsg(false);
    singleReader3->setAutoAddMsg(false);
    msg.set_timestamp(int64_t(3));
    singleReader1->push(msg);
    ASSERT_EQ((int64_t)-1, _reader->getMaxLastMsgTimestamp());

    msg.set_timestamp(int64_t(1));
    singleReader2->push(msg);
    ASSERT_EQ((int64_t)-1, _reader->getMaxLastMsgTimestamp());

    msg.set_timestamp(int64_t(2));
    singleReader3->push(msg);
    ASSERT_EQ((int64_t)-1, _reader->getMaxLastMsgTimestamp());

    timestamp = -1;
    EXPECT_TRUE(ERROR_NONE == _reader->read(timestamp, msg));
    ASSERT_EQ(int64_t(2), timestamp);
    ASSERT_EQ(int64_t(1), msg.timestamp());
    ASSERT_EQ((int64_t)1, _reader->getMaxLastMsgTimestamp());
    EXPECT_TRUE(ERROR_NONE == _reader->read(timestamp, msg));
    ASSERT_EQ(int64_t(2), timestamp);
    ASSERT_EQ(int64_t(2), msg.timestamp());
    ASSERT_EQ((int64_t)2, _reader->getMaxLastMsgTimestamp());
    EXPECT_TRUE(ERROR_NONE == _reader->read(timestamp, msg));
    ASSERT_EQ(int64_t(2), timestamp);
    ASSERT_EQ(int64_t(3), msg.timestamp());
    ASSERT_EQ((int64_t)3, _reader->getMaxLastMsgTimestamp());
    EXPECT_TRUE(ERROR_NONE != _reader->read(timestamp, msg, 10));
}

TEST_F(SwiftReaderImplTest, testGetSwiftPartitionStatus) {

    SwiftPartitionStatus status;
    status.refreshTime = 2;
    status.maxMessageId = -1;
    status.maxMessageTimestamp = 2;
    FakeSwiftSinglePartitionReader *singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[0]);
    singleReader->setSwiftPartitionStatus(status);

    status.refreshTime = 3;
    status.maxMessageTimestamp = 3;
    singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[1]);
    singleReader->setSwiftPartitionStatus(status);

    status.refreshTime = 1;
    status.maxMessageTimestamp = 1;
    singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[2]);
    singleReader->setSwiftPartitionStatus(status);

    status = _reader->getSwiftPartitionStatus();
    ASSERT_EQ((int64_t)1, status.refreshTime);
    ASSERT_EQ((int64_t)-1, status.maxMessageId);
    ASSERT_EQ((int64_t)3, status.maxMessageTimestamp);
}

TEST_F(SwiftReaderImplTest, testGetMinCheckpointTimeStamp) {
    EXPECT_EQ(0, _reader->getMinCheckpointTimestamp());
    FakeSwiftSinglePartitionReader *singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[0]);
    protocol::Message msg1;
    msg1.set_timestamp(10);
    singleReader->push(msg1);
    EXPECT_EQ(0, _reader->getMinCheckpointTimestamp());
    singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[1]);
    protocol::Message msg2;
    msg2.set_timestamp(8);
    singleReader->push(msg2);
    EXPECT_EQ(0, _reader->getMinCheckpointTimestamp());
    singleReader = dynamic_cast<FakeSwiftSinglePartitionReader *>(_reader->_readers[2]);
    protocol::Message msg3;
    msg3.set_timestamp(15);
    singleReader->push(msg3);
    EXPECT_EQ(8, _reader->getMinCheckpointTimestamp());
}

TEST_F(SwiftReaderImplTest, testModifiedBufferSize) {
    vector<uint32_t> partitionIds;
    partitionIds.push_back(0);
    partitionIds.push_back(1);
    SwiftReaderConfig config;
    config.topicName = "topicName";
    config.readBufferSize = 1024;
    dynamic_cast<FakeSwiftAdminAdapter *>(_adapter.get())->setPartCount((uint32_t)2);

    config.readPartVec = partitionIds;
    SwiftReaderImpl swiftReader(_adapter, SwiftRpcChannelManagerPtr(), _topicInfo);
    ASSERT_EQ(ERROR_NONE, swiftReader.init(config));
    ASSERT_EQ((size_t)2, swiftReader._readers.size());

    SwiftSinglePartitionReader *singleReader = swiftReader._readers[0];
    ASSERT_EQ((uint64_t)1024 / 2, singleReader->getReaderConfig().readBufferSize);
    singleReader = swiftReader._readers[1];
    ASSERT_EQ((uint64_t)1024 / 2, singleReader->getReaderConfig().readBufferSize);
}

TEST_F(SwiftReaderImplTest, testSinglePartitionReader) {

    string topicName = "topic_name";
    uint32_t partitionId = 1;
    SwiftReaderConfig config;
    config.topicName = topicName;
    config.readBufferSize = 1024;
    config.oneRequestReadCount = 128;
    SwiftAdminAdapterPtr adminAdapter;
    SwiftSinglePartitionReader *singleReader =
        new SwiftSinglePartitionReader(adminAdapter, SwiftRpcChannelManagerPtr(), partitionId, config);
    FakeSwiftTransportAdapter<TRT_GETMESSAGE> *fakeAdapter =
        new FakeSwiftTransportAdapter<TRT_GETMESSAGE>(topicName, partitionId);
    FakeSwiftTransportClient *fakeClient = fakeAdapter->getFakeTransportClient();
    singleReader->resetTransportAdapter(fakeAdapter);
    uint32_t messageCount = 1499;
    int64_t msgId = 0;
    FakeClientHelper::makeData(fakeClient, messageCount, msgId, 3, "test_data");
    SwiftReaderImpl reader(adminAdapter, SwiftRpcChannelManagerPtr(), _topicInfo);
    reader._readers.push_back(singleReader);
    reader._exceedLimitVec.push_back(false);
    reader._timestamps.push_back(-1);

    Message msg;
    int64_t timestamp;
    ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, reader.read(timestamp, msg, 10));
    fakeClient->finishAsyncCall();

    for (uint32_t i = 0; i < messageCount / 10; ++i, ++msgId) {
        Message msg;
        ASSERT_EQ(ERROR_NONE, reader.read(timestamp, msg, 10));
        ASSERT_EQ(msgId, msg.msgid());
        ASSERT_EQ(msgId * 3, msg.timestamp());
        ASSERT_EQ(string("test_data"), msg.data());
        fakeClient->finishAsyncCall();
    }

    // test seek by id
    msgId = messageCount / 2;
    ASSERT_EQ(ERROR_NONE, reader.seekByMessageId(msgId));
    ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, reader.read(timestamp, msg, 1));
    fakeClient->finishAsyncCall();

    for (uint32_t i = msgId; i < messageCount; ++i, ++msgId) {
        Message msg;
        ASSERT_EQ(ERROR_NONE, reader.read(timestamp, msg, 1));
        ASSERT_EQ(msgId, msg.msgid());
        ASSERT_EQ(msgId * 3, msg.timestamp());
        ASSERT_EQ(string("test_data"), msg.data());
        fakeClient->finishAsyncCall();
    }

    // reach max msg
    ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, reader.read(timestamp, msg, 10));
    fakeClient->finishAsyncCall();
    fakeClient->setAutoAsyncCall(true);
    ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, reader.read(timestamp, msg, 10));

    // test seek id > max msg id
    ASSERT_EQ(ERROR_NONE, reader.seekByMessageId(messageCount + 10));
    ASSERT_EQ(int64_t(messageCount + 10), singleReader->getNextMsgId());
    ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, reader.read(timestamp, msg, 1000000));
    ASSERT_EQ((int64_t)messageCount, singleReader->getNextMsgId());
    msgId = messageCount;

    FakeClientHelper::makeData(fakeClient, 10, messageCount, 3, "test_data");
    messageCount += 10;
    ASSERT_EQ(ERROR_NONE, reader.read(timestamp, msg, 1000000));
    ASSERT_EQ(msgId, msg.msgid());
    ASSERT_EQ(msgId * 3, msg.timestamp());
    ASSERT_EQ(string("test_data"), msg.data());
    ++msgId;
    fakeClient->finishAsyncCall();

    // add 10 message
    FakeClientHelper::makeData(fakeClient, 10, messageCount, 3, "test_data");
    messageCount += 10;

    for (uint32_t i = msgId; i < messageCount; ++i, ++msgId) {
        Message msg;
        ASSERT_EQ(ERROR_NONE, reader.read(timestamp, msg, 1000000));
        ASSERT_EQ(msgId, msg.msgid());
        ASSERT_EQ(msgId * 3, msg.timestamp());
        ASSERT_EQ(string("test_data"), msg.data());
        fakeClient->finishAsyncCall();
    }
}

TEST_F(SwiftReaderImplTest, testSetRequiredFilterNames) {
    string topicName = "topic_name";
    uint32_t partitionId = 1;
    SwiftReaderConfig config;
    config.topicName = topicName;
    config.readBufferSize = 1024;
    config.oneRequestReadCount = 128;
    config.batchReadCount = 1;
    SwiftAdminAdapterPtr adminAdapter;
    SwiftSinglePartitionReader *singleReader =
        new SwiftSinglePartitionReader(adminAdapter, SwiftRpcChannelManagerPtr(), partitionId, config);
    FakeSwiftTransportAdapter<TRT_GETMESSAGE> *fakeAdapter =
        new FakeSwiftTransportAdapter<TRT_GETMESSAGE>(topicName, partitionId);
    FakeSwiftTransportClient *fakeClient = fakeAdapter->getFakeTransportClient();
    fakeClient->setAutoAsyncCall();
    singleReader->resetTransportAdapter(fakeAdapter);
    SwiftReaderImpl reader(adminAdapter, SwiftRpcChannelManagerPtr(), _topicInfo);
    reader._readers.push_back(singleReader);
    reader._exceedLimitVec.push_back(false);
    reader._timestamps.push_back(-1);

    FakeClientHelper::makeData(fakeClient, 20, 0);

    int64_t timestamp = 0;
    Message msg;
    vector<string> requiredNames;
    requiredNames.push_back("f1");
    reader.setRequiredFieldNames(requiredNames);
    for (uint32_t i = 0; i < 10; ++i) {
        ASSERT_EQ(ERROR_NONE, reader.read(timestamp, msg));
        ASSERT_EQ((int64_t)i, msg.msgid());
    }
    ASSERT_EQ((int64_t)10, singleReader->getUnReadMsgCount());

    requiredNames.push_back("f2");
    reader.setRequiredFieldNames(requiredNames);
    ASSERT_EQ((int64_t)0, singleReader->getUnReadMsgCount());
    for (uint32_t i = 10; i < 20; ++i) {
        ASSERT_EQ(ERROR_NONE, reader.read(timestamp, msg));
        ASSERT_EQ((int64_t)i, msg.msgid());
    }
    ASSERT_EQ((int64_t)0, singleReader->getUnReadMsgCount());
}

TEST_F(SwiftReaderImplTest, testSinglePartitionTimeStampLimit) {

    string topicName = "topic_name";
    uint32_t partitionId = 1;
    SwiftReaderConfig config;
    config.topicName = topicName;
    config.readBufferSize = 1024;
    config.oneRequestReadCount = 128;
    config.batchReadCount = 1;
    SwiftAdminAdapterPtr adminAdapter;
    SwiftSinglePartitionReader *singleReader =
        new SwiftSinglePartitionReader(adminAdapter, SwiftRpcChannelManagerPtr(), partitionId, config);
    FakeSwiftTransportAdapter<TRT_GETMESSAGE> *fakeAdapter =
        new FakeSwiftTransportAdapter<TRT_GETMESSAGE>(topicName, partitionId);
    FakeSwiftTransportClient *fakeClient = fakeAdapter->getFakeTransportClient();
    singleReader->resetTransportAdapter(fakeAdapter);
    uint32_t messageCount = 1000;
    int64_t msgId = 0;
    FakeClientHelper::makeData(fakeClient, messageCount, msgId, 3, "test_data");
    SwiftReaderImpl reader(adminAdapter, SwiftRpcChannelManagerPtr(), _topicInfo);
    reader._readers.push_back(singleReader);
    reader._exceedLimitVec.push_back(false);
    reader._timestamps.push_back(-1);
    uint32_t limitMessageCount = 500;
    int64_t timestampLimit = limitMessageCount * 3;
    int64_t acceptTimestamp;

    reader.setTimestampLimit(timestampLimit, acceptTimestamp);
    ASSERT_EQ(timestampLimit, acceptTimestamp);

    Message msg;
    int64_t timestamp;
    ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, reader.read(timestamp, msg, 10));
    fakeClient->finishAsyncCall();

    for (uint32_t i = 0; i <= limitMessageCount; ++i, ++msgId) {
        Message msg;
        ASSERT_EQ(ERROR_NONE, reader.read(timestamp, msg, 10));
        ASSERT_EQ(msgId, msg.msgid());
        ASSERT_EQ(msgId * 3, msg.timestamp());
        ASSERT_EQ(string("test_data"), msg.data());
        fakeClient->finishAsyncCall();
    }
    ASSERT_EQ(ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT, reader.read(timestamp, msg, 10));
    fakeClient->finishAsyncCall();
    { // seek forward
        limitMessageCount += 2;
        ASSERT_EQ(ERROR_NONE, reader.seekByMessageId(limitMessageCount));
        fakeClient->finishAsyncCall();
        ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, reader.read(timestamp, msg, 1));
        fakeClient->finishAsyncCall();
        ASSERT_EQ(ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT, reader.read(timestamp, msg, 10));
        fakeClient->finishAsyncCall();
        timestampLimit = limitMessageCount * 3;
        reader.setTimestampLimit(timestampLimit, acceptTimestamp);
        ASSERT_EQ(timestampLimit, acceptTimestamp);
        ASSERT_EQ(ERROR_NONE, reader.read(timestamp, msg, 1));
        ASSERT_EQ(int64_t(limitMessageCount), msg.msgid());
        fakeClient->finishAsyncCall();
        ASSERT_EQ(ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT, reader.read(timestamp, msg, 10));
        fakeClient->finishAsyncCall();
    }
    { // seek back
        ASSERT_EQ(ERROR_NONE, reader.seekByMessageId(limitMessageCount - 1));
        fakeClient->finishAsyncCall();
        ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, reader.read(timestamp, msg, 1));
        fakeClient->finishAsyncCall();
        ASSERT_EQ(ERROR_NONE, reader.read(timestamp, msg, 1));
        ASSERT_EQ(int64_t(limitMessageCount - 1), msg.msgid());
        fakeClient->finishAsyncCall();
        ASSERT_EQ(ERROR_NONE, reader.read(timestamp, msg, 1));
        ASSERT_EQ(int64_t(limitMessageCount), msg.msgid());
        fakeClient->finishAsyncCall();
        ASSERT_EQ(ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT, reader.read(timestamp, msg, 10));
        fakeClient->finishAsyncCall();
    }
    { // seek back and set new timestamp
        ASSERT_EQ(ERROR_NONE, reader.seekByMessageId(limitMessageCount - 1));
        fakeClient->finishAsyncCall();
        timestampLimit = (limitMessageCount - 1) * 3;
        reader.setTimestampLimit(timestampLimit, acceptTimestamp);
        ASSERT_EQ(timestampLimit, acceptTimestamp);
        ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, reader.read(timestamp, msg, 1));
        fakeClient->finishAsyncCall();
        ASSERT_EQ(ERROR_NONE, reader.read(timestamp, msg, 1));
        ASSERT_EQ(int64_t(limitMessageCount - 1), msg.msgid());
        fakeClient->finishAsyncCall();
        ASSERT_EQ(ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT, reader.read(timestamp, msg, 10));
        fakeClient->finishAsyncCall();
    }
    { // set max timestamp
        ASSERT_EQ(ERROR_NONE, reader.seekByMessageId(limitMessageCount - 5));
        fakeClient->finishAsyncCall();
        timestampLimit = (limitMessageCount - 5) * 3;
        reader.setTimestampLimit(timestampLimit, acceptTimestamp);
        ASSERT_EQ(timestampLimit, acceptTimestamp);
        ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, reader.read(timestamp, msg, 1));
        fakeClient->finishAsyncCall();
        ASSERT_EQ(ERROR_NONE, reader.read(timestamp, msg, 1));
        ASSERT_EQ(int64_t(limitMessageCount - 5), msg.msgid());
        fakeClient->finishAsyncCall();
        ASSERT_EQ(ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT, reader.read(timestamp, msg, 10));
        fakeClient->finishAsyncCall();
        int64_t MAX_TIME_STAMP = numeric_limits<int64_t>::max();
        reader.setTimestampLimit(MAX_TIME_STAMP, acceptTimestamp);
        ASSERT_EQ(ERROR_NONE, reader.read(timestamp, msg, 1));
        ASSERT_EQ(int64_t(limitMessageCount - 4), msg.msgid());
    }

    { // set old timestamp, get last msg timestamp
        timestampLimit = (limitMessageCount - 100) * 3;
        reader.setTimestampLimit(timestampLimit, acceptTimestamp);
        ASSERT_EQ(int64_t(limitMessageCount - 4) * 3, acceptTimestamp);
        ASSERT_EQ(ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT, reader.read(timestamp, msg, 10));
        fakeClient->finishAsyncCall();
    }
    { // empty message
        fakeClient->clearMessage();
        timestampLimit = autil::TimeUtility::currentTime();
        reader.setTimestampLimit(timestampLimit, acceptTimestamp);
        ASSERT_EQ(timestampLimit, acceptTimestamp);
        ASSERT_EQ(ERROR_NONE, reader.seekByMessageId(0));
        fakeClient->finishAsyncCall();
        ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, reader.read(timestamp, msg, 1));
        fakeClient->finishAsyncCall();
        ASSERT_EQ(ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT, reader.read(timestamp, msg, 10));
        fakeClient->finishAsyncCall();
    }
}

TEST_F(SwiftReaderImplTest, testMulitPartitionTimeStampLimit) {
    string topicName = "topic_name";
    SwiftAdminAdapterPtr adminAdapter;
    SwiftReaderImpl reader(adminAdapter, SwiftRpcChannelManagerPtr(), _topicInfo);
    vector<FakeSwiftTransportClient *> fakeClientVec;
    for (uint32_t i = 0; i < 3; i++) {
        uint32_t partitionId = i;
        SwiftReaderConfig config;
        config.topicName = topicName;
        config.readBufferSize = 1;
        config.oneRequestReadCount = 1;
        SwiftSinglePartitionReader *singleReader =
            new SwiftSinglePartitionReader(adminAdapter, SwiftRpcChannelManagerPtr(), partitionId, config);
        FakeSwiftTransportAdapter<TRT_GETMESSAGE> *fakeAdapter =
            new FakeSwiftTransportAdapter<TRT_GETMESSAGE>(topicName, partitionId);
        FakeSwiftTransportClient *fakeClient = fakeAdapter->getFakeTransportClient();
        fakeClientVec.push_back(fakeClient);
        singleReader->resetTransportAdapter(fakeAdapter);
        FakeClientHelper::makeData(fakeClient, 10, 0, i + 1, "test_data");
        reader._readers.push_back(singleReader);
        reader._exceedLimitVec.push_back(false);
        reader._timestamps.push_back(-1);
    }
    int64_t acceptTimestamp;
    int64_t timestamp;
    {
        // all single readers are ok
        reader.setTimestampLimit(5, acceptTimestamp);
        ASSERT_EQ(int64_t(5), acceptTimestamp);
        Message msg;
        ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, reader.read(timestamp, msg, 10));
        finishAsyncCall(fakeClientVec);
        for (uint32_t i = 0; i < 11; ++i) {
            Message msg;
            ASSERT_EQ(ERROR_NONE, reader.read(timestamp, msg, 10));
            ASSERT_EQ(string("test_data"), msg.data());
            finishAsyncCall(fakeClientVec);
        }
        ASSERT_EQ(ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT, reader.read(timestamp, msg, 10));
        finishAsyncCall(fakeClientVec);
        { // seek forward
            ASSERT_EQ(ERROR_NONE, reader.seekByMessageId(6));
            finishAsyncCall(fakeClientVec);
            ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, reader.read(timestamp, msg, 1));
            finishAsyncCall(fakeClientVec);
            ASSERT_EQ(ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT, reader.read(timestamp, msg, 10));
            finishAsyncCall(fakeClientVec);
            reader.setTimestampLimit(6, acceptTimestamp);
            ASSERT_EQ(int64_t(6), acceptTimestamp);
            ASSERT_EQ(ERROR_NONE, reader.read(timestamp, msg, 1));
            finishAsyncCall(fakeClientVec);
            ASSERT_EQ(int64_t(6), msg.msgid());
            ASSERT_EQ(ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT, reader.read(timestamp, msg, 10));
            finishAsyncCall(fakeClientVec);
        }
        { // seek back
            ASSERT_EQ(ERROR_NONE, reader.seekByMessageId(5));
            finishAsyncCall(fakeClientVec);
            ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, reader.read(timestamp, msg, 1));
            finishAsyncCall(fakeClientVec);
            ASSERT_EQ(ERROR_NONE, reader.read(timestamp, msg, 1));
            finishAsyncCall(fakeClientVec);
            ASSERT_EQ(int64_t(5), msg.msgid());
            ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, reader.read(timestamp, msg, 1));
            finishAsyncCall(fakeClientVec); // fill message
            ASSERT_EQ(ERROR_NONE, reader.read(timestamp, msg, 1));
            ASSERT_EQ(int64_t(6), msg.msgid());
            finishAsyncCall(fakeClientVec);
            ASSERT_EQ(ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT, reader.read(timestamp, msg, 10));
            finishAsyncCall(fakeClientVec);
        }
        { // seek back and reset timestamp
            ASSERT_EQ(ERROR_NONE, reader.seekByMessageId(5));
            reader.setTimestampLimit(5, acceptTimestamp);
            ASSERT_EQ(int64_t(5), acceptTimestamp);
            finishAsyncCall(fakeClientVec);
            ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, reader.read(timestamp, msg, 1));
            finishAsyncCall(fakeClientVec);
            ASSERT_EQ(ERROR_NONE, reader.read(timestamp, msg, 1));
            finishAsyncCall(fakeClientVec);
            ASSERT_EQ(int64_t(5), msg.msgid());
            ASSERT_EQ(ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT, reader.read(timestamp, msg, 10));
            finishAsyncCall(fakeClientVec);
        }
        { // seek old timestamp
            reader.setTimestampLimit(4, acceptTimestamp);
            ASSERT_EQ(int64_t(5), acceptTimestamp);
            ASSERT_EQ(ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT, reader.read(timestamp, msg, 10));
            finishAsyncCall(fakeClientVec);
        }
    }
    {
        // one single reader is abnormal
        ASSERT_EQ(ERROR_NONE, reader.seekByMessageId(0));
        finishAsyncCall(fakeClientVec);
        reader.setTimestampLimit(5, acceptTimestamp);
        ASSERT_EQ(int64_t(5), acceptTimestamp);
        fakeClientVec[0]->setTimeoutCount(10);
        Message msg;
        ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, reader.read(timestamp, msg, 10));
        finishAsyncCall(fakeClientVec);
        for (uint32_t i = 0; i < 5; ++i) {
            Message msg;
            ASSERT_EQ(ERROR_NONE, reader.read(timestamp, msg, 10));
            ASSERT_EQ(string("test_data"), msg.data());
            finishAsyncCall(fakeClientVec);
        }
        for (uint32_t i = 0; i < 5; ++i) {
            ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, reader.read(timestamp, msg, 10));
            finishAsyncCall(fakeClientVec);
        }
        for (uint32_t i = 0; i < 11; ++i) {
            Message msg;
            if (i % 2 == 0) {
                ASSERT_EQ(ERROR_NONE, reader.read(timestamp, msg, 10));
                ASSERT_EQ(string("test_data"), msg.data());
            } else { // to fill message
                ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, reader.read(timestamp, msg, 10));
            }
            finishAsyncCall(fakeClientVec);
        }

        ASSERT_EQ(ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT, reader.read(timestamp, msg, 10));
        finishAsyncCall(fakeClientVec);
        ASSERT_EQ(ERROR_NONE, reader.seekByMessageId(6));
        finishAsyncCall(fakeClientVec);
        ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, reader.read(timestamp, msg, 1));
        finishAsyncCall(fakeClientVec);
        reader.setTimestampLimit(6, acceptTimestamp);
        ASSERT_EQ(int64_t(6), acceptTimestamp);
        ASSERT_EQ(ERROR_NONE, reader.read(timestamp, msg, 1));
        finishAsyncCall(fakeClientVec);
        ASSERT_EQ(int64_t(6), msg.msgid());
        ASSERT_EQ(ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT, reader.read(timestamp, msg, 10));
    }
    {
        ASSERT_EQ(ERROR_NONE, reader.seekByMessageId(0));
        finishAsyncCall(fakeClientVec);
        reader.setTimestampLimit(5, acceptTimestamp);
        Message msg;
        ASSERT_EQ(int64_t(5), acceptTimestamp);
        ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, reader.read(timestamp, msg, 10));
        finishAsyncCall(fakeClientVec);

        for (uint32_t i = 0; i < 10; ++i) {
            Message msg;
            ASSERT_EQ(ERROR_NONE, reader.read(timestamp, msg, 10));
            ASSERT_EQ(string("test_data"), msg.data());
            finishAsyncCall(fakeClientVec);
        }

        fakeClientVec[2]->setTimeoutCount(1); // have read all message, but this reader become abnormal

        ASSERT_EQ(ERROR_NONE, reader.read(timestamp, msg, 10));
        ASSERT_EQ(string("test_data"), msg.data());
        finishAsyncCall(fakeClientVec);

        ASSERT_EQ(ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT, reader.read(timestamp, msg, 10));
        finishAsyncCall(fakeClientVec);
        ASSERT_EQ(ERROR_NONE, reader.seekByMessageId(6));
        finishAsyncCall(fakeClientVec);
        ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, reader.read(timestamp, msg, 1));
        finishAsyncCall(fakeClientVec);
        reader.setTimestampLimit(6, acceptTimestamp);
        ASSERT_EQ(int64_t(6), acceptTimestamp);
        ASSERT_EQ(ERROR_NONE, reader.read(timestamp, msg, 1));
        finishAsyncCall(fakeClientVec);
        ASSERT_EQ(int64_t(6), msg.msgid());
        ASSERT_EQ(ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT, reader.read(timestamp, msg, 10));
    }
    { // empty message
        Message msg;
        for (size_t i = 0; i < fakeClientVec.size(); i++) {
            fakeClientVec[i]->clearMessage();
        }
        int64_t timestampLimit = autil::TimeUtility::currentTime();
        reader.setTimestampLimit(timestampLimit, acceptTimestamp);
        ASSERT_EQ(timestampLimit, acceptTimestamp);
        ASSERT_EQ(ERROR_NONE, reader.seekByMessageId(0));
        finishAsyncCall(fakeClientVec);
        ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, reader.read(timestamp, msg, 1));
        finishAsyncCall(fakeClientVec);
        ASSERT_EQ(ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT, reader.read(timestamp, msg, 10));
    }
}

TEST_F(SwiftReaderImplTest, testSetTimestampLimit) {
    string topicName = "topic_name";
    SwiftAdminAdapterPtr adminAdapter;
    SwiftReaderImpl reader(adminAdapter, SwiftRpcChannelManagerPtr(), _topicInfo);
    vector<FakeSwiftTransportClient *> fakeClientVec;
    for (uint32_t i = 0; i < 3; i++) {
        uint32_t partitionId = i;
        SwiftReaderConfig config;
        config.topicName = topicName;
        config.readBufferSize = 1;
        config.oneRequestReadCount = 1;
        SwiftSinglePartitionReader *singleReader =
            new SwiftSinglePartitionReader(adminAdapter, SwiftRpcChannelManagerPtr(), partitionId, config);
        singleReader->_lastMsgTimestamp = (i + 1);
        singleReader->_nextTimestamp = singleReader->_lastMsgTimestamp + 1;
        if (i == 2) {
            singleReader->_nextTimestamp += 1;
        }
        reader._readers.push_back(singleReader);
        reader._exceedLimitVec.push_back(true);
        reader._timestamps.push_back(-1);
    }
    int64_t timestampLimit = 2;
    reader.setTimestampLimit(timestampLimit, timestampLimit);
    ASSERT_EQ(int64_t(3), timestampLimit);

    for (size_t i = 0; i < reader._readers.size(); i++) {
        ASSERT_EQ(int64_t(3), reader._readers[i]->_timestampLimit);
        if (i == 2) {
            EXPECT_TRUE(reader._exceedLimitVec[i]);
        } else {
            EXPECT_TRUE(!reader._exceedLimitVec[i]);
        }
    }
}

TEST_F(SwiftReaderImplTest, testGetReaderProgress) {
    string topicName = "topic_name";
    SwiftAdminAdapterPtr adminAdapter;
    SwiftReaderImpl reader(adminAdapter, SwiftRpcChannelManagerPtr(), _topicInfo);
    vector<FakeSwiftTransportClient *> fakeClientVec;
    SwiftReaderConfig config;
    config.topicName = topicName;
    config.swiftFilter.set_from(20000);
    config.swiftFilter.set_to(40000);
    config.checkpointRefreshTimestampOffset = 0;
    ASSERT_EQ(ERROR_NONE, reader.init(config));
    size_t readerSize = reader._config.readPartVec.size();
    ASSERT_EQ(4, readerSize);
    for (uint32_t i = 0; i < readerSize; ++i) {
        reader._readers[i]->_nextTimestamp = 1000 * i;
    }

    { // 1. impl empty, return all singleReader ts
        ReaderProgress progress;
        ASSERT_EQ(ERROR_NONE, reader.getReaderProgress(progress));
        EXPECT_EQ(1, progress.topicprogress_size());
        EXPECT_EQ(4, progress.topicprogress(0).partprogress_size());
        ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 0, 20000, 26215, 0));
        ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 1, 26216, 32769, 1000));
        ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 2, 32770, 39323, 2000));
        ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 3, 39324, 40000, 3000));
    }
    { // 2. last read 3, but _messageBuffer empty
        ReaderProgress progress;
        reader._bufferReadIndex = 3;
        ASSERT_EQ(ERROR_NONE, reader.getReaderProgress(progress));
        EXPECT_EQ(1, progress.topicprogress_size());
        EXPECT_EQ(4, progress.topicprogress(0).partprogress_size());
        ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 0, 20000, 26215, 0));
        ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 1, 26216, 32769, 1000));
        ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 2, 32770, 39323, 2000));
        ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 3, 39324, 40000, 3000));
    }
    // set _messageBuffer
    for (uint32_t i = 0; i < 10; ++i) {
        Message *msg = reader._messageBuffer.add_msgs();
        msg->set_msgid(i * 100);
        msg->set_timestamp(i * 100);
    }
    ///
    { // 3. last read 3, _messageBuffer not empty, but _bufferCursor not set, default 0
        ReaderProgress progress;
        reader._bufferReadIndex = 3;
        ASSERT_EQ(ERROR_NONE, reader.getReaderProgress(progress));
        EXPECT_EQ(1, progress.topicprogress_size());
        EXPECT_EQ(4, progress.topicprogress(0).partprogress_size());
        ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 0, 20000, 26215, 0));
        ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 1, 26216, 32769, 1000));
        ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 2, 32770, 39323, 2000));
        ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 3, 39324, 40000, 0));
    }
    { // 4. last read 3, _messageBuffer not empty,
        // but _bufferCursor equal _messageBuffer size
        ReaderProgress progress;
        reader._bufferReadIndex = 3;
        reader._bufferCursor = 10;
        ASSERT_EQ(ERROR_NONE, reader.getReaderProgress(progress));
        EXPECT_EQ(1, progress.topicprogress_size());
        EXPECT_EQ(4, progress.topicprogress(0).partprogress_size());
        ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 0, 20000, 26215, 0));
        ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 1, 26216, 32769, 1000));
        ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 2, 32770, 39323, 2000));
        ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 3, 39324, 40000, 3000));
    }
    { // 5. last read 3, _messageBuffer not empty,
        // and _bufferCursor less than _messageBuffer size
        ReaderProgress progress;
        reader._bufferReadIndex = 3;
        reader._bufferCursor = 8;
        ASSERT_EQ(ERROR_NONE, reader.getReaderProgress(progress));
        EXPECT_EQ(1, progress.topicprogress_size());
        EXPECT_EQ(4, progress.topicprogress(0).partprogress_size());
        ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 0, 20000, 26215, 0));
        ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 1, 26216, 32769, 1000));
        ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 2, 32770, 39323, 2000));
        ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 3, 39324, 40000, 800));
    }
}

TEST_F(SwiftReaderImplTest, testGetReaderProgressWithMergeMsg) {
    string topicName = "topic_name";
    SwiftReaderConfig config;
    config.swiftFilter.set_from(0);
    config.swiftFilter.set_to(128);
    config.topicName = topicName;
    config.batchReadCount = 3;
    SwiftAdminAdapterPtr adminAdapter;
    SwiftSinglePartitionReader *singleReader =
        new SwiftSinglePartitionReader(adminAdapter, SwiftRpcChannelManagerPtr(), 0, config);
    FakeSwiftTransportAdapter<TRT_GETMESSAGE> *fakeAdapter =
        new FakeSwiftTransportAdapter<TRT_GETMESSAGE>(topicName, 0);
    FakeSwiftTransportClient *fakeClient = fakeAdapter->getFakeTransportClient();
    singleReader->resetTransportAdapter(fakeAdapter);
    uint32_t messageCount = 64;
    int64_t msgId = 0;
    FakeClientHelper::makeData(fakeClient, messageCount, msgId, 3, "test_data", 100, true);
    SwiftReaderImpl reader(adminAdapter, SwiftRpcChannelManagerPtr(), _topicInfo);
    ASSERT_EQ(ERROR_NONE, reader.init(config));
    ASSERT_EQ(1, reader._readers.size());
    auto range = reader._readers[0]->getFilterRange();
    singleReader->setFilterRange(range.first, range.second);
    delete reader._readers[0];
    reader._readers[0] = singleReader;

    Message msg;
    int64_t timestamp;
    ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, reader.read(timestamp, msg, 10));
    ReaderProgress progress;
    ASSERT_EQ(ERROR_NONE, reader.getReaderProgress(progress));
    ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 0, 0, 128, 0, 0, topicName));
    fakeClient->finishAsyncCall();

    for (uint32_t i = 0; i < messageCount; i++) {
        Message msg;
        ASSERT_EQ(ERROR_NONE, reader.read(timestamp, msg, 10));
        ASSERT_EQ(0, msg.msgid());
        ASSERT_EQ(1, msg.timestamp()); // merge msg id and timestamp re-sign in broker,
        ASSERT_EQ(string("test_data"), msg.data());
        progress.Clear();
        ASSERT_EQ(ERROR_NONE, reader.getReaderProgress(progress));
        if (i == messageCount - 1) {
            ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 0, 0, 128, 2, 0, topicName)) << i;
        } else {
            ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 0, 0, 128, 1, i + 1, topicName)) << i;
        }
    }
    progress.Clear();
    ASSERT_EQ(ERROR_NONE, reader.getReaderProgress(progress));
    ASSERT_NO_FATAL_FAILURE(assertSingleProgress(progress, 0, 0, 128, 2, 0, topicName));
}

TEST_F(SwiftReaderImplTest, testReportFatalError) {
    string topicName = "topic_name";
    SwiftAdminAdapterPtr adminAdapter;
    SwiftReaderConfig config;
    config.fatalErrorReportInterval = 0;
    config.fatalErrorTimeLimit = 0;
    SwiftReaderImpl reader(adminAdapter, SwiftRpcChannelManagerPtr(), _topicInfo);

    uint32_t sReaderCount = _topicInfo.partitioncount();
    reader._readers.clear();
    for (uint32_t i = 0; i < sReaderCount; ++i) {
        SwiftSinglePartitionReader *singleReader =
            new SwiftSinglePartitionReader(adminAdapter, SwiftRpcChannelManagerPtr(), i, config, -1, nullptr);
        reader._readers.push_back(singleReader);
    }
    ASSERT_EQ(sReaderCount, reader._readers.size());
    EXPECT_EQ(ERROR_NONE, reader.reportFatalError());
    { // test return
        reader._readers[1]->_lastErrorCode = ERROR_BROKER_SOME_MESSAGE_LOST;
        reader._readers[2]->_lastErrorCode = ERROR_BROKER_NO_DATA;
        reader._readers[3]->_lastErrorCode = ERROR_CLIENT_NO_MORE_MESSAGE;
        reader._readers[4]->_lastErrorCode = ERROR_SEALED_TOPIC_READ_FINISH;
        EXPECT_EQ(ERROR_NONE, reader.reportFatalError());

        reader._readers[5]->_lastErrorCode = ERROR_CLIENT_ARPC_ERROR;
        EXPECT_EQ(ERROR_CLIENT_ARPC_ERROR, reader.reportFatalError());

        reader._readers[5]->_lastErrorCode = ERROR_CLIENT_ARPC_ERROR;
        for (uint32_t i = 6; i < sReaderCount; ++i) {
            reader._readers[i]->_lastErrorCode = ERROR_SEALED_TOPIC_READ_FINISH;
        }
        EXPECT_EQ(ERROR_CLIENT_ARPC_ERROR, reader.reportFatalError());

        reader._readers[5]->_lastErrorCode = ERROR_NONE;
        EXPECT_EQ(ERROR_NONE, reader.reportFatalError());
        for (uint32_t i = 0; i < 6; ++i) {
            reader._readers[i]->_lastErrorCode = ERROR_SEALED_TOPIC_READ_FINISH;
        }
        EXPECT_EQ(ERROR_SEALED_TOPIC_READ_FINISH, reader.reportFatalError());
    }
    { // test reset error
        reader._readers[0]->_lastErrorCode = ERROR_NONE;
        reader._readers[1]->_lastErrorCode = ERROR_BROKER_SOME_MESSAGE_LOST; // not fatal
        reader._readers[2]->_lastErrorCode = ERROR_CLIENT_ARPC_ERROR;
        reader._readers[3]->_lastErrorCode = ERROR_SEALED_TOPIC_READ_FINISH;
        reader._readers[4]->_lastErrorCode = ERROR_CLIENT_ARPC_ERROR;
        EXPECT_EQ(ERROR_CLIENT_ARPC_ERROR, reader.reportFatalError());
        EXPECT_EQ(ERROR_NONE, reader._readers[0]->_lastErrorCode);
        EXPECT_EQ(ERROR_BROKER_SOME_MESSAGE_LOST, reader._readers[1]->_lastErrorCode);
        EXPECT_EQ(ERROR_NONE, reader._readers[2]->_lastErrorCode);
        EXPECT_EQ(ERROR_SEALED_TOPIC_READ_FINISH, reader._readers[3]->_lastErrorCode);
        EXPECT_EQ(ERROR_NONE, reader._readers[4]->_lastErrorCode);
    }
}

TEST_F(SwiftReaderImplTest, testSetTopicChanged) {
    SwiftAdminAdapterPtr adminAdapter;
    TopicInfo topicInfo;
    {
        SwiftReaderImpl reader(adminAdapter, SwiftRpcChannelManagerPtr(), topicInfo);
        ASSERT_FALSE(reader._isTopicChanged);
        ASSERT_EQ(-1, reader._topicVersion);
        ASSERT_EQ(ERROR_NONE, reader.setTopicChanged(12345));
        ASSERT_TRUE(reader._isTopicChanged);
        ASSERT_EQ(12345, reader._topicVersion);
        reader.resetTopicChanged();
        ASSERT_FALSE(reader._isTopicChanged);
        ASSERT_EQ(12345, reader._topicVersion);
    }
    {
        topicInfo.set_modifytime(325);
        SwiftReaderImpl reader(adminAdapter, SwiftRpcChannelManagerPtr(), topicInfo);
        ASSERT_FALSE(reader._isTopicChanged);
        ASSERT_EQ(325, reader._topicVersion);
        ASSERT_EQ(ERROR_NONE, reader.setTopicChanged(12345));
        ASSERT_TRUE(reader._isTopicChanged);
        ASSERT_EQ(12345, reader._topicVersion);
    }
}

void SwiftReaderImplTest::finishAsyncCall(vector<FakeSwiftTransportClient *> &fakeClientVec) {
    for (size_t i = 0; i < fakeClientVec.size(); i++) {
        fakeClientVec[i]->finishAsyncCall();
    }
}

} // namespace client
} // namespace swift
