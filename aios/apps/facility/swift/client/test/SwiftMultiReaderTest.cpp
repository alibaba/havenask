#include "swift/client/SwiftMultiReader.h"

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <stddef.h>
#include <string>
#include <unistd.h>
#include <vector>

#include "autil/TimeUtility.h"
#include "swift/client/MessageInfo.h"
#include "swift/client/SwiftPartitionStatus.h"
#include "swift/client/SwiftReader.h"
#include "swift/client/SwiftReaderAdapter.h"
#include "swift/client/SwiftReaderImpl.h"
#include "swift/client/SwiftSinglePartitionReader.h"
#include "swift/client/SwiftTransportClientCreator.h"
#include "swift/client/fake_client/FakeClientHelper.h"
#include "swift/client/fake_client/FakeSwiftAdminAdapter.h"
#include "swift/client/fake_client/FakeSwiftClient.h"
#include "swift/client/fake_client/FakeSwiftTransportClient.h"
#include "swift/common/SchemaWriter.h"
#include "swift/network/SwiftAdminAdapter.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/util/MessageUtil.h"
#include "unittest/unittest.h"

namespace swift {
namespace common {
class SchemaReader;
} // namespace common
} // namespace swift

using namespace std;
using namespace testing;
using namespace swift::protocol;

namespace swift {
namespace client {

const int64_t MAX_TIME_STAMP = numeric_limits<int64_t>::max();
class SwiftMultiReaderTest : public TESTBASE {
public:
    void setUp() {}
    void tearDown();

private:
    SwiftReaderPtr _reader;
    FakeSwiftTransportClient *_fakeClient1 = nullptr;
    FakeSwiftTransportClient *_fakeClient2 = nullptr;
    FakeSwiftTransportClient *_fakeClient3 = nullptr;

private:
    void assertReadOk(SwiftReader *reader, int64_t msgid, const string &data, int64_t timestampExpect = -1) {
        int64_t timestamp = -1;
        protocol::Message msg;
        ErrorCode ec = reader->read(timestamp, msg);
        cout << msg.ShortDebugString() << endl;
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ(msgid, msg.msgid());
        ASSERT_EQ(data, msg.data());
        if (-1 != timestampExpect) {
            ASSERT_EQ(timestampExpect, timestamp);
        }
    }
    void caseBaseInit();
};

void SwiftMultiReaderTest::caseBaseInit() {
    SwiftTransportClientCreator::FAKE_CLIENT_AUTO_ASYNC_CALL = true;
    FakeSwiftClient client;
    ASSERT_TRUE(ERROR_NONE == client.initByConfigStr("zkPath=zk1##zkPath=zk2##zkPath=zk3"));
    string configStr = "topicName=topic1;zkPath=zk1;batchReadCount=1;checkpointRefreshTimestampOffset=0##topicName="
                       "topic2;zkPath=zk2;batchReadCount=1;checkpointRefreshTimestampOffset=0##"
                       "topicName=topic3;zkPath=zk3;batchReadCount=1;checkpointRefreshTimestampOffset=0";
    // topic default partition 1
    ErrorInfo errorInfo;
    _reader.reset(client.createReader(configStr, &errorInfo));
    EXPECT_EQ(ERROR_NONE, errorInfo.errcode());
    EXPECT_TRUE(_reader);
    auto multiReader = dynamic_cast<SwiftMultiReader *>(_reader.get());
    ASSERT_TRUE(multiReader);
    ASSERT_EQ(3, multiReader->size());
    ASSERT_TRUE(multiReader->_readerAdapterVec[0] != NULL);
    ASSERT_TRUE(multiReader->_readerAdapterVec[1] != NULL);
    ASSERT_TRUE(multiReader->_readerAdapterVec[2] != NULL);
    int64_t timestamp = -1;
    protocol::Message msg;
    ErrorCode ec = multiReader->read(timestamp, msg, 200 * 1000);
    ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, ec);
    multiReader->_nextTimestampVec[0] = 0;
    multiReader->_nextTimestampVec[1] = 0;
    multiReader->_nextTimestampVec[2] = 0;
    multiReader->_checkpointTimestampVec[0] = -1;
    multiReader->_checkpointTimestampVec[1] = -1;
    multiReader->_checkpointTimestampVec[2] = -1;
    multiReader->_readerAdapterVec[0]->_swiftReaderImpl->_readers[0]->_nextTimestamp = -1;
    multiReader->_readerAdapterVec[1]->_swiftReaderImpl->_readers[0]->_nextTimestamp = -1;
    multiReader->_readerAdapterVec[2]->_swiftReaderImpl->_readers[0]->_nextTimestamp = -1;
    _fakeClient1 = FakeClientHelper::getTransportClient(multiReader->_readerAdapterVec[0], 0);
    _fakeClient2 = FakeClientHelper::getTransportClient(multiReader->_readerAdapterVec[1], 0);
    _fakeClient3 = FakeClientHelper::getTransportClient(multiReader->_readerAdapterVec[2], 0);
    ASSERT_TRUE(_fakeClient1 != NULL);
    ASSERT_TRUE(_fakeClient2 != NULL);
    ASSERT_TRUE(_fakeClient3 != NULL);
    _fakeClient1->setUseRealTimeStamp();
    _fakeClient2->setUseRealTimeStamp();
    _fakeClient3->setUseRealTimeStamp();
    _fakeClient1->setRealRetTimeStamp(100);
    _fakeClient2->setRealRetTimeStamp(100);
    _fakeClient3->setRealRetTimeStamp(100);
}

void SwiftMultiReaderTest::tearDown() {
    SwiftTransportClientCreator::FAKE_CLIENT_AUTO_ASYNC_CALL = false;
    SwiftTransportClientCreator::_fakeDataMap.clear();
}

TEST_F(SwiftMultiReaderTest, testMultiReaderSuccess) {
    SwiftTransportClientCreator::FAKE_CLIENT_AUTO_ASYNC_CALL = true;
    FakeSwiftClient client;
    ASSERT_TRUE(ERROR_NONE == client.initByConfigStr("zkPath=zk1##zkPath=zk2"));
    string configStr = "topicName=topic1;zkPath=zk1;checkpointRefreshTimestampOffset=0##topicName=topic2;zkPath=zk2;"
                       "checkpointRefreshTimestampOffset=0";
    // topic default partition 1
    ErrorInfo errorInfo;
    SwiftReaderPtr swiftReader(client.createReader(configStr, &errorInfo));
    EXPECT_EQ(ERROR_NONE, errorInfo.errcode());
    EXPECT_TRUE(swiftReader);
    SwiftMultiReader *reader = dynamic_cast<SwiftMultiReader *>(swiftReader.get());
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader->size());
    ASSERT_TRUE(reader->_readerAdapterVec[0] != NULL);
    ASSERT_TRUE(reader->_readerAdapterVec[1] != NULL);

    int64_t timestamp = -1;
    protocol::Message msg;
    ErrorCode ec = reader->read(timestamp, msg, 200 * 1000); // to init fake client
    ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, ec);

    auto readerAdapter1 = (SwiftReaderAdapter *)reader->_readerAdapterVec[0];
    auto readerAdapter2 = (SwiftReaderAdapter *)reader->_readerAdapterVec[1];
    FakeSwiftTransportClient *fakeClient1 = FakeClientHelper::getTransportClient(readerAdapter1, 0);
    FakeSwiftTransportClient *fakeClient2 = FakeClientHelper::getTransportClient(readerAdapter2, 0);
    readerAdapter1->_swiftReaderImpl->_readers[0]->setNextTimestamp(-1);
    readerAdapter2->_swiftReaderImpl->_readers[0]->setNextTimestamp(-1);
    ASSERT_TRUE(fakeClient1 != NULL);
    ASSERT_TRUE(fakeClient2 != NULL);
    fakeClient1->setUseRealTimeStamp();
    fakeClient2->setUseRealTimeStamp();
    fakeClient1->setRealRetTimeStamp(10);
    fakeClient2->setRealRetTimeStamp(15);

    reader->_nextTimestampVec[0] = 0;
    reader->_nextTimestampVec[1] = 0;
    reader->_checkpointTimestampVec[0] = -1;
    reader->_checkpointTimestampVec[1] = -1;
    FakeClientHelper::makeData(fakeClient1, 3, 0, 1, "topic1_data", 1);
    FakeClientHelper::makeData(fakeClient2, 4, 0, 1, "topic2_data", 2);
    ASSERT_EQ(3, fakeClient1->_messageVec->size());
    ASSERT_EQ(4, fakeClient2->_messageVec->size());
    sleep(1);

#define PRINT_MSG(msgVec)                                                                                              \
    for (auto msg : msgVec) {                                                                                          \
        cout << msg.ShortDebugString() << endl;                                                                        \
    }                                                                                                                  \
    cout << "_______" << endl;

    PRINT_MSG(*fakeClient1->_messageVec);
    PRINT_MSG(*fakeClient2->_messageVec);

    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 0, "topic1_data", 0));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 0, "topic2_data", 2));

    auto status = reader->getSwiftPartitionStatus();
    cout << "refreshTime: " << status.refreshTime << ", maxMessageId: " << status.maxMessageId
         << ", maxMessageTimestamp: " << status.maxMessageTimestamp << endl;
    ASSERT_EQ(3, status.maxMessageId);
    ASSERT_EQ(5, status.maxMessageTimestamp);
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 1, "topic1_data", 3));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 2, "topic1_data", 3));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 1, "topic2_data", 4));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 2, "topic2_data", 5));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 3, "topic2_data", 10));
    ec = reader->read(timestamp, msg);
    ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, ec);
    ASSERT_EQ(10, timestamp);

    status = reader->getSwiftPartitionStatus();
    cout << "refreshTime: " << status.refreshTime << ", maxMessageId: " << status.maxMessageId
         << ", maxMessageTimestamp: " << status.maxMessageTimestamp << endl;
    ASSERT_EQ(3, status.maxMessageId);
    ASSERT_EQ(5, status.maxMessageTimestamp);

    int64_t acceptTimestamp = 0;
    ec = reader->seekByTimestamp(1, true);
    ASSERT_EQ(ERROR_NONE, ec);
    reader->setTimestampLimit(2, acceptTimestamp);
    ASSERT_EQ(2, acceptTimestamp);
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 0, "topic1_data", 2));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 1, "topic1_data", 2));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 0, "topic2_data", 3));

    ec = reader->read(timestamp, msg);
    ASSERT_EQ(ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT, ec);
    ASSERT_EQ(3, timestamp);

    reader->setTimestampLimit(20, acceptTimestamp);
    ASSERT_EQ(ERROR_NONE, reader->seekByMessageId(2));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 2, "topic1_data", -1));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 2, "topic2_data", 5));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 3, "topic2_data", 10));
}

TEST_F(SwiftMultiReaderTest, testMultiReaderSuccessWithCheckpointReadedMode) {
    SwiftTransportClientCreator::FAKE_CLIENT_AUTO_ASYNC_CALL = true;
    FakeSwiftClient client;
    ASSERT_TRUE(ERROR_NONE == client.initByConfigStr("zkPath=zk1##zkPath=zk2"));
    string configStr =
        "topicName=topic1;zkPath=zk1;checkpointMode=readed##topicName=topic2;zkPath=zk2;checkpointMode=readed";
    // topic default partition 1
    ErrorInfo errorInfo;
    SwiftReaderPtr swiftReader(client.createReader(configStr, &errorInfo));
    EXPECT_EQ(ERROR_NONE, errorInfo.errcode());
    EXPECT_TRUE(swiftReader);
    SwiftMultiReader *reader = dynamic_cast<SwiftMultiReader *>(swiftReader.get());
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader->size());
    ASSERT_TRUE(reader->_readerAdapterVec[0] != NULL);
    ASSERT_TRUE(reader->_readerAdapterVec[1] != NULL);

    int64_t timestamp = -1;
    protocol::Message msg;
    ErrorCode ec = reader->read(timestamp, msg, 200 * 1000); // to init fake client
    ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, ec);

    auto readerAdapter1 = (SwiftReaderAdapter *)reader->_readerAdapterVec[0];
    auto readerAdapter2 = (SwiftReaderAdapter *)reader->_readerAdapterVec[1];
    FakeSwiftTransportClient *fakeClient1 = FakeClientHelper::getTransportClient(readerAdapter1, 0);
    FakeSwiftTransportClient *fakeClient2 = FakeClientHelper::getTransportClient(readerAdapter2, 0);
    readerAdapter1->_swiftReaderImpl->_readers[0]->setNextTimestamp(-1);
    readerAdapter2->_swiftReaderImpl->_readers[0]->setNextTimestamp(-1);
    ASSERT_TRUE(fakeClient1 != NULL);
    ASSERT_TRUE(fakeClient2 != NULL);
    fakeClient1->setUseRealTimeStamp();
    fakeClient2->setUseRealTimeStamp();
    fakeClient1->setRealRetTimeStamp(10);
    fakeClient2->setRealRetTimeStamp(15);

    reader->_nextTimestampVec[0] = 0;
    reader->_nextTimestampVec[1] = 0;
    reader->_checkpointTimestampVec[0] = -1;
    reader->_checkpointTimestampVec[1] = -1;
    FakeClientHelper::makeData(fakeClient1, 3, 0, 1, "topic1_data", 1);
    FakeClientHelper::makeData(fakeClient2, 4, 0, 1, "topic2_data", 2);
    ASSERT_EQ(3, fakeClient1->_messageVec->size());
    ASSERT_EQ(4, fakeClient2->_messageVec->size());
    sleep(1);

#define PRINT_MSG(msgVec)                                                                                              \
    for (auto msg : msgVec) {                                                                                          \
        cout << msg.ShortDebugString() << endl;                                                                        \
    }                                                                                                                  \
    cout << "_______" << endl;

    PRINT_MSG(*fakeClient1->_messageVec);
    PRINT_MSG(*fakeClient2->_messageVec);

    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 0, "topic1_data", 0));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 0, "topic2_data", 2));

    auto status = reader->getSwiftPartitionStatus();
    cout << "refreshTime: " << status.refreshTime << ", maxMessageId: " << status.maxMessageId
         << ", maxMessageTimestamp: " << status.maxMessageTimestamp << endl;
    ASSERT_EQ(3, status.maxMessageId);
    ASSERT_EQ(5, status.maxMessageTimestamp);
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 1, "topic1_data", 3));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 2, "topic1_data", 3));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 1, "topic2_data", 4));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 2, "topic2_data", 4));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 3, "topic2_data", 4));
    ec = reader->read(timestamp, msg);
    ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, ec);
    ASSERT_EQ(4, timestamp);

    status = reader->getSwiftPartitionStatus();
    cout << "refreshTime: " << status.refreshTime << ", maxMessageId: " << status.maxMessageId
         << ", maxMessageTimestamp: " << status.maxMessageTimestamp << endl;
    ASSERT_EQ(3, status.maxMessageId);
    ASSERT_EQ(5, status.maxMessageTimestamp);

    int64_t acceptTimestamp = 0;
    ec = reader->seekByTimestamp(1, true);
    ASSERT_EQ(ERROR_NONE, ec);
    reader->setTimestampLimit(2, acceptTimestamp);
    ASSERT_EQ(2, acceptTimestamp);
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 0, "topic1_data", 1));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 1, "topic1_data", 1));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 0, "topic2_data", 3));

    ec = reader->read(timestamp, msg);
    ASSERT_EQ(ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT, ec);
    ASSERT_EQ(3, timestamp);

    reader->setTimestampLimit(20, acceptTimestamp);
    ASSERT_EQ(ERROR_NONE, reader->seekByMessageId(2));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 2, "topic1_data", 0));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 2, "topic2_data", 4));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 3, "topic2_data", 4));
}

TEST_F(SwiftMultiReaderTest, testMultiReaderSuccessWithDefaultRefreshMode) {
    SwiftTransportClientCreator::FAKE_CLIENT_AUTO_ASYNC_CALL = true;
    FakeSwiftClient client;
    ASSERT_TRUE(ERROR_NONE == client.initByConfigStr("zkPath=zk1##zkPath=zk2"));
    string configStr = "topicName=topic1;zkPath=zk1##topicName=topic2;zkPath=zk2";
    // topic default partition 1
    ErrorInfo errorInfo;
    SwiftReaderPtr swiftReader(client.createReader(configStr, &errorInfo));
    EXPECT_EQ(ERROR_NONE, errorInfo.errcode());
    EXPECT_TRUE(swiftReader);
    SwiftMultiReader *reader = dynamic_cast<SwiftMultiReader *>(swiftReader.get());
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader->size());
    ASSERT_TRUE(reader->_readerAdapterVec[0] != NULL);
    ASSERT_TRUE(reader->_readerAdapterVec[1] != NULL);

    int64_t timestamp = -1;
    protocol::Message msg;
    ErrorCode ec = reader->read(timestamp, msg, 200 * 1000); // to init fake client
    ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, ec);

    auto readerAdapter1 = (SwiftReaderAdapter *)reader->_readerAdapterVec[0];
    auto readerAdapter2 = (SwiftReaderAdapter *)reader->_readerAdapterVec[1];
    FakeSwiftTransportClient *fakeClient1 = FakeClientHelper::getTransportClient(readerAdapter1, 0);
    FakeSwiftTransportClient *fakeClient2 = FakeClientHelper::getTransportClient(readerAdapter2, 0);
    readerAdapter1->_swiftReaderImpl->_readers[0]->setNextTimestamp(-1);
    readerAdapter2->_swiftReaderImpl->_readers[0]->setNextTimestamp(-1);
    ASSERT_TRUE(fakeClient1 != NULL);
    ASSERT_TRUE(fakeClient2 != NULL);
    fakeClient1->setUseRealTimeStamp();
    fakeClient2->setUseRealTimeStamp();
    fakeClient1->setRealRetTimeStamp(10);
    fakeClient2->setRealRetTimeStamp(15);

    reader->_nextTimestampVec[0] = 0;
    reader->_nextTimestampVec[1] = 0;
    reader->_checkpointTimestampVec[0] = -1;
    reader->_checkpointTimestampVec[1] = -1;
    FakeClientHelper::makeData(fakeClient1, 3, 0, 1, "topic1_data", 1);
    FakeClientHelper::makeData(fakeClient2, 4, 0, 1, "topic2_data", 2);
    ASSERT_EQ(3, fakeClient1->_messageVec->size());
    ASSERT_EQ(4, fakeClient2->_messageVec->size());
    sleep(1);

#define PRINT_MSG(msgVec)                                                                                              \
    for (auto msg : msgVec) {                                                                                          \
        cout << msg.ShortDebugString() << endl;                                                                        \
    }                                                                                                                  \
    cout << "_______" << endl;

    PRINT_MSG(*fakeClient1->_messageVec);
    PRINT_MSG(*fakeClient2->_messageVec);

    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 0, "topic1_data", 0));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 0, "topic2_data", 2));

    auto status = reader->getSwiftPartitionStatus();
    cout << "refreshTime: " << status.refreshTime << ", maxMessageId: " << status.maxMessageId
         << ", maxMessageTimestamp: " << status.maxMessageTimestamp << endl;
    ASSERT_EQ(3, status.maxMessageId);
    ASSERT_EQ(5, status.maxMessageTimestamp);
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 1, "topic1_data", 3));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 2, "topic1_data", 3));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 1, "topic2_data", 4));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 2, "topic2_data", 4));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 3, "topic2_data", 4));
    ec = reader->read(timestamp, msg);
    ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, ec);
    ASSERT_EQ(4, timestamp);

    status = reader->getSwiftPartitionStatus();
    cout << "refreshTime: " << status.refreshTime << ", maxMessageId: " << status.maxMessageId
         << ", maxMessageTimestamp: " << status.maxMessageTimestamp << endl;
    ASSERT_EQ(3, status.maxMessageId);
    ASSERT_EQ(5, status.maxMessageTimestamp);

    int64_t acceptTimestamp = 0;
    ec = reader->seekByTimestamp(1, true);
    ASSERT_EQ(ERROR_NONE, ec);
    reader->setTimestampLimit(2, acceptTimestamp);
    ASSERT_EQ(2, acceptTimestamp);
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 0, "topic1_data", 1));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 1, "topic1_data", 1));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 0, "topic2_data", 3));

    ec = reader->read(timestamp, msg);
    ASSERT_EQ(ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT, ec);
    ASSERT_EQ(3, timestamp);

    reader->setTimestampLimit(20, acceptTimestamp);
    ASSERT_EQ(ERROR_NONE, reader->seekByMessageId(2));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 2, "topic1_data", 0));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 2, "topic2_data", 4));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 3, "topic2_data", 4));
}

TEST_F(SwiftMultiReaderTest, testMultiReaderSuccessWithRefrestTimestampOffset_2) {
    SwiftTransportClientCreator::FAKE_CLIENT_AUTO_ASYNC_CALL = true;
    FakeSwiftClient client;
    ASSERT_TRUE(ERROR_NONE == client.initByConfigStr("zkPath=zk1##zkPath=zk2"));
    string configStr = "topicName=topic1;zkPath=zk1;checkpointRefreshTimestampOffset=2##topicName=topic2;zkPath=zk2;"
                       "checkpointRefreshTimestampOffset=2";
    // topic default partition 1
    ErrorInfo errorInfo;
    SwiftReaderPtr swiftReader(client.createReader(configStr, &errorInfo));
    EXPECT_EQ(ERROR_NONE, errorInfo.errcode());
    EXPECT_TRUE(swiftReader);
    SwiftMultiReader *reader = dynamic_cast<SwiftMultiReader *>(swiftReader.get());
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader->size());
    ASSERT_TRUE(reader->_readerAdapterVec[0] != NULL);
    ASSERT_TRUE(reader->_readerAdapterVec[1] != NULL);

    int64_t timestamp = -1;
    protocol::Message msg;
    ErrorCode ec = reader->read(timestamp, msg, 200 * 1000); // to init fake client
    ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, ec);

    auto readerAdapter1 = (SwiftReaderAdapter *)reader->_readerAdapterVec[0];
    auto readerAdapter2 = (SwiftReaderAdapter *)reader->_readerAdapterVec[1];
    FakeSwiftTransportClient *fakeClient1 = FakeClientHelper::getTransportClient(readerAdapter1, 0);
    FakeSwiftTransportClient *fakeClient2 = FakeClientHelper::getTransportClient(readerAdapter2, 0);
    readerAdapter1->_swiftReaderImpl->_readers[0]->setNextTimestamp(-1);
    readerAdapter2->_swiftReaderImpl->_readers[0]->setNextTimestamp(-1);
    ASSERT_TRUE(fakeClient1 != NULL);
    ASSERT_TRUE(fakeClient2 != NULL);
    fakeClient1->setUseRealTimeStamp();
    fakeClient2->setUseRealTimeStamp();
    fakeClient1->setRealRetTimeStamp(10);
    fakeClient2->setRealRetTimeStamp(15);

    reader->_nextTimestampVec[0] = 0;
    reader->_nextTimestampVec[1] = 0;
    reader->_checkpointTimestampVec[0] = -1;
    reader->_checkpointTimestampVec[1] = -1;
    FakeClientHelper::makeData(fakeClient1, 3, 0, 1, "topic1_data", 1);
    FakeClientHelper::makeData(fakeClient2, 4, 0, 1, "topic2_data", 2);
    ASSERT_EQ(3, fakeClient1->_messageVec->size());
    ASSERT_EQ(4, fakeClient2->_messageVec->size());
    sleep(1);

#define PRINT_MSG(msgVec)                                                                                              \
    for (auto msg : msgVec) {                                                                                          \
        cout << msg.ShortDebugString() << endl;                                                                        \
    }                                                                                                                  \
    cout << "_______" << endl;

    PRINT_MSG(*fakeClient1->_messageVec);
    PRINT_MSG(*fakeClient2->_messageVec);

    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 0, "topic1_data", 0));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 0, "topic2_data", 2));

    auto status = reader->getSwiftPartitionStatus();
    cout << "refreshTime: " << status.refreshTime << ", maxMessageId: " << status.maxMessageId
         << ", maxMessageTimestamp: " << status.maxMessageTimestamp << endl;
    ASSERT_EQ(3, status.maxMessageId);
    ASSERT_EQ(5, status.maxMessageTimestamp);
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 1, "topic1_data", 3));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 2, "topic1_data", 3));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 1, "topic2_data", 4));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 2, "topic2_data", 5));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 3, "topic2_data", 8));
    ec = reader->read(timestamp, msg);
    ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, ec);
    ASSERT_EQ(8, timestamp);

    status = reader->getSwiftPartitionStatus();
    cout << "refreshTime: " << status.refreshTime << ", maxMessageId: " << status.maxMessageId
         << ", maxMessageTimestamp: " << status.maxMessageTimestamp << endl;
    ASSERT_EQ(3, status.maxMessageId);
    ASSERT_EQ(5, status.maxMessageTimestamp);

    int64_t acceptTimestamp = 0;
    ec = reader->seekByTimestamp(1, true);
    ASSERT_EQ(ERROR_NONE, ec);
    reader->setTimestampLimit(2, acceptTimestamp);
    ASSERT_EQ(2, acceptTimestamp);
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 0, "topic1_data", 1));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 1, "topic1_data", 1));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 0, "topic2_data", 3));

    ec = reader->read(timestamp, msg);
    ASSERT_EQ(ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT, ec);
    ASSERT_EQ(3, timestamp);

    reader->setTimestampLimit(20, acceptTimestamp);
    ASSERT_EQ(ERROR_NONE, reader->seekByMessageId(2));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 2, "topic1_data", 0));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 2, "topic2_data", 5));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 3, "topic2_data", 8));
}

TEST_F(SwiftMultiReaderTest, testMultiReaderBatchSuccess) {
    SwiftTransportClientCreator::FAKE_CLIENT_AUTO_ASYNC_CALL = true;
    FakeSwiftClient client;
    ASSERT_TRUE(ERROR_NONE == client.initByConfigStr("zkPath=zk1##zkPath=zk2"));
    string configStr = "topicName=topic1;zkPath=zk1;oneRequestReadCount=2;checkpointRefreshTimestampOffset=0##"
                       "topicName=topic2;zkPath=zk2;oneRequestReadCount=3;checkpointRefreshTimestampOffset=0";
    // topic default partition 1
    ErrorInfo errorInfo;
    SwiftReaderPtr swiftReader(client.createReader(configStr, &errorInfo));
    EXPECT_EQ(ERROR_NONE, errorInfo.errcode());
    EXPECT_TRUE(swiftReader);
    SwiftMultiReader *reader = dynamic_cast<SwiftMultiReader *>(swiftReader.get());
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader->size());
    ASSERT_TRUE(reader->_readerAdapterVec[0] != NULL);
    ASSERT_TRUE(reader->_readerAdapterVec[1] != NULL);
    int64_t timestamp = -1;
    protocol::Messages msgs;
    ErrorCode ec = reader->read(timestamp, msgs, 200 * 1000);
    ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, ec);

    auto readerAdapter1 = (SwiftReaderAdapter *)reader->_readerAdapterVec[0];
    auto readerAdapter2 = (SwiftReaderAdapter *)reader->_readerAdapterVec[1];
    FakeSwiftTransportClient *fakeClient1 = FakeClientHelper::getTransportClient(readerAdapter1, 0);
    FakeSwiftTransportClient *fakeClient2 = FakeClientHelper::getTransportClient(readerAdapter2, 0);
    readerAdapter1->_swiftReaderImpl->_readers[0]->setNextTimestamp(-1);
    readerAdapter2->_swiftReaderImpl->_readers[0]->setNextTimestamp(-1);

    fakeClient1->setUseRealTimeStamp();
    fakeClient2->setUseRealTimeStamp();
    fakeClient1->setRealRetTimeStamp(10);
    fakeClient2->setRealRetTimeStamp(15);

    reader->_nextTimestampVec[0] = 0;
    reader->_nextTimestampVec[1] = 0;
    reader->_checkpointTimestampVec[0] = -1;
    reader->_checkpointTimestampVec[1] = -1;
    FakeClientHelper::makeData(fakeClient1, 3, 0, 1, "topic1_data", 1);
    FakeClientHelper::makeData(fakeClient2, 4, 0, 1, "topic2_data", 2);
    ASSERT_EQ(3, fakeClient1->_messageVec->size());
    ASSERT_EQ(4, fakeClient2->_messageVec->size());
    sleep(1);

#define PRINT_MSG(msgVec)                                                                                              \
    for (auto msg : msgVec) {                                                                                          \
        cout << msg.ShortDebugString() << endl;                                                                        \
    }                                                                                                                  \
    cout << "_______" << endl;

    PRINT_MSG(*fakeClient1->_messageVec);
    PRINT_MSG(*fakeClient2->_messageVec);

#define PRINT_MSGS                                                                                                     \
    for (int idx = 0; idx < msgs.msgs_size(); ++idx) {                                                                 \
        cout << "Error: " << ec << ": " << msgs.msgs(idx).ShortDebugString() << endl;                                  \
    }

    ec = reader->read(timestamp, msgs);
    PRINT_MSGS;
    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_EQ(2, msgs.msgs_size());
    ASSERT_EQ(timestamp, 0);

    ec = reader->read(timestamp, msgs);
    PRINT_MSGS;
    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_EQ(3, msgs.msgs_size());
    ASSERT_EQ(timestamp, 3);

    ec = reader->read(timestamp, msgs);
    PRINT_MSGS;
    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_EQ(1, msgs.msgs_size());
    ASSERT_EQ(timestamp, 5);

    ec = reader->read(timestamp, msgs);
    PRINT_MSGS;
    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_EQ(1, msgs.msgs_size());
    ASSERT_EQ(timestamp, 10);
}

TEST_F(SwiftMultiReaderTest, testMultiReaderOneFail) {
    SwiftTransportClientCreator::FAKE_CLIENT_AUTO_ASYNC_CALL = true;
    FakeSwiftClient client;
    ASSERT_TRUE(ERROR_NONE == client.initByConfigStr("zkPath=zk1##zkPath=zk2"));
    string configStr = "topicName=topic1;zkPath=zk1;checkpointRefreshTimestampOffset=0##topicName=topic2;zkPath=zk2;"
                       "checkpointRefreshTimestampOffset=0";
    // topic default partition 1
    ErrorInfo errorInfo;
    SwiftReaderPtr swiftReader(client.createReader(configStr, &errorInfo));
    EXPECT_EQ(ERROR_NONE, errorInfo.errcode());
    EXPECT_TRUE(swiftReader);
    SwiftMultiReader *reader = dynamic_cast<SwiftMultiReader *>(swiftReader.get());
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader->size());
    ASSERT_TRUE(reader->_readerAdapterVec[0] != NULL);
    ASSERT_TRUE(reader->_readerAdapterVec[1] != NULL);
    int64_t timestamp = -1;
    protocol::Message msg;
    ErrorCode ec = reader->read(timestamp, msg, 200 * 1000);
    ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, ec);
    auto readerAdapter1 = (SwiftReaderAdapter *)reader->_readerAdapterVec[0];
    auto readerAdapter2 = (SwiftReaderAdapter *)reader->_readerAdapterVec[1];
    FakeSwiftTransportClient *fakeClient1 = FakeClientHelper::getTransportClient(readerAdapter1, 0);
    FakeSwiftTransportClient *fakeClient2 = FakeClientHelper::getTransportClient(readerAdapter2, 0);
    readerAdapter1->_swiftReaderImpl->_readers[0]->setNextTimestamp(-1);
    readerAdapter2->_swiftReaderImpl->_readers[0]->setNextTimestamp(-1);

    ASSERT_TRUE(fakeClient1 != NULL);
    ASSERT_TRUE(fakeClient2 != NULL);
    fakeClient1->setUseRealTimeStamp();
    fakeClient2->setUseRealTimeStamp();
    fakeClient1->setRealRetTimeStamp(10);
    fakeClient2->setRealRetTimeStamp(15);
    reader->_nextTimestampVec[0] = -2;
    reader->_nextTimestampVec[1] = -2;

    FakeClientHelper::makeData(fakeClient2, 4, 0, 1, "topic2_data", 1);
    ASSERT_EQ(0, fakeClient1->_messageVec->size());
    ASSERT_EQ(4, fakeClient2->_messageVec->size());
    sleep(1);

    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 0, "topic2_data", 2));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 1, "topic2_data", 3));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 2, "topic2_data", 4));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 3, "topic2_data", 10));
    ec = reader->read(timestamp, msg);
    ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, ec);
    ASSERT_EQ(timestamp, 10);

    { // test RO_SEQ
        reader->setReaderOrder("sequence");
        ASSERT_EQ(ERROR_NONE, reader->seekByTimestamp(0, true));
        ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 0, "topic2_data", 0));
        ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 1, "topic2_data", 0));
        ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 2, "topic2_data", 0));
        ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 3, "topic2_data", 0));
        timestamp = 0;
        for (int retry = 0; retry < 10; ++retry) {
            cout << "time: " << autil::TimeUtility::currentTimeInSeconds() << endl;
            ec = reader->read(timestamp, msg);
            ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, ec);
            if (10 == timestamp) {
                break;
            }
        }
        ASSERT_EQ(timestamp, 10);
    }
}

TEST_F(SwiftMultiReaderTest, testMultiReaderBothFail) {
    SwiftTransportClientCreator::FAKE_CLIENT_AUTO_ASYNC_CALL = true;
    FakeSwiftClient client;
    ASSERT_TRUE(ERROR_NONE == client.initByConfigStr("zkPath=zk1##zkPath=zk2"));
    string configStr = "topicName=topic1;zkPath=zk1##topicName=topic2;zkPath=zk2";
    // topic default partition 1
    ErrorInfo errorInfo;
    SwiftReaderPtr swiftReader(client.createReader(configStr, &errorInfo));
    EXPECT_EQ(ERROR_NONE, errorInfo.errcode());
    EXPECT_TRUE(swiftReader);
    SwiftMultiReader *reader = dynamic_cast<SwiftMultiReader *>(swiftReader.get());
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader->size());
    ASSERT_TRUE(reader->_readerAdapterVec[0] != NULL);
    ASSERT_TRUE(reader->_readerAdapterVec[1] != NULL);
    int64_t timestamp = -1;
    protocol::Message msg;
    ErrorCode ec = reader->read(timestamp, msg, 200 * 1000);
    ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, ec);

    string errorMsg;
    reader->checkCurrentError(ec, errorMsg);
    ASSERT_EQ(ERROR_NONE, ec);
}

TEST_F(SwiftMultiReaderTest, testSetTimestampLimit) {
    caseBaseInit();
    int64_t acceptTimestamp = -1;
    auto reader = dynamic_cast<SwiftMultiReader *>(_reader.get());
    reader->setTimestampLimit(MAX_TIME_STAMP, acceptTimestamp);
    ASSERT_EQ(MAX_TIME_STAMP, acceptTimestamp);
    ASSERT_EQ(MAX_TIME_STAMP, reader->_timestampLimit);
    reader->setTimestampLimit(0, acceptTimestamp);
    ASSERT_EQ(0, acceptTimestamp);
    ASSERT_EQ(0, reader->_timestampLimit);
    reader->setTimestampLimit(-1, acceptTimestamp);
    ASSERT_EQ(-1, acceptTimestamp);
    ASSERT_EQ(-1, reader->_timestampLimit);
    reader->setTimestampLimit(10, acceptTimestamp);
    ASSERT_EQ(10, acceptTimestamp);
    ASSERT_EQ(10, reader->_timestampLimit);

    protocol::Message msg;
    protocol::Messages msgs;
    FakeClientHelper::makeData(_fakeClient1, 3, 0, 1, "topic1_data", 1);
    FakeClientHelper::makeData(_fakeClient2, 4, 0, 1, "topic2_data", 2);
    FakeClientHelper::makeData(_fakeClient3, 5, 0, 1, "topic3_data", 3);
    sleep(1);

#define PRINT_MSG(msgVec)                                                                                              \
    for (auto msg : msgVec) {                                                                                          \
        cout << msg.ShortDebugString() << endl;                                                                        \
    }                                                                                                                  \
    cout << "_______" << endl;

    PRINT_MSG(*_fakeClient1->_messageVec);
    PRINT_MSG(*_fakeClient2->_messageVec);
    PRINT_MSG(*_fakeClient3->_messageVec);
#undef PRINT_MSG

#define PRINT_MSGS                                                                                                     \
    for (int idx = 0; idx < msgs.msgs_size(); ++idx) {                                                                 \
        cout << "Error: " << ec << ": " << msgs.msgs(idx).ShortDebugString() << endl;                                  \
    }

    ErrorCode ec = ERROR_NONE;
    int64_t timestamp = -1;
    reader->setTimestampLimit(-1, acceptTimestamp);
    ASSERT_EQ(-1, acceptTimestamp);
    reader->setTimestampLimit(MAX_TIME_STAMP, acceptTimestamp);
    ASSERT_EQ(MAX_TIME_STAMP, acceptTimestamp);
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 0, "topic1_data", 0));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 0, "topic2_data", 0));
    reader->setTimestampLimit(1, acceptTimestamp);
    ASSERT_EQ(2, acceptTimestamp);
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 1, "topic1_data", 3));

    msgs.clear_msgs();
    ec = reader->read(timestamp, msgs);
    ASSERT_EQ(ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT, ec);
    ASSERT_EQ(0, msgs.msgs_size());

    reader->setTimestampLimit(4, acceptTimestamp);
    ASSERT_EQ(4, acceptTimestamp);
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 2, "topic1_data", 3));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 1, "topic2_data", 3));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 0, "topic3_data", 4));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 2, "topic2_data", 4));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 1, "topic3_data", 5));
    reader->read(timestamp, msg);
    ASSERT_EQ(ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT, ec);

    reader->setTimestampLimit(MAX_TIME_STAMP, acceptTimestamp);
    ASSERT_EQ(MAX_TIME_STAMP, acceptTimestamp);
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 3, "topic2_data", 5));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 2, "topic3_data", 6));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 3, "topic3_data", 7));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 4, "topic3_data", 100));
    ec = reader->read(timestamp, msgs);
    ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, ec);

    _fakeClient1->clearMsg();
    _fakeClient2->clearMsg();
    _fakeClient3->clearMsg();
}

TEST_F(SwiftMultiReaderTest, testSetTimestampLimit_f2_not_all_exceed_timelimit) {
    caseBaseInit();
    int64_t acceptTimestamp = -1;
    auto reader = dynamic_cast<SwiftMultiReader *>(_reader.get());
    protocol::Message msg;
    protocol::Messages msgs;
    FakeClientHelper::makeData(_fakeClient1, 3, 0, 4, "topic1_data", 1);
    FakeClientHelper::makeData(_fakeClient2, 4, 0, 4, "topic2_data", 2);
    FakeClientHelper::makeData(_fakeClient3, 5, 0, 4, "topic3_data", 3);
    sleep(1);

#define PRINT_MSG(msgVec)                                                                                              \
    for (auto msg : msgVec) {                                                                                          \
        cout << msg.ShortDebugString() << endl;                                                                        \
    }                                                                                                                  \
    cout << "_______" << endl;

    PRINT_MSG(*_fakeClient1->_messageVec);
    PRINT_MSG(*_fakeClient2->_messageVec);
    PRINT_MSG(*_fakeClient3->_messageVec);
#undef PRINT_MSG

#define PRINT_MSGS                                                                                                     \
    for (int idx = 0; idx < msgs.msgs_size(); ++idx) {                                                                 \
        cout << "Error: " << ec << ": " << msgs.msgs(idx).ShortDebugString() << endl;                                  \
    }

    ErrorCode ec = ERROR_NONE;
    int64_t timestamp = -1;
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 0, "topic1_data", 0));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 0, "topic2_data", 0));
    reader->setTimestampLimit(1, acceptTimestamp);
    ASSERT_EQ(2, acceptTimestamp);
    msgs.clear_msgs();
    ec = reader->read(timestamp, msgs);
    ASSERT_EQ(ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT, ec);
    ASSERT_EQ(0, msgs.msgs_size());

    reader->setTimestampLimit(4, acceptTimestamp);
    ASSERT_EQ(4, acceptTimestamp);
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 0, "topic3_data", 5));
    reader->read(timestamp, msg);
    ASSERT_EQ(ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT, ec);

    reader->setTimestampLimit(11, acceptTimestamp);
    ASSERT_EQ(11, acceptTimestamp);
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 1, "topic1_data", 6));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 1, "topic2_data", 7));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 1, "topic3_data", 9));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 2, "topic1_data", 10));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 2, "topic2_data", 11));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 2, "topic3_data", 14));
    reader->read(timestamp, msg);
    ASSERT_EQ(ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT, ec);

    reader->setTimestampLimit(MAX_TIME_STAMP, acceptTimestamp);
    ASSERT_EQ(MAX_TIME_STAMP, acceptTimestamp);
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 3, "topic2_data", 15));

    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 3, "topic3_data", 19));
    ASSERT_NO_FATAL_FAILURE(assertReadOk(reader, 4, "topic3_data", 100));
    ec = reader->read(timestamp, msgs);
    ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, ec);

    _fakeClient1->clearMsg();
    _fakeClient2->clearMsg();
    _fakeClient3->clearMsg();
}

TEST_F(SwiftMultiReaderTest, testCheckCurrentError) {
    caseBaseInit();
    ErrorCode ec = ERROR_NONE;
    string errorMsg;
    auto multiReader = dynamic_cast<SwiftMultiReader *>(_reader.get());
    multiReader->checkCurrentError(ec, errorMsg);
    ASSERT_EQ(ERROR_NONE, ec);

#define SET_SINGLE_READER_ERRORCODE(index, errorcode)                                                                  \
    {                                                                                                                  \
        auto adapter = dynamic_cast<SwiftReaderAdapter *>(multiReader->_readerAdapterVec[index]);                      \
        auto singleReader = dynamic_cast<SwiftSinglePartitionReader *>(adapter->_swiftReaderImpl->_readers[0]);        \
        singleReader->_errorCodeForCheck = errorcode;                                                                  \
        singleReader->_lastSuccessResponseTime = 0;                                                                    \
    }
    SET_SINGLE_READER_ERRORCODE(0, ERROR_CLIENT_NO_MORE_MESSAGE);
    SET_SINGLE_READER_ERRORCODE(1, ERROR_CLIENT_READ_MESSAGE_TIMEOUT);
    SET_SINGLE_READER_ERRORCODE(2, ERROR_CLIENT_ARPC_ERROR);
    multiReader->checkCurrentError(ec, errorMsg);
    cout << ec << " " << errorMsg << endl;
    ASSERT_EQ(ERROR_CLIENT_ARPC_ERROR, ec);
    ASSERT_TRUE(errorMsg.find("topic1") == string::npos);
    ASSERT_TRUE(errorMsg.find("topic2") != string::npos);
    ASSERT_TRUE(errorMsg.find("topic3") != string::npos);
#undef SET_SINGLE_READER_ERRORCODE
}

TEST_F(SwiftMultiReaderTest, testGetSwiftPatitionStatus) {
    caseBaseInit();
    auto multiReader = dynamic_cast<SwiftMultiReader *>(_reader.get());
    auto status = multiReader->getSwiftPartitionStatus();
    // ASSERT_EQ(currentTime, status.refreshTime);
    ASSERT_EQ(-1, status.maxMessageId);
    ASSERT_EQ(-1, status.maxMessageTimestamp);
}

TEST_F(SwiftMultiReaderTest, testSetGetRequiredFieldNames) {
    caseBaseInit();
    vector<string> fieldNames;
    fieldNames.push_back("f1");
    fieldNames.push_back("f2");
    fieldNames.push_back("f3");
    auto multiReader = dynamic_cast<SwiftMultiReader *>(_reader.get());
    multiReader->setRequiredFieldNames(fieldNames);
    auto retFieldNames = multiReader->getRequiredFieldNames();
    ASSERT_EQ(fieldNames, retFieldNames);
    for (auto reader : multiReader->_readerAdapterVec) {
        auto ret = reader->getRequiredFieldNames();
        ASSERT_EQ(fieldNames, ret);
    }
}

TEST_F(SwiftMultiReaderTest, testSetFieldFilterDesc) {
    caseBaseInit();
#define ASSERT_FILTER_DESC_EQ(index, expect)                                                                           \
    {                                                                                                                  \
        auto adapter = dynamic_cast<SwiftReaderAdapter *>(multiReader->_readerAdapterVec[index]);                      \
        auto singleReader = dynamic_cast<SwiftSinglePartitionReader *>(adapter->_swiftReaderImpl->_readers[0]);        \
        ASSERT_EQ(expect, singleReader->getFieldFilterDesc());                                                         \
    }
    auto multiReader = dynamic_cast<SwiftMultiReader *>(_reader.get());
    ASSERT_FILTER_DESC_EQ(0, "");
    ASSERT_FILTER_DESC_EQ(1, "");
    ASSERT_FILTER_DESC_EQ(2, "");
    string filterDesc("A IN f1");
    multiReader->setFieldFilterDesc(filterDesc);
    ASSERT_FILTER_DESC_EQ(0, filterDesc);
    ASSERT_FILTER_DESC_EQ(1, filterDesc);
    ASSERT_FILTER_DESC_EQ(2, filterDesc);
#undef ASSERT_FILTER_DESC_EQ
}

TEST_F(SwiftMultiReaderTest, testUpdateCommittedCheckpoint) {
    caseBaseInit();
#define ASSERT_CHECKPOINT_EQ(index, expect)                                                                            \
    {                                                                                                                  \
        auto adapter = dynamic_cast<SwiftReaderAdapter *>(multiReader->_readerAdapterVec[index]);                      \
        auto singleReader = dynamic_cast<SwiftSinglePartitionReader *>(adapter->_swiftReaderImpl->_readers[0]);        \
        ASSERT_EQ(expect, singleReader->_clientCommittedCheckpoint);                                                   \
    }
    auto multiReader = dynamic_cast<SwiftMultiReader *>(_reader.get());
    for (size_t index = 0; index < 3; ++index) {
        auto adapter = dynamic_cast<SwiftReaderAdapter *>(multiReader->_readerAdapterVec[index]);
        auto singleReader = dynamic_cast<SwiftSinglePartitionReader *>(adapter->_swiftReaderImpl->_readers[0]);
        singleReader->_clientCommittedCheckpoint = 0;
    }
    multiReader->updateCommittedCheckpoint(100);
    ASSERT_CHECKPOINT_EQ(0, 100);
    ASSERT_CHECKPOINT_EQ(1, 100);
    ASSERT_CHECKPOINT_EQ(2, 100);
#undef ASSERT_CHECKPOINT_EQ
}

TEST_F(SwiftMultiReaderTest, testGetMinCheckpointTimeStamp) {
    SwiftMultiReader reader;
    ASSERT_EQ(SwiftMultiReader::RO_DEFAULT, reader._readerOrder);
    {
        int64_t timeStamps[5] = {5, 3, 1, -1, 0};
        reader._checkpointTimestampVec.clear();
        reader._checkpointTimestampVec.insert(reader._checkpointTimestampVec.begin(), timeStamps, timeStamps + 5);
        ASSERT_EQ(0, reader.getCheckpointTimestamp());
    }
    {
        int64_t timeStamps[5] = {0, 3, 1, -1, 5};
        reader._checkpointTimestampVec.clear();
        reader._checkpointTimestampVec.insert(reader._checkpointTimestampVec.begin(), timeStamps, timeStamps + 5);
        ASSERT_EQ(0, reader.getCheckpointTimestamp());
    }
    {
        int64_t timeStamps[5] = {5, 3, 0, 0, -1};
        reader._checkpointTimestampVec.clear();
        reader._checkpointTimestampVec.insert(reader._checkpointTimestampVec.begin(), timeStamps, timeStamps + 5);
        ASSERT_EQ(0, reader.getCheckpointTimestamp());
    }
}

TEST_F(SwiftMultiReaderTest, testGetReaderPos) {
    {
        SwiftMultiReader reader;
        int64_t timeStamps[5] = {5, 3, 1, -1, 0};
        int64_t exceeds[5] = {false, false, false, false, false};
        reader._nextTimestampVec.insert(reader._nextTimestampVec.begin(), timeStamps, timeStamps + 5);
        reader._exceedTimestampLimitVec.insert(reader._exceedTimestampLimitVec.begin(), exceeds, exceeds + 5);
        reader.getReaderPos();
        ASSERT_EQ(3, reader._readerPos);
    }
    {
        SwiftMultiReader reader;
        int64_t timeStamps[5] = {5, 3, 1, -1, 0};
        int64_t exceeds[5] = {true, true, true, true, true};
        reader._nextTimestampVec.insert(reader._nextTimestampVec.begin(), timeStamps, timeStamps + 5);
        reader._exceedTimestampLimitVec.insert(reader._exceedTimestampLimitVec.begin(), exceeds, exceeds + 5);
        reader.getReaderPos();
        reader._readerPos = 0;
        ASSERT_EQ(0, reader._readerPos); // initial value
    }
    {
        SwiftMultiReader reader;
        int64_t timeStamps[5] = {5, 3, 1, -1, 0};
        int64_t exceeds[5] = {false, false, false, true, true};
        reader._nextTimestampVec.insert(reader._nextTimestampVec.begin(), timeStamps, timeStamps + 5);
        reader._exceedTimestampLimitVec.insert(reader._exceedTimestampLimitVec.begin(), exceeds, exceeds + 5);
        reader.getReaderPos();
        ASSERT_EQ(2, reader._readerPos);
    }
    { // RO_DEFAULT not affect by no_more_msg
        SwiftMultiReader reader;
        int64_t timeStamps[5] = {5, 3, 1, -1, 0};
        int64_t exceeds[5] = {false, false, false, true, true};
        reader._nextTimestampVec.insert(reader._nextTimestampVec.begin(), timeStamps, timeStamps + 5);
        reader._exceedTimestampLimitVec.insert(reader._exceedTimestampLimitVec.begin(), exceeds, exceeds + 5);
        reader.getReaderPos();
        ASSERT_EQ(2, reader._readerPos);
    }
    // RO_SEQ, set reader order
    {
        SwiftMultiReader reader;
        int64_t timeStamps[5] = {5, 3, -1, 1, 0};
        int64_t exceeds[5] = {false, false, false, false, false};
        reader._nextTimestampVec.insert(reader._nextTimestampVec.begin(), timeStamps, timeStamps + 5);
        reader._exceedTimestampLimitVec.insert(reader._exceedTimestampLimitVec.begin(), exceeds, exceeds + 5);
        reader.getReaderPos();
        ASSERT_EQ(2, reader._readerPos);
        ASSERT_TRUE(reader.setReaderOrder("sequence"));
        for (int i = 0; i < 5; ++i) {
            reader.getReaderPos();
            ASSERT_EQ((3 + i) % 5, reader._readerPos);
        }
        // now pos is 2
        reader._exceedTimestampLimitVec[0] = true;
        reader._exceedTimestampLimitVec[3] = true;
        reader.getReaderPos();
        ASSERT_EQ(4, reader._readerPos);
        reader.getReaderPos();
        ASSERT_EQ(1, reader._readerPos);
        reader.getReaderPos();
        ASSERT_EQ(2, reader._readerPos);
        ASSERT_FALSE(reader.setReaderOrder("illege"));
        reader.getReaderPos();
        ASSERT_EQ(4, reader._readerPos);
        ASSERT_TRUE(reader.setReaderOrder("default"));
        reader.getReaderPos();
        ASSERT_EQ(2, reader._readerPos);
        ASSERT_FALSE(reader.setReaderOrder(""));
        reader.getReaderPos();
        ASSERT_EQ(2, reader._readerPos);
    }
}

TEST_F(SwiftMultiReaderTest, testCheckTimestampLimit) {
    SwiftMultiReader reader;
    {
        bool exceedTimestampLimits[5] = {true, true, true, true, true};
        reader._exceedTimestampLimitVec.clear();
        reader._exceedTimestampLimitVec.insert(
            reader._exceedTimestampLimitVec.begin(), exceedTimestampLimits, exceedTimestampLimits + 5);
        ASSERT_EQ(ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT,
                  reader.checkTimestampLimit(ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT));
        ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, reader.checkTimestampLimit(ERROR_CLIENT_NO_MORE_MESSAGE));
        ASSERT_EQ(ERROR_CLIENT_READ_MESSAGE_TIMEOUT, reader.checkTimestampLimit(ERROR_CLIENT_READ_MESSAGE_TIMEOUT));
        ASSERT_EQ(ERROR_CLIENT_ARPC_ERROR, reader.checkTimestampLimit(ERROR_CLIENT_ARPC_ERROR));
    }
    {
        bool exceedTimestampLimits[5] = {false, true, true, true, true};
        reader._exceedTimestampLimitVec.clear();
        reader._exceedTimestampLimitVec.insert(
            reader._exceedTimestampLimitVec.begin(), exceedTimestampLimits, exceedTimestampLimits + 5);
        ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, reader.checkTimestampLimit(ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT));
        ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, reader.checkTimestampLimit(ERROR_CLIENT_NO_MORE_MESSAGE));
        ASSERT_EQ(ERROR_CLIENT_READ_MESSAGE_TIMEOUT, reader.checkTimestampLimit(ERROR_CLIENT_READ_MESSAGE_TIMEOUT));
        ASSERT_EQ(ERROR_CLIENT_ARPC_ERROR, reader.checkTimestampLimit(ERROR_CLIENT_ARPC_ERROR));
    }
}

TEST_F(SwiftMultiReaderTest, testGetSchemaReader) {
    SwiftTransportClientCreator::FAKE_CLIENT_AUTO_ASYNC_CALL = true;
    FakeSwiftClient client;
    ASSERT_TRUE(ERROR_NONE == client.initByConfigStr("zkPath=fake_zk"));

    { // 1. normal topic not support
        SwiftMultiReader mreader;
        // topic default partition 1
        ErrorInfo errorInfo;
        auto *reader1 = client.createReader("topicName=topic1", &errorInfo);
        auto *reader2 = client.createReader("topicName=topic2", &errorInfo);
        EXPECT_EQ(ERROR_NONE, errorInfo.errcode());
        mreader.addReader(reader1);
        mreader.addReader(reader2);
        ASSERT_EQ(2, mreader.size());

        ErrorCode ec = ERROR_NONE;
        string data("test_data");
        EXPECT_EQ(0, mreader._readerPos);
        common::SchemaReader *sr = mreader.getSchemaReader(data.c_str(), ec);
        EXPECT_EQ(ERROR_ADMIN_TOPIC_NOT_SUPPORT_SCHEMA, ec);
        EXPECT_TRUE(sr == nullptr);
        mreader._readerPos = 1;
        sr = mreader.getSchemaReader(data.c_str(), ec);
        EXPECT_EQ(ERROR_ADMIN_TOPIC_NOT_SUPPORT_SCHEMA, ec);
        EXPECT_TRUE(sr == nullptr);
    }

    { // 2. schema topic
        string schema1(
            R"json({"topic":"topic1","fields":[{"name":"CMD","type":"string"},{"name":"nid","type":"string"}]})json");
        string schema2(R"json({"topic":"topic2","fields":[{"name":"title","type":"string"}]})json");
        auto fakeAdminAdapter = dynamic_cast<FakeSwiftAdminAdapter *>(client.getAdminAdapter("fake_zk").get());
        fakeAdminAdapter->setNeedSchema(true);
        RegisterSchemaResponse response;
        RegisterSchemaRequest regScmReq;
        regScmReq.set_topicname("topic1");
        regScmReq.set_schema(schema1);
        regScmReq.set_version(100);
        EXPECT_EQ(ERROR_NONE, fakeAdminAdapter->registerSchema(regScmReq, response));
        EXPECT_EQ(100, response.version());
        regScmReq.set_topicname("topic2");
        regScmReq.set_schema(schema2);
        regScmReq.set_version(200);
        EXPECT_EQ(ERROR_NONE, fakeAdminAdapter->registerSchema(regScmReq, response));
        EXPECT_EQ(200, response.version());

        SwiftMultiReader mreader;
        ErrorInfo errorInfo;
        auto *reader1 = client.createReader("topicName=topic1", &errorInfo);
        auto *reader2 = client.createReader("topicName=topic2", &errorInfo);
        EXPECT_EQ(ERROR_NONE, errorInfo.errcode());
        mreader.addReader(reader1);
        mreader.addReader(reader2);
        ASSERT_EQ(2, mreader.size());

        common::SchemaWriter sw;
        EXPECT_TRUE(sw.loadSchema(schema1));
        sw._version = 100;
        MessageInfo msgInfo;
        msgInfo.data = sw.toString();
        util::MessageUtil::writeDataHead(msgInfo);
        ErrorCode ec = ERROR_NONE;
        common::SchemaReader *sr = mreader.getSchemaReader(msgInfo.data.c_str(), ec);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_TRUE(sr != nullptr);

        EXPECT_TRUE(sw.loadSchema(schema2));
        sw._version = 200;
        msgInfo.data = sw.toString();
        util::MessageUtil::writeDataHead(msgInfo);
        sr = mreader.getSchemaReader(msgInfo.data.c_str(), ec);
        EXPECT_EQ(ERROR_ADMIN_SCHEMA_NOT_FOUND, ec);
        EXPECT_TRUE(sr == nullptr);

        mreader._readerPos = 1;
        sr = mreader.getSchemaReader(msgInfo.data.c_str(), ec);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_TRUE(sr != nullptr);
    }
}

}; // namespace client
}; // namespace swift
