#include "swift/client/SwiftReaderAdapter.h"

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <typeinfo>
#include <unistd.h>
#include <vector>

#include "autil/TimeUtility.h"
#include "swift/client/SwiftReaderConfig.h"
#include "swift/client/SwiftReaderImpl.h"
#include "swift/client/SwiftSchemaReaderImpl.h"
#include "swift/client/SwiftSinglePartitionReader.h"
#include "swift/client/SwiftTransportClientCreator.h"
#include "swift/client/fake_client/FakeClientHelper.h"
#include "swift/client/fake_client/FakeSwiftAdminAdapter.h"
#include "swift/client/fake_client/FakeSwiftTransportClient.h"
#include "swift/common/Common.h"
#include "swift/network/SwiftAdminAdapter.h"
#include "swift/network/SwiftRpcChannelManager.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace autil;
using namespace swift::protocol;
using namespace swift::common;
using namespace swift::network;

namespace swift {
namespace client {

class SwiftReaderAdapterTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void SwiftReaderAdapterTest::setUp() {}

void SwiftReaderAdapterTest::tearDown() {
    SwiftTransportClientCreator::FAKE_CLIENT_AUTO_ASYNC_CALL = false;
    SwiftTransportClientCreator::_fakeDataMap.clear();
}

TEST_F(SwiftReaderAdapterTest, testReadTopicChangedWithPartCountChange) {
    SwiftTransportClientCreator::FAKE_CLIENT_AUTO_ASYNC_CALL = true;
    FakeSwiftAdminAdapter *fakeAdminAdapter = new FakeSwiftAdminAdapter();
    fakeAdminAdapter->setPartCount(2);
    SwiftAdminAdapterPtr adminAdapter(fakeAdminAdapter);
    string topicName = "topic_name";
    SwiftReaderConfig config;
    config.topicName = topicName;
    config.readBufferSize = 1;
    config.oneRequestReadCount = 1;
    config.batchReadCount = 1;

    SwiftReaderAdapter reader(adminAdapter, SwiftRpcChannelManagerPtr());
    ASSERT_EQ(ERROR_NONE, reader.init(config));
    ASSERT_EQ(uint32_t(2), reader._swiftReaderImpl->_readers.size());
    protocol::Message msg;
    int64_t timestamp;
    ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, reader.read(timestamp, msg, 200 * 1000));
    sleep(1); // for delay post request after 500ms
    FakeSwiftTransportClient *fakeClient0 = FakeClientHelper::getTransportClient(&reader, 0);
    FakeSwiftTransportClient *fakeClient1 = FakeClientHelper::getTransportClient(&reader, 1);
    ASSERT_TRUE(fakeClient0 != NULL);
    ASSERT_TRUE(fakeClient1 != NULL);
    uint32_t messageCount = 3;
    int64_t msgId = 0;
    int64_t baseTime = autil::TimeUtility::currentTime() + 1000;
    FakeClientHelper::makeData(fakeClient0, messageCount, msgId, 3, "test0", baseTime);     // 0 3 6
    FakeClientHelper::makeData(fakeClient1, messageCount, msgId, 2, "test1", baseTime + 1); // 1 3 5
    fakeClient1 = FakeClientHelper::getTransportClient(&reader, 1);
    ASSERT_EQ(messageCount, fakeClient1->_messageVec->size());

    int64_t acceptTimestamp = 0;
    int64_t timeLimit = baseTime + 3;
    reader.setTimestampLimit(timeLimit, acceptTimestamp);
    ASSERT_TRUE(acceptTimestamp == timeLimit);
    ASSERT_EQ(ERROR_NONE, reader.read(timestamp, msg));
    ASSERT_EQ(int64_t(0), msg.msgid());
    ASSERT_EQ(string("test0"), msg.data());
    ASSERT_EQ(baseTime, msg.timestamp());

    sleep(1);
    // inc partitionCount
    fakeAdminAdapter->setPartCount(4);
    fakeClient1 = FakeClientHelper::getTransportClient(&reader, 1);
    fakeClient1->setErrorCode(ERROR_BROKER_SESSION_CHANGED); // to reconnect

    ASSERT_EQ(ERROR_NONE, reader.read(timestamp, msg));
    ASSERT_EQ(int64_t(0), msg.msgid());
    ASSERT_EQ(baseTime + 1, msg.timestamp());
    ASSERT_EQ(string("test1"), msg.data());

    ASSERT_EQ(ERROR_NONE, reader.read(timestamp, msg)); // reader get topic change
    ASSERT_EQ(int64_t(1), msg.msgid());
    ASSERT_EQ(baseTime + 3, msg.timestamp());
    ASSERT_EQ(string("test0"), msg.data());
    fakeClient1->setErrorCode(ERROR_NONE);

    ASSERT_EQ(ERROR_NONE, reader.read(timestamp, msg)); // reader reinit, seek to baseTime +3; seek by timestamp
    ASSERT_EQ(int64_t(1), msg.msgid());
    ASSERT_EQ(baseTime + 3, msg.timestamp());
    ASSERT_EQ(string("test0"), msg.data());

    reader.read(timestamp, msg);
    ASSERT_EQ(int64_t(1), msg.msgid());
    ASSERT_EQ(baseTime + 3, msg.timestamp());
    ASSERT_EQ(string("test1"), msg.data());

    ErrorCode ec = reader.read(timestamp, msg);
    ASSERT_EQ(ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT, ec);

    ASSERT_EQ(uint32_t(4), reader._swiftReaderImpl->_readers.size());
    for (size_t i = 0; i < reader._swiftReaderImpl->_readers.size(); i++) {
        ASSERT_TRUE(reader._swiftReaderImpl->_readers[i]->getTimestampLimit() == timeLimit);
    }
}

TEST_F(SwiftReaderAdapterTest, testReadTopicChangedPartCountNotChangeIgnoreCommitted) {
    SwiftTransportClientCreator::FAKE_CLIENT_AUTO_ASYNC_CALL = true;
    FakeSwiftAdminAdapter *fakeAdminAdapter = new FakeSwiftAdminAdapter();
    fakeAdminAdapter->setPartCount(2);
    SwiftAdminAdapterPtr adminAdapter(fakeAdminAdapter);
    string topicName = "topic_name";
    SwiftReaderConfig config;
    config.topicName = topicName;
    config.readBufferSize = 1;
    config.oneRequestReadCount = 1;
    config.batchReadCount = 1;

    SwiftReaderAdapter reader(adminAdapter, SwiftRpcChannelManagerPtr());
    ASSERT_EQ(ERROR_NONE, reader.init(config));
    ASSERT_EQ(uint32_t(2), reader._swiftReaderImpl->_readers.size());
    protocol::Message msg;
    int64_t timestamp;
    ASSERT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, reader.read(timestamp, msg, 200 * 1000));
    sleep(1); // for delay post request after 500ms
    FakeSwiftTransportClient *fakeClient0 = FakeClientHelper::getTransportClient(&reader, 0);
    FakeSwiftTransportClient *fakeClient1 = FakeClientHelper::getTransportClient(&reader, 1);
    ASSERT_TRUE(fakeClient0 != NULL);
    ASSERT_TRUE(fakeClient1 != NULL);
    uint32_t messageCount = 3;
    int64_t msgId = 0;
    int64_t baseTime = autil::TimeUtility::currentTime() + 1000;
    FakeClientHelper::makeData(fakeClient0, messageCount, msgId, 3, "test0", baseTime);     // 0 3 6
    FakeClientHelper::makeData(fakeClient1, messageCount, msgId, 2, "test1", baseTime + 1); // 1 3 5
    fakeClient1 = FakeClientHelper::getTransportClient(&reader, 1);
    ASSERT_EQ(messageCount, fakeClient1->_messageVec->size());

    int64_t acceptTimestamp = 0;
    int64_t timeLimit = baseTime + 3;
    reader.setTimestampLimit(timeLimit, acceptTimestamp);
    ASSERT_TRUE(acceptTimestamp == timeLimit);
    ASSERT_EQ(ERROR_NONE, reader.read(timestamp, msg));
    ASSERT_EQ(int64_t(0), msg.msgid());
    ASSERT_EQ(string("test0"), msg.data());
    ASSERT_EQ(baseTime, msg.timestamp());
    sleep(1);
    // topic part count not change
    fakeClient1 = FakeClientHelper::getTransportClient(&reader, 1);
    fakeClient1->setErrorCode(ERROR_BROKER_SESSION_CHANGED); // to reconnect
    auto oldReaderImpl = reader._swiftReaderImpl;
    ASSERT_EQ(ERROR_NONE, reader.read(timestamp, msg));
    ASSERT_EQ(int64_t(0), msg.msgid());
    ASSERT_EQ(baseTime + 1, msg.timestamp());
    ASSERT_EQ(string("test1"), msg.data());

    ASSERT_EQ(ERROR_NONE, reader.read(timestamp, msg)); // reader get topic change
    ASSERT_EQ(int64_t(1), msg.msgid());
    ASSERT_EQ(baseTime + 3, msg.timestamp());
    ASSERT_EQ(string("test0"), msg.data());
    fakeClient1->setErrorCode(ERROR_NONE);
    ASSERT_EQ(ERROR_NONE, reader.read(timestamp, msg)); // reader not  reinit
    ASSERT_EQ(int64_t(1), msg.msgid());
    ASSERT_EQ(baseTime + 3, msg.timestamp());
    ASSERT_EQ(string("test1"), msg.data());
    auto newReaderImpl = reader._swiftReaderImpl;
    ASSERT_TRUE(oldReaderImpl.get() == newReaderImpl.get());
    ErrorCode ec = reader.read(timestamp, msg);
    ASSERT_EQ(ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT, ec);
}

TEST_F(SwiftReaderAdapterTest, testInit) {
    SwiftTransportClientCreator::FAKE_CLIENT_AUTO_ASYNC_CALL = true;
    FakeSwiftAdminAdapter *fakeAdminAdapter = new FakeSwiftAdminAdapter();
    fakeAdminAdapter->setPartCount(2);
    SwiftAdminAdapterPtr adminAdapter(fakeAdminAdapter);
    string topicName = "topic";
    SwiftReaderConfig config;
    config.topicName = topicName;
    config.readBufferSize = 1;
    config.oneRequestReadCount = 1;
    config.batchReadCount = 1;
    config.readPartVec = {0};

    ErrorCode ec;
    {
        SwiftReaderAdapter reader(adminAdapter, SwiftRpcChannelManagerPtr());
        ec = reader.init(config);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ(config.topicName, reader._swiftReaderImpl->_config.topicName);
    }
    {
        fakeAdminAdapter->setCallCnt(0);
        fakeAdminAdapter->setErrorCodeMap({{1, ERROR_UNKNOWN}});
        SwiftReaderAdapter reader(adminAdapter, SwiftRpcChannelManagerPtr());
        ec = reader.init(config);
        EXPECT_EQ(ERROR_UNKNOWN, ec);
        EXPECT_TRUE(reader._swiftReaderImpl == nullptr);
    }
    {
        fakeAdminAdapter->setTopicType(TOPIC_TYPE_LOGIC);
        fakeAdminAdapter->_physicTopicLst = {};
        SwiftReaderAdapter reader(adminAdapter, SwiftRpcChannelManagerPtr());
        ec = reader.init(config);
        EXPECT_EQ(ERROR_CLIENT_INIT_INVALID_PARAMETERS, ec);
        EXPECT_TRUE(reader._swiftReaderImpl == nullptr);
    }
    {
        fakeAdminAdapter->setTopicType(TOPIC_TYPE_LOGIC);
        fakeAdminAdapter->_physicTopicLst = {"topic-100-2"};
        fakeAdminAdapter->setCallCnt(0);
        fakeAdminAdapter->setErrorCodeMap({{2, ERROR_UNKNOWN}});
        SwiftReaderAdapter reader(adminAdapter, SwiftRpcChannelManagerPtr());
        ec = reader.init(config);
        EXPECT_EQ(ERROR_UNKNOWN, ec);
        EXPECT_TRUE(reader._swiftReaderImpl == nullptr);
    }
    {
        fakeAdminAdapter->setTopicType(TOPIC_TYPE_PHYSIC);
        SwiftReaderAdapter reader(adminAdapter, SwiftRpcChannelManagerPtr());
        ec = reader.init(config);
        EXPECT_EQ(ERROR_CLIENT_INIT_INVALID_PARAMETERS, ec);
        EXPECT_TRUE(reader._swiftReaderImpl == nullptr);
    }
    {
        fakeAdminAdapter->setTopicType(TOPIC_TYPE_LOGIC);
        fakeAdminAdapter->_physicTopicLst = {"topic-100-2"};
        SwiftReaderAdapter reader(adminAdapter, SwiftRpcChannelManagerPtr());
        ec = reader.init(config);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ("topic-100-2", reader._swiftReaderImpl->_config.topicName);
    }
    {
        fakeAdminAdapter->setTopicType(TOPIC_TYPE_LOGIC_PHYSIC);
        fakeAdminAdapter->_physicTopicLst = {"topic-100-2"};
        SwiftReaderAdapter reader(adminAdapter, SwiftRpcChannelManagerPtr());
        ec = reader.init(config);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ(config.topicName, reader._swiftReaderImpl->_config.topicName);
    }
    { // permission denied
        fakeAdminAdapter->setHasPermission(true);
        protocol::TopicPermission opControl;
        opControl.set_canread(false);
        fakeAdminAdapter->setTopicPermission(opControl);
        SwiftReaderAdapter reader(adminAdapter, SwiftRpcChannelManagerPtr());
        reader._readerConfig.topicName = "NOT_EXIST";
        ec = reader.init(config);
        ASSERT_EQ(ERROR_CLIENT_INIT_FAILED, ec);
        ASSERT_EQ("NOT_EXIST", reader._readerConfig.topicName);

        opControl.set_canread(true);
        fakeAdminAdapter->setTopicPermission(opControl);
        ec = reader.init(config);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ(config.topicName, reader._readerConfig.topicName);
        fakeAdminAdapter->setHasPermission(false);
    }
}

TEST_F(SwiftReaderAdapterTest, testDoFindPhysicName) {
    SwiftTransportClientCreator::FAKE_CLIENT_AUTO_ASYNC_CALL = true;
    FakeSwiftAdminAdapter *fakeAdminAdapter = new FakeSwiftAdminAdapter();
    fakeAdminAdapter->setPartCount(2);
    SwiftAdminAdapterPtr adminAdapter(fakeAdminAdapter);
    ErrorCode ec;
    {
        SwiftReaderAdapter reader(adminAdapter, SwiftRpcChannelManagerPtr());
        reader._topicInfo.set_topictype(TOPIC_TYPE_LOGIC);
        reader._topicInfo.add_physictopiclst("topic-100-2");
        reader._topicInfo.add_physictopiclst("topic-200-2");
        string physicName;
        bool isLastPhysic = false;
        ec = reader.doFindPhysicName(50, physicName, isLastPhysic);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ("topic-100-2", physicName);
        EXPECT_EQ(false, isLastPhysic);

        ec = reader.doFindPhysicName(100, physicName, isLastPhysic);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ("topic-100-2", physicName);
        EXPECT_EQ(false, isLastPhysic);

        ec = reader.doFindPhysicName(150, physicName, isLastPhysic);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ("topic-100-2", physicName);
        EXPECT_EQ(false, isLastPhysic);

        ec = reader.doFindPhysicName(200, physicName, isLastPhysic);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ("topic-200-2", physicName);
        EXPECT_EQ(true, isLastPhysic);

        ec = reader.doFindPhysicName(250, physicName, isLastPhysic);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ("topic-200-2", physicName);
        EXPECT_EQ(true, isLastPhysic);
    }
    {
        SwiftReaderAdapter reader(adminAdapter, SwiftRpcChannelManagerPtr());
        reader._topicInfo.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
        reader._topicInfo.set_name("topic");
        reader._topicInfo.add_physictopiclst("topic-200-2");
        string physicName;
        bool isLastPhysic = false;
        ec = reader.doFindPhysicName(50, physicName, isLastPhysic);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ("topic", physicName);
        EXPECT_EQ(false, isLastPhysic);

        ec = reader.doFindPhysicName(100, physicName, isLastPhysic);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ("topic", physicName);
        EXPECT_EQ(false, isLastPhysic);

        ec = reader.doFindPhysicName(150, physicName, isLastPhysic);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ("topic", physicName);
        EXPECT_EQ(false, isLastPhysic);

        ec = reader.doFindPhysicName(200, physicName, isLastPhysic);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ("topic-200-2", physicName);
        EXPECT_EQ(true, isLastPhysic);

        ec = reader.doFindPhysicName(250, physicName, isLastPhysic);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ("topic-200-2", physicName);
        EXPECT_EQ(true, isLastPhysic);
    }
}

TEST_F(SwiftReaderAdapterTest, testFindPhysicName) {
    SwiftTransportClientCreator::FAKE_CLIENT_AUTO_ASYNC_CALL = true;
    FakeSwiftAdminAdapter *fakeAdminAdapter = new FakeSwiftAdminAdapter();
    fakeAdminAdapter->setPartCount(2);
    SwiftAdminAdapterPtr adminAdapter(fakeAdminAdapter);
    string topicName = "topic";
    ErrorCode ec;
    {
        string physicName;
        SwiftReaderAdapter reader(adminAdapter, SwiftRpcChannelManagerPtr());
        reader._topicInfo.set_topictype(TOPIC_TYPE_LOGIC);
        reader._topicInfo.add_physictopiclst("topic-100-2");
        reader._topicInfo.add_physictopiclst("topic-200-2");

        ec = reader.findPhysicName(150, physicName);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ("topic-100-2", physicName);

        fakeAdminAdapter->_physicTopicLst = {"topic-100-2", "topic-200-2", "topic-300-2"};
        ec = reader.findPhysicName(150, physicName);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ("topic-100-2", physicName);
    }
    {
        string physicName;
        SwiftReaderAdapter reader(adminAdapter, SwiftRpcChannelManagerPtr());
        reader._topicInfo.set_topictype(TOPIC_TYPE_LOGIC);
        reader._topicInfo.add_physictopiclst("topic-100-2");

        fakeAdminAdapter->_physicTopicLst = {"topic-100-2"};
        ec = reader.findPhysicName(200, physicName);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ("topic-100-2", physicName);

        fakeAdminAdapter->_physicTopicLst = {"topic-100-2", "topic-200-2"};
        ec = reader.findPhysicName(200, physicName);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ("topic-200-2", physicName);
    }
    {
        string physicName;
        SwiftReaderAdapter reader(adminAdapter, SwiftRpcChannelManagerPtr());
        reader._topicInfo.set_topictype(TOPIC_TYPE_LOGIC);
        reader._topicInfo.add_physictopiclst("topic-100-2");

        fakeAdminAdapter->setCallCnt(0);
        fakeAdminAdapter->setErrorCodeMap({{1, ERROR_UNKNOWN}});
        ec = reader.seekPhysicTopic(100);
        EXPECT_EQ(ERROR_UNKNOWN, ec);
    }
}

TEST_F(SwiftReaderAdapterTest, testCreateAndInitReaderImpl) {
    SwiftTransportClientCreator::FAKE_CLIENT_AUTO_ASYNC_CALL = true;
    FakeSwiftAdminAdapter *fakeAdminAdapter = new FakeSwiftAdminAdapter();
    fakeAdminAdapter->setPartCount(2);
    SwiftAdminAdapterPtr adminAdapter(fakeAdminAdapter);
    string topicName = "topic";
    SwiftReaderConfig config;
    config.topicName = topicName;
    config.readBufferSize = 1;
    config.oneRequestReadCount = 1;
    config.batchReadCount = 1;
    config.readPartVec = {0};
    SwiftReaderAdapter reader(adminAdapter, SwiftRpcChannelManagerPtr());
    reader._readerConfig = config;

    ErrorCode ec;
    {
        TopicInfo tinfo;
        tinfo.set_name("topic");
        tinfo.set_partitioncount(2);
        tinfo.set_needschema(false);
        SwiftReaderImplPtr retReaderImpl;
        ec = reader.createAndInitReaderImpl(tinfo, retReaderImpl);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_TRUE(retReaderImpl.get());
        auto &readerImpl = *retReaderImpl.get();
        ASSERT_EQ(string(typeid(SwiftReaderImpl).name()), string(typeid(readerImpl).name()));
    }
    {
        TopicInfo tinfo;
        tinfo.set_name("topic");
        tinfo.set_partitioncount(0);
        tinfo.set_needschema(false);
        SwiftReaderImplPtr retReaderImpl;
        ec = reader.createAndInitReaderImpl(tinfo, retReaderImpl);
        EXPECT_EQ(ERROR_CLIENT_INVALID_PARTITIONID, ec);
        EXPECT_TRUE(retReaderImpl.get());
    }
    {
        string schemaStr = R"json({"topic":"topic","fields":[{"name":"nid","type":"string"}]})json";
        fakeAdminAdapter->_schemaCache[0] = FakeSwiftAdminAdapter::SchemaCacheItem(1, schemaStr);
        fakeAdminAdapter->setErrorCode(ERROR_NONE);
        TopicInfo tinfo;
        tinfo.set_name("topic");
        tinfo.set_partitioncount(2);
        tinfo.set_needschema(true);
        SwiftReaderImplPtr retReaderImpl;
        ec = reader.createAndInitReaderImpl(tinfo, retReaderImpl);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_TRUE(retReaderImpl.get());
        auto &readerImpl = *retReaderImpl.get();
        ASSERT_EQ(string(typeid(SwiftSchemaReaderImpl).name()), string(typeid(readerImpl).name()));
    }
    {
        string schemaStr = R"json({"topic":"topic","fields":[{"name":"nid","type":"string"}]})json";
        fakeAdminAdapter->_schemaCache[0] = FakeSwiftAdminAdapter::SchemaCacheItem(1, schemaStr);
        fakeAdminAdapter->setErrorCode(ERROR_NONE);
        TopicInfo tinfo;
        tinfo.set_name("topic");
        tinfo.set_partitioncount(0);
        tinfo.set_needschema(true);
        SwiftReaderImplPtr retReaderImpl;
        ec = reader.createAndInitReaderImpl(tinfo, retReaderImpl);
        EXPECT_EQ(ERROR_CLIENT_INVALID_PARTITIONID, ec);
        EXPECT_TRUE(retReaderImpl.get());
    }
}

TEST_F(SwiftReaderAdapterTest, testCreateNextPhysicReaderImpl) {
    SwiftTransportClientCreator::FAKE_CLIENT_AUTO_ASYNC_CALL = true;
    FakeSwiftAdminAdapter *fakeAdminAdapter = new FakeSwiftAdminAdapter();
    fakeAdminAdapter->setPartCount(2);
    SwiftAdminAdapterPtr adminAdapter(fakeAdminAdapter);
    string topicName = "topic";
    SwiftReaderConfig config;
    config.topicName = topicName;
    config.readBufferSize = 1;
    config.oneRequestReadCount = 1;
    config.batchReadCount = 1;
    config.readPartVec = {0};
    SwiftReaderAdapter reader(adminAdapter, SwiftRpcChannelManagerPtr());
    ASSERT_EQ(ERROR_NONE, reader.init(config));

    ErrorCode ec;
    SwiftReaderImplPtr retReaderImpl;
    {
        reader._topicInfo.set_name("topic");
        reader._topicInfo.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
        reader._topicInfo.add_physictopiclst("topic-100-2");

        reader._swiftReaderImpl->_config.topicName = "topic";
        ec = reader.createNextPhysicReaderImpl(retReaderImpl);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ("topic-100-2", retReaderImpl->_config.topicName);

        reader._swiftReaderImpl->_config.topicName = "topic-100-2";
        fakeAdminAdapter->_physicTopicLst = {"topic-100-2"};
        ec = reader.createNextPhysicReaderImpl(retReaderImpl);
        EXPECT_EQ(ERROR_CLIENT_PHYSIC_TOPIC_SWITCH_NOT_READY, ec);

        fakeAdminAdapter->setCallCnt(0);
        fakeAdminAdapter->setErrorCodeMap({{1, ERROR_UNKNOWN}});
        reader._swiftReaderImpl->_config.topicName = "topic";
        ec = reader.createNextPhysicReaderImpl(retReaderImpl);
        EXPECT_EQ(ERROR_UNKNOWN, ec);
    }
    {
        fakeAdminAdapter->setErrorCode(ERROR_NONE);
        reader._topicInfo.set_name("topic");
        reader._topicInfo.set_topictype(TOPIC_TYPE_LOGIC);
        // cur:1 local: 1,2 remote: 3,4 -> 1|3,4
        fakeAdminAdapter->setCallCnt(0);
        fakeAdminAdapter->setErrorCodeMap({{1, ERROR_ADMIN_TOPIC_NOT_EXISTED}});
        reader._topicInfo.clear_physictopiclst();
        reader._topicInfo.add_physictopiclst("topic-100-1");
        reader._topicInfo.add_physictopiclst("topic-200-2");
        fakeAdminAdapter->_topicType = TOPIC_TYPE_LOGIC;
        fakeAdminAdapter->_physicTopicLst = {"topic-300-3", "topic-400-4"};
        reader._swiftReaderImpl->_config.topicName = "topic-100-1";
        ec = reader.createNextPhysicReaderImpl(retReaderImpl);
        EXPECT_EQ(ERROR_ADMIN_TOPIC_NOT_EXISTED, ec);
        EXPECT_EQ("topic-100-1", reader._swiftReaderImpl->_config.topicName);
        EXPECT_EQ(2, reader._topicInfo.physictopiclst_size());
        EXPECT_EQ("topic-300-3", reader._topicInfo.physictopiclst(0));
        EXPECT_EQ("topic-400-4", reader._topicInfo.physictopiclst(1));
        // cur:1 local: 3,4 remote: 3,4 -> 3|3,4
        reader._topicInfo.clear_physictopiclst();
        reader._topicInfo.add_physictopiclst("topic-300-3");
        reader._topicInfo.add_physictopiclst("topic-400-4");
        fakeAdminAdapter->_physicTopicLst = {"topic-300-3", "topic-400-4"};
        reader._swiftReaderImpl->_config.topicName = "topic-100-1";
        ec = reader.createNextPhysicReaderImpl(retReaderImpl);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ("topic-300-3", retReaderImpl->_config.topicName);
        EXPECT_EQ(2, reader._topicInfo.physictopiclst_size());
        EXPECT_EQ("topic-300-3", reader._topicInfo.physictopiclst(0));
        EXPECT_EQ("topic-400-4", reader._topicInfo.physictopiclst(1));
        // cur:2 local: 1,2 remote: 3,4 -> 3|3,4
        reader._topicInfo.clear_physictopiclst();
        reader._topicInfo.add_physictopiclst("topic-100-1");
        reader._topicInfo.add_physictopiclst("topic-200-2");
        fakeAdminAdapter->_physicTopicLst = {"topic-300-3", "topic-400-4"};
        reader._swiftReaderImpl->_config.topicName = "topic-200-2";
        ec = reader.createNextPhysicReaderImpl(retReaderImpl);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ("topic-300-3", retReaderImpl->_config.topicName);
        EXPECT_EQ(2, reader._topicInfo.physictopiclst_size());
        EXPECT_EQ("topic-300-3", reader._topicInfo.physictopiclst(0));
        EXPECT_EQ("topic-400-4", reader._topicInfo.physictopiclst(1));
        // cur:2 local: 2,3 remote: 3,4 -> 3|2,3
        reader._topicInfo.clear_physictopiclst();
        reader._topicInfo.add_physictopiclst("topic-200-2");
        reader._topicInfo.add_physictopiclst("topic-300-3");
        fakeAdminAdapter->_physicTopicLst = {"topic-300-3", "topic-400-4"};
        reader._swiftReaderImpl->_config.topicName = "topic-200-2";
        ec = reader.createNextPhysicReaderImpl(retReaderImpl);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ("topic-300-3", retReaderImpl->_config.topicName);
        EXPECT_EQ(2, reader._topicInfo.physictopiclst_size());
        EXPECT_EQ("topic-200-2", reader._topicInfo.physictopiclst(0));
        EXPECT_EQ("topic-300-3", reader._topicInfo.physictopiclst(1));
        // cur:3 local: 2,3 remote: 3,4 -> 4|3,4
        reader._topicInfo.clear_physictopiclst();
        reader._topicInfo.add_physictopiclst("topic-200-2");
        reader._topicInfo.add_physictopiclst("topic-300-3");
        fakeAdminAdapter->_physicTopicLst = {"topic-300-3", "topic-400-4"};
        reader._swiftReaderImpl->_config.topicName = "topic-300-3";
        ec = reader.createNextPhysicReaderImpl(retReaderImpl);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ("topic-400-4", retReaderImpl->_config.topicName);
        EXPECT_EQ(2, reader._topicInfo.physictopiclst_size());
        EXPECT_EQ("topic-300-3", reader._topicInfo.physictopiclst(0));
        EXPECT_EQ("topic-400-4", reader._topicInfo.physictopiclst(1));
        // cur:3 local: 3,4 remote: 3,4 -> 4|3,4
        reader._topicInfo.clear_physictopiclst();
        reader._topicInfo.add_physictopiclst("topic-300-3");
        reader._topicInfo.add_physictopiclst("topic-400-4");
        fakeAdminAdapter->_physicTopicLst = {"topic-300-3", "topic-400-4"};
        reader._swiftReaderImpl->_config.topicName = "topic-300-3";
        ec = reader.createNextPhysicReaderImpl(retReaderImpl);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ("topic-400-4", retReaderImpl->_config.topicName);
        EXPECT_EQ(2, reader._topicInfo.physictopiclst_size());
        EXPECT_EQ("topic-300-3", reader._topicInfo.physictopiclst(0));
        EXPECT_EQ("topic-400-4", reader._topicInfo.physictopiclst(1));
    }
}

TEST_F(SwiftReaderAdapterTest, testUpdateReaderImpl) {
    SwiftTransportClientCreator::FAKE_CLIENT_AUTO_ASYNC_CALL = true;
    FakeSwiftAdminAdapter *fakeAdminAdapter = new FakeSwiftAdminAdapter();
    fakeAdminAdapter->setPartCount(2);
    SwiftAdminAdapterPtr adminAdapter(fakeAdminAdapter);
    string topicName = "topic";
    SwiftReaderConfig config;
    config.topicName = topicName;
    SwiftReaderAdapter reader(adminAdapter, SwiftRpcChannelManagerPtr());
    ASSERT_EQ(ERROR_NONE, reader.init(config));
    ErrorCode ec;
    {
        fakeAdminAdapter->setCallCnt(0);
        fakeAdminAdapter->setErrorCodeMap({{1, ERROR_UNKNOWN}});
        ec = reader.updateReaderImpl("topic2");
        EXPECT_EQ(ERROR_UNKNOWN, ec);
        EXPECT_EQ("topic", reader._swiftReaderImpl->getTopicName());
    }
    {
        fakeAdminAdapter->setCallCnt(0);
        fakeAdminAdapter->setErrorCodeMap({});
        ec = reader.updateReaderImpl("topic2");
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ("topic2", reader._swiftReaderImpl->_config.topicName);
    }
    {
        fakeAdminAdapter->setCallCnt(0);
        fakeAdminAdapter->setErrorCodeMap({{1, ERROR_ADMIN_TOPIC_NOT_EXISTED}, {2, ERROR_ADMIN_WAIT_IN_QUEUE_TIMEOUT}});
        ec = reader.updateReaderImpl("topic3");
        EXPECT_EQ(ERROR_ADMIN_TOPIC_NOT_EXISTED, ec);
        EXPECT_EQ(TOPIC_TYPE_NORMAL, reader._topicInfo.topictype());
        EXPECT_EQ("topic2", reader._swiftReaderImpl->_config.topicName);
    }
    {
        fakeAdminAdapter->setCallCnt(0);
        fakeAdminAdapter->setErrorCodeMap({{1, ERROR_ADMIN_TOPIC_NOT_EXISTED}});
        fakeAdminAdapter->setTopicType(TOPIC_TYPE_LOGIC);
        ec = reader.updateReaderImpl("topic4");
        EXPECT_EQ(ERROR_ADMIN_TOPIC_NOT_EXISTED, ec);
        EXPECT_EQ(TOPIC_TYPE_LOGIC, reader._topicInfo.topictype());
        EXPECT_EQ("topic2", reader._swiftReaderImpl->_config.topicName);
    }
}

TEST_F(SwiftReaderAdapterTest, testSeekPhysicTopic_timestamp) {
    SwiftTransportClientCreator::FAKE_CLIENT_AUTO_ASYNC_CALL = true;
    FakeSwiftAdminAdapter *fakeAdminAdapter = new FakeSwiftAdminAdapter();
    fakeAdminAdapter->setPartCount(2);
    SwiftAdminAdapterPtr adminAdapter(fakeAdminAdapter);
    string topicName = "topic";
    SwiftReaderConfig config;
    config.topicName = topicName;
    SwiftReaderAdapter reader(adminAdapter, SwiftRpcChannelManagerPtr());
    ASSERT_EQ(ERROR_NONE, reader.init(config));

    reader._topicInfo.set_name("topic");
    reader._topicInfo.set_topictype(TOPIC_TYPE_LOGIC);
    reader._topicInfo.add_physictopiclst("topic-100-2");
    reader._topicInfo.add_physictopiclst("topic-200-2");
    fakeAdminAdapter->_physicTopicLst = {"topic-100-2", "topic-200-2"};

    ErrorCode ec;
    ec = reader.seekPhysicTopic(250);
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ("topic-200-2", reader._swiftReaderImpl->_config.topicName);

    ec = reader.seekPhysicTopic(200);
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ("topic-200-2", reader._swiftReaderImpl->_config.topicName);

    ec = reader.seekPhysicTopic(150);
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ("topic-100-2", reader._swiftReaderImpl->_config.topicName);

    ec = reader.seekPhysicTopic(100);
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ("topic-100-2", reader._swiftReaderImpl->_config.topicName);

    ec = reader.seekPhysicTopic(50);
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ("topic-100-2", reader._swiftReaderImpl->_config.topicName);

    fakeAdminAdapter->setCallCnt(0);
    fakeAdminAdapter->setErrorCodeMap({{1, ERROR_UNKNOWN}});
    ec = reader.seekPhysicTopic(200);
    EXPECT_EQ(ERROR_UNKNOWN, ec);

    fakeAdminAdapter->setCallCnt(0);
    fakeAdminAdapter->setErrorCodeMap({{1, ERROR_UNKNOWN}});
    ec = reader.seekPhysicTopic(0);
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ("topic-100-2", reader._swiftReaderImpl->_config.topicName);
}

TEST_F(SwiftReaderAdapterTest, testSeekPhysicTopic_progress) {
    SwiftTransportClientCreator::FAKE_CLIENT_AUTO_ASYNC_CALL = true;
    FakeSwiftAdminAdapter *fakeAdminAdapter = new FakeSwiftAdminAdapter();
    fakeAdminAdapter->setPartCount(2);
    SwiftAdminAdapterPtr adminAdapter(fakeAdminAdapter);
    string topicName = "topic";
    SwiftReaderConfig config;
    config.topicName = topicName;
    config.readBufferSize = 1;
    config.oneRequestReadCount = 1;
    config.batchReadCount = 1;
    config.readPartVec = {0};
    SwiftReaderAdapter reader(adminAdapter, SwiftRpcChannelManagerPtr());
    ASSERT_EQ(ERROR_NONE, reader.init(config));

    reader._topicInfo.set_name("topic");
    reader._topicInfo.set_topictype(TOPIC_TYPE_LOGIC);
    reader._topicInfo.add_physictopiclst("topic-100-2");
    reader._topicInfo.add_physictopiclst("topic-200-2");
    fakeAdminAdapter->_physicTopicLst = {"topic-100-2", "topic-200-2"};

    ErrorCode ec;
    {
        ReaderProgress progress;
        protocol::TopicReaderProgress *topicProgress;
        topicProgress = progress.add_topicprogress();
        topicProgress->set_topicname("topic");
        protocol::PartReaderProgress *partProgress;
        partProgress = topicProgress->add_partprogress();
        partProgress->set_timestamp(100);
        partProgress = topicProgress->add_partprogress();
        partProgress->set_timestamp(150);
        ec = reader.seekPhysicTopic(progress);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ("topic-100-2", reader._swiftReaderImpl->_config.topicName);
    }
    {
        ReaderProgress progress;
        protocol::TopicReaderProgress *topicProgress;
        topicProgress = progress.add_topicprogress();
        topicProgress->set_topicname("topic");
        protocol::PartReaderProgress *partProgress;
        partProgress = topicProgress->add_partprogress();
        partProgress->set_timestamp(250);
        partProgress = topicProgress->add_partprogress();
        partProgress->set_timestamp(150);

        ec = reader.seekPhysicTopic(progress);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ("topic-100-2", reader._swiftReaderImpl->_config.topicName);
    }
    {
        ReaderProgress progress;
        protocol::TopicReaderProgress *topicProgress;
        topicProgress = progress.add_topicprogress();
        topicProgress->set_topicname("topic");
        protocol::PartReaderProgress *partProgress;
        partProgress = topicProgress->add_partprogress();
        partProgress->set_timestamp(250);
        partProgress = topicProgress->add_partprogress();
        partProgress->set_timestamp(300);
        ec = reader.seekPhysicTopic(progress);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ("topic-200-2", reader._swiftReaderImpl->_config.topicName);
    }
}

TEST_F(SwiftReaderAdapterTest, testDoGetNextPhysicTopicName) {
    SwiftTransportClientCreator::FAKE_CLIENT_AUTO_ASYNC_CALL = true;
    FakeSwiftAdminAdapter *fakeAdminAdapter = new FakeSwiftAdminAdapter();
    fakeAdminAdapter->setPartCount(2);
    SwiftAdminAdapterPtr adminAdapter(fakeAdminAdapter);
    string topicName = "topic";
    SwiftReaderConfig config;
    config.topicName = topicName;
    config.readBufferSize = 1;
    config.oneRequestReadCount = 1;
    config.batchReadCount = 1;
    config.readPartVec = {0};
    {
        SwiftReaderAdapter reader(adminAdapter, SwiftRpcChannelManagerPtr());
        ASSERT_EQ(ERROR_NONE, reader.init(config));

        reader._topicInfo.set_name("topic");
        reader._topicInfo.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);

        bool ret;
        string topicName;
        reader._swiftReaderImpl->_config.topicName = "topic";
        ret = reader.doGetNextPhysicTopicName(topicName);
        EXPECT_FALSE(ret);

        reader._topicInfo.add_physictopiclst("topic-100-2");
        reader._swiftReaderImpl->_config.topicName = "topic";
        ret = reader.doGetNextPhysicTopicName(topicName);
        EXPECT_TRUE(ret);
        EXPECT_EQ("topic-100-2", topicName);
    }
    {
        SwiftReaderAdapter reader(adminAdapter, SwiftRpcChannelManagerPtr());
        ASSERT_EQ(ERROR_NONE, reader.init(config));

        reader._topicInfo.set_name("topic");
        reader._topicInfo.set_topictype(TOPIC_TYPE_LOGIC);
        reader._topicInfo.add_physictopiclst("topic-100-2");
        reader._topicInfo.add_physictopiclst("topic-200-2");
        reader._topicInfo.add_physictopiclst("topic-300-2");

        bool ret;
        string topicName;
        reader._swiftReaderImpl->_config.topicName = "topic-100-2";
        ret = reader.doGetNextPhysicTopicName(topicName);
        EXPECT_TRUE(ret);
        EXPECT_EQ("topic-200-2", topicName);

        reader._swiftReaderImpl->_config.topicName = "topic-200-2";
        ret = reader.doGetNextPhysicTopicName(topicName);
        EXPECT_TRUE(ret);
        EXPECT_EQ("topic-300-2", topicName);

        reader._swiftReaderImpl->_config.topicName = "topic-300-2";
        ret = reader.doGetNextPhysicTopicName(topicName);
        EXPECT_FALSE(ret);
    }
    {
        SwiftReaderAdapter reader(adminAdapter, SwiftRpcChannelManagerPtr());
        ASSERT_EQ(ERROR_NONE, reader.init(config));

        reader._topicInfo.set_name("topic");
        reader._topicInfo.set_topictype(TOPIC_TYPE_LOGIC);
        reader._topicInfo.add_physictopiclst("topic-300-3");
        reader._topicInfo.add_physictopiclst("topic-400-4");

        bool ret;
        string topicName;
        reader._swiftReaderImpl->_config.topicName = "topic-100-1";
        ret = reader.doGetNextPhysicTopicName(topicName);
        EXPECT_TRUE(ret);
        EXPECT_EQ("topic-300-3", topicName);

        reader._swiftReaderImpl->_config.topicName = "topic-200-2";
        ret = reader.doGetNextPhysicTopicName(topicName);
        EXPECT_TRUE(ret);
        EXPECT_EQ("topic-300-3", topicName);

        reader._swiftReaderImpl->_config.topicName = "topic-300-3";
        ret = reader.doGetNextPhysicTopicName(topicName);
        EXPECT_TRUE(ret);
        EXPECT_EQ("topic-400-4", topicName);

        reader._swiftReaderImpl->_config.topicName = "topic-400-4";
        ret = reader.doGetNextPhysicTopicName(topicName);
        EXPECT_FALSE(ret);
    }
}

TEST_F(SwiftReaderAdapterTest, testGetNextPhysicTopicName) {
    SwiftTransportClientCreator::FAKE_CLIENT_AUTO_ASYNC_CALL = true;
    FakeSwiftAdminAdapter *fakeAdminAdapter = new FakeSwiftAdminAdapter();
    fakeAdminAdapter->setPartCount(2);
    SwiftAdminAdapterPtr adminAdapter(fakeAdminAdapter);
    string topicName = "topic";
    SwiftReaderConfig config;
    config.topicName = topicName;
    config.readBufferSize = 1;
    config.oneRequestReadCount = 1;
    config.batchReadCount = 1;
    config.readPartVec = {0};

    ErrorCode ec;
    {
        SwiftReaderAdapter reader(adminAdapter, SwiftRpcChannelManagerPtr());
        ASSERT_EQ(ERROR_NONE, reader.init(config));

        reader._topicInfo.set_name("topic");
        reader._topicInfo.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);

        string topicName;
        reader._topicInfo.add_physictopiclst("topic-100-2");
        reader._swiftReaderImpl->_config.topicName = "topic";
        ec = reader.getNextPhysicTopicName(topicName);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ("topic-100-2", topicName);

        reader._swiftReaderImpl->_config.topicName = "topic-100-2";
        fakeAdminAdapter->_physicTopicLst = {"topic-100-2"};
        ec = reader.getNextPhysicTopicName(topicName);
        ASSERT_EQ(ERROR_CLIENT_PHYSIC_TOPIC_SWITCH_NOT_READY, ec);

        reader._swiftReaderImpl->_config.topicName = "topic-100-2";
        fakeAdminAdapter->_physicTopicLst = {"topic-100-2", "topic-200-2"};
        ec = reader.getNextPhysicTopicName(topicName);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ("topic-200-2", topicName);

        reader._swiftReaderImpl->_config.topicName = "topic-100-2";
        reader._topicInfo.clear_physictopiclst();
        reader._topicInfo.add_physictopiclst("topic-100-2");
        fakeAdminAdapter->_physicTopicLst = {"topic-100-2", "topic-200-2"};
        fakeAdminAdapter->setCallCnt(0);
        fakeAdminAdapter->setErrorCodeMap({{1, ERROR_UNKNOWN}, {2, ERROR_UNKNOWN}});
        ec = reader.getNextPhysicTopicName(topicName);
        ASSERT_EQ(ERROR_UNKNOWN, ec);
    }
}

TEST_F(SwiftReaderAdapterTest, testDoSwitch) {
    SwiftTransportClientCreator::FAKE_CLIENT_AUTO_ASYNC_CALL = true;
    FakeSwiftAdminAdapter *fakeAdminAdapter = new FakeSwiftAdminAdapter();
    fakeAdminAdapter->setPartCount(2);
    SwiftAdminAdapterPtr adminAdapter(fakeAdminAdapter);
    string topicName = "topic";
    SwiftReaderConfig config;
    config.topicName = topicName;
    config.readBufferSize = 1;
    config.oneRequestReadCount = 1;
    config.batchReadCount = 1;
    config.readPartVec = {0};

    bool ret;
    {
        SwiftReaderAdapter reader(adminAdapter, SwiftRpcChannelManagerPtr());
        ASSERT_EQ(ERROR_NONE, reader.init(config));

        reader._topicInfo.set_name("topic");
        reader._topicInfo.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
        reader._topicInfo.add_physictopiclst("topic-100-2");
        fakeAdminAdapter->_physicTopicLst = {"topic-100-2", "topic-200-3"};
        reader._swiftReaderImpl->_config.topicName = "topic";

        fakeAdminAdapter->setPartCount(2);
        ret = reader.doSwitch();
        EXPECT_TRUE(ret);
        EXPECT_EQ("topic-100-2", reader._swiftReaderImpl->_config.topicName);
        EXPECT_EQ(2, reader._swiftReaderImpl->_partitionCount);

        fakeAdminAdapter->setPartCount(3);
        ret = reader.doSwitch();
        EXPECT_TRUE(ret);
        EXPECT_EQ("topic-200-3", reader._swiftReaderImpl->_config.topicName);
        EXPECT_EQ(3, reader._swiftReaderImpl->_partitionCount);

        fakeAdminAdapter->setPartCount(4);
        fakeAdminAdapter->setCallCnt(0);
        fakeAdminAdapter->setErrorCodeMap({{1, ERROR_UNKNOWN}});
        ret = reader.doSwitch();
        EXPECT_FALSE(ret);
        EXPECT_EQ("topic-200-3", reader._swiftReaderImpl->_config.topicName);
        EXPECT_EQ(3, reader._swiftReaderImpl->_partitionCount);
    }
    {
        SwiftReaderAdapter reader(adminAdapter, SwiftRpcChannelManagerPtr());
        ASSERT_EQ(ERROR_NONE, reader.init(config));

        reader._topicInfo.set_name("topic");
        reader._topicInfo.set_topictype(TOPIC_TYPE_LOGIC);
        reader._topicInfo.add_physictopiclst("topic-100-2");
        fakeAdminAdapter->_physicTopicLst = {"topic-100-2", "topic-200-3"};
        reader._swiftReaderImpl->_config.topicName = "topic-100-2";

        fakeAdminAdapter->setPartCount(3);
        ret = reader.doSwitch();
        EXPECT_TRUE(ret);
        EXPECT_EQ("topic-200-3", reader._swiftReaderImpl->_config.topicName);
        EXPECT_EQ(3, reader._swiftReaderImpl->_partitionCount);

        fakeAdminAdapter->setPartCount(4);
        fakeAdminAdapter->setCallCnt(0);
        fakeAdminAdapter->setErrorCodeMap({{1, ERROR_UNKNOWN}});
        ret = reader.doSwitch();
        EXPECT_FALSE(ret);
        EXPECT_EQ("topic-200-3", reader._swiftReaderImpl->_config.topicName);
        EXPECT_EQ(3, reader._swiftReaderImpl->_partitionCount);
    }
}

TEST_F(SwiftReaderAdapterTest, testUpdateLogicTopicInfo) {
    SwiftTransportClientCreator::FAKE_CLIENT_AUTO_ASYNC_CALL = true;
    FakeSwiftAdminAdapter *fakeAdminAdapter = new FakeSwiftAdminAdapter();
    fakeAdminAdapter->setPartCount(2);
    SwiftAdminAdapterPtr adminAdapter(fakeAdminAdapter);
    SwiftReaderAdapter reader(adminAdapter, SwiftRpcChannelManagerPtr());
    reader._topicInfo.set_name("test_topic");
    EXPECT_EQ(ERROR_NONE, reader.updateLogicTopicInfo());
    string expect("name: \"test_topic\" partitionCount: 2 needFieldFilter: false "
                  "needSchema: false sealed: false topicType: TOPIC_TYPE_NORMAL");
    EXPECT_EQ(expect, reader._topicInfo.ShortDebugString());

    fakeAdminAdapter->_physicTopicLst = {"topic-100-2", "topic-200-3"};
    fakeAdminAdapter->setTopicType(TOPIC_TYPE_LOGIC_PHYSIC);
    EXPECT_EQ(ERROR_NONE, reader.updateLogicTopicInfo());
    expect = "name: \"test_topic\" partitionCount: 2 needFieldFilter: false "
             "needSchema: false sealed: false topicType: TOPIC_TYPE_LOGIC_PHYSIC "
             "physicTopicLst: \"topic-100-2\" physicTopicLst: \"topic-200-3\"";
    EXPECT_EQ(expect, reader._topicInfo.ShortDebugString());

    // update error
    fakeAdminAdapter->_physicTopicLst.clear();
    fakeAdminAdapter->setTopicType(TOPIC_TYPE_NORMAL);
    fakeAdminAdapter->setPartCount(1);
    fakeAdminAdapter->setErrorCode(ERROR_ADMIN_WAIT_IN_QUEUE_TIMEOUT);
    EXPECT_EQ(ERROR_ADMIN_WAIT_IN_QUEUE_TIMEOUT, reader.updateLogicTopicInfo());
    EXPECT_EQ(expect, reader._topicInfo.ShortDebugString()); // not change
}

TEST_F(SwiftReaderAdapterTest, testDoTopicChanged) {
    SwiftTransportClientCreator::FAKE_CLIENT_AUTO_ASYNC_CALL = true;
    FakeSwiftAdminAdapter *fakeAdminAdapter = new FakeSwiftAdminAdapter();
    fakeAdminAdapter->setPartCount(1);
    SwiftAdminAdapterPtr adminAdapter(fakeAdminAdapter);
    string topicName = "test_topic";
    SwiftReaderConfig config;
    config.topicName = topicName;

    SwiftReaderAdapter reader(adminAdapter, SwiftRpcChannelManagerPtr());
    ASSERT_EQ(ERROR_NONE, reader.init(config));
    // 1. not reader->isTopicChanged
    ASSERT_EQ(ERROR_NONE, reader.doTopicChanged());

    // 2. getTopicInfo fail
    reader._swiftReaderImpl->setTopicChanged(12345);
    fakeAdminAdapter->setErrorCode(ERROR_ADMIN_TOPIC_NOT_EXISTED);
    ASSERT_EQ(ERROR_ADMIN_TOPIC_NOT_EXISTED, reader.doTopicChanged());

    // 3. getTopicInfo success, not logic topic, part count not change
    fakeAdminAdapter->setErrorCode(ERROR_NONE);
    ASSERT_EQ(ERROR_NONE, reader.doTopicChanged());
    ASSERT_FALSE(reader._swiftReaderImpl->isTopicChanged());
    ASSERT_EQ(1, reader._swiftReaderImpl->_readers.size());

    // 4. getTopicInfo success, not logic topic, part changed
    fakeAdminAdapter->setErrorCode(ERROR_NONE);
    fakeAdminAdapter->setPartCount(2);
    reader._swiftReaderImpl->setTopicChanged(12345);
    reader._swiftReaderImpl->_checkpointTimestamp = 0;
    ASSERT_EQ(ERROR_NONE, reader.doTopicChanged());
    ASSERT_FALSE(reader._swiftReaderImpl->isTopicChanged());
    ASSERT_EQ(2, reader._swiftReaderImpl->_readers.size());

    // 5. logic topic not exist
    fakeAdminAdapter->setTopicType(TOPIC_TYPE_LOGIC);
    fakeAdminAdapter->setErrorCode(ERROR_ADMIN_TOPIC_NOT_EXISTED);
    reader._swiftReaderImpl->setTopicChanged(12345);
    ASSERT_EQ(ERROR_ADMIN_TOPIC_NOT_EXISTED, reader.doTopicChanged());
    ASSERT_TRUE(reader._swiftReaderImpl->isTopicChanged());
    ASSERT_EQ(2, reader._swiftReaderImpl->_readers.size());

    // 6. logic topic, physic switch fail
    fakeAdminAdapter->setTopicType(TOPIC_TYPE_LOGIC);
    fakeAdminAdapter->setErrorCode(ERROR_ADMIN_TOPIC_NOT_EXISTED);
    reader._topicInfo.set_name("physic");
    reader._topicInfo.set_topictype(TOPIC_TYPE_LOGIC);
    ASSERT_EQ(ERROR_CLIENT_PHYSIC_TOPIC_SWITCH_NOT_READY, reader.doTopicChanged());
    ASSERT_TRUE(reader._swiftReaderImpl->isTopicChanged());
    ASSERT_EQ(2, reader._swiftReaderImpl->_readers.size());

    // 7. logic topic, LP -> L, no physic topic, switch fail
    fakeAdminAdapter->setTopicType(TOPIC_TYPE_LOGIC);
    fakeAdminAdapter->setErrorCode(ERROR_NONE);
    reader._topicInfo.set_name(topicName);
    reader._topicInfo.set_topictype(TOPIC_TYPE_LOGIC);
    fakeAdminAdapter->setPartCount(1);
    ASSERT_EQ(ERROR_CLIENT_PHYSIC_TOPIC_SWITCH_NOT_READY, reader.doTopicChanged());
    ASSERT_TRUE(reader._swiftReaderImpl->isTopicChanged());
    ASSERT_EQ(2, reader._swiftReaderImpl->_readers.size());

    // 8. logic topic, LP -> L, switch success
    fakeAdminAdapter->_physicTopicLst = {"test_topic-100-2"};
    ASSERT_EQ(ERROR_NONE, reader.doTopicChanged());
    ASSERT_FALSE(reader._swiftReaderImpl->isTopicChanged());
    ASSERT_EQ(1, reader._swiftReaderImpl->_readers.size());
}

TEST_F(SwiftReaderAdapterTest, testNeedResetReader) {
    SwiftReaderAdapter adapter({}, {});
    {
        TopicInfo topicInfo;
        topicInfo.set_partitioncount(1);
        ASSERT_FALSE(adapter.needResetReader(topicInfo, 1));
        topicInfo.set_topictype(TOPIC_TYPE_LOGIC);
        ASSERT_FALSE(adapter.needResetReader(topicInfo, 1));
    }
    {
        TopicInfo topicInfo;
        topicInfo.set_partitioncount(1);
        ASSERT_TRUE(adapter.needResetReader(topicInfo, 2));
    }
    {
        TopicInfo topicInfo;
        topicInfo.set_topicmode(TOPIC_MODE_SECURITY);
        topicInfo.set_partitioncount(1);
        ASSERT_FALSE(adapter.needResetReader(topicInfo, 1));
    }
    {
        TopicInfo topicInfo;
        topicInfo.set_partitioncount(1);
        ASSERT_FALSE(adapter.needResetReader(topicInfo, 1));
    }
}

}; // namespace client
}; // namespace swift
