#include "swift/client/SwiftWriterAdapter.h"

#include <atomic>
#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/LoopThread.h"
#include "autil/Thread.h"
#include "autil/ThreadPool.h"
#include "swift/client/BufferSizeLimiter.h"
#include "swift/client/MessageInfo.h"
#include "swift/client/MessageWriteBuffer.h"
#include "swift/client/SingleSwiftWriter.h"
#include "swift/client/SwiftSchemaWriterImpl.h"
#include "swift/client/SwiftWriterConfig.h"
#include "swift/client/SwiftWriterImpl.h"
#include "swift/client/fake_client/FakeSwiftAdminAdapter.h"
#include "swift/client/fake_client/FakeSwiftWriter.h"
#include "swift/common/MessageInfo.h"
#include "swift/network/SwiftAdminAdapter.h"
#include "swift/network/SwiftRpcChannelManager.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/util/BlockPool.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace swift::protocol;
using namespace swift::util;
using namespace swift::network;

namespace swift {
namespace client {

class SwiftWriterAdapterTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void SwiftWriterAdapterTest::setUp() {}

void SwiftWriterAdapterTest::tearDown() {}

TEST_F(SwiftWriterAdapterTest, testInit) {
    FakeSwiftAdminAdapter *fakeAdapter = new FakeSwiftAdminAdapter();
    SwiftAdminAdapterPtr adminAdapter(fakeAdapter);

    SwiftWriterAdapter *wa =
        new SwiftWriterAdapter(adminAdapter, SwiftRpcChannelManagerPtr(), ThreadPoolPtr(), BufferSizeLimiterPtr());
    SwiftWriterConfig config;
    config.topicName = "test_topic";
    {
        fakeAdapter->setErrorCode(ERROR_ADMIN_TOPIC_NOT_EXISTED);
        ErrorCode ec = wa->init(config);
        ASSERT_EQ(ERROR_ADMIN_TOPIC_NOT_EXISTED, ec);
    }
    {
        config.functionChain = "not_exist_chain";
        fakeAdapter->setErrorCode(ERROR_NONE);
        fakeAdapter->setNeedSchema(false);
        ErrorCode ec = wa->init(config);
        auto *ptr = dynamic_cast<SwiftWriterImpl *>(wa->_writerImpl.get());
        ASSERT_TRUE(ptr != nullptr);
        ASSERT_EQ(ERROR_CLIENT_INIT_FAILED, ec);
    }
    {
        config.functionChain = "not_exist_chain";
        fakeAdapter->setErrorCode(ERROR_NONE);
        fakeAdapter->setNeedSchema(true);
        ErrorCode ec = wa->init(config);
        auto *ptr = dynamic_cast<SwiftSchemaWriterImpl *>(wa->_writerImpl.get());
        ASSERT_TRUE(ptr != nullptr);
        ASSERT_EQ(ERROR_CLIENT_INIT_FAILED, ec);
    }
    {
        config.functionChain = "not_exist_chain";
        fakeAdapter->setErrorCode(ERROR_NONE);
        fakeAdapter->setNeedSchema(true);
        fakeAdapter->setSealed(true);
        ErrorCode ec = wa->init(config);
        ASSERT_EQ(ERROR_TOPIC_SEALED, ec);
        fakeAdapter->setSealed(false);
    }
    {
        config.functionChain = "HASH";
        fakeAdapter->setErrorCode(ERROR_NONE);
        fakeAdapter->setNeedSchema(false);
        ErrorCode ec = wa->init(config);
        auto *ptr = dynamic_cast<SwiftWriterImpl *>(wa->_writerImpl.get());
        ASSERT_TRUE(ptr != nullptr);
        ASSERT_EQ(ERROR_NONE, ec);
    }
    { // TOPIC_TYPE_LOGIC_PHYSIC no child
        config.functionChain = "HASH";
        fakeAdapter->setErrorCode(ERROR_NONE);
        fakeAdapter->setNeedSchema(true);
        fakeAdapter->setTopicType(TOPIC_TYPE_LOGIC_PHYSIC);
        ErrorCode ec = wa->init(config);
        auto *ptr = dynamic_cast<SwiftSchemaWriterImpl *>(wa->_writerImpl.get());
        ASSERT_TRUE(ptr != nullptr);
        ASSERT_EQ(ERROR_NONE, ec);
    }
    { // TOPIC_TYPE_LOGIC_PHYSIC one child
        config.functionChain = "HASH";
        fakeAdapter->setErrorCode(ERROR_NONE);
        fakeAdapter->setNeedSchema(true);
        fakeAdapter->setTopicType(TOPIC_TYPE_LOGIC_PHYSIC);
        fakeAdapter->setPhysicTopicLst({"lp-1-1"});
        ErrorCode ec = wa->init(config);
        auto *ptr = dynamic_cast<SwiftSchemaWriterImpl *>(wa->_writerImpl.get());
        ASSERT_TRUE(ptr != nullptr);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ("lp-1-1", ptr->getTopicName());
    }
    { // TOPIC_TYPE_LOGIC two child
        config.functionChain = "HASH";
        fakeAdapter->setErrorCode(ERROR_NONE);
        fakeAdapter->setNeedSchema(true);
        fakeAdapter->setTopicType(TOPIC_TYPE_LOGIC);
        fakeAdapter->setPhysicTopicLst({"lp-1-1", "lp-2-1"});
        ErrorCode ec = wa->init(config);
        auto *ptr = dynamic_cast<SwiftSchemaWriterImpl *>(wa->_writerImpl.get());
        ASSERT_TRUE(ptr != nullptr);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ("lp-2-1", ptr->getTopicName());
    }
    { // TOPIC_TYPE_PHYSIC
        config.functionChain = "HASH";
        fakeAdapter->setErrorCode(ERROR_NONE);
        fakeAdapter->setNeedSchema(true);
        fakeAdapter->setTopicType(TOPIC_TYPE_PHYSIC);
        ErrorCode ec = wa->init(config);
        ASSERT_EQ(ERROR_CLIENT_INIT_INVALID_PARAMETERS, ec);
    }
    { // permission denied
        SwiftWriterAdapter writer(adminAdapter, SwiftRpcChannelManagerPtr(), ThreadPoolPtr(), BufferSizeLimiterPtr());
        SwiftWriterConfig writerConf;
        writerConf.topicName = "test_topic";
        fakeAdapter->setHasPermission(true);
        writer._config.functionChain = "NOT_EXIST";
        protocol::TopicPermission opControl;
        opControl.set_canwrite(false);
        fakeAdapter->setTopicPermission(opControl);
        writerConf.functionChain = "HASH";
        ErrorCode ec = writer.init(config);
        ASSERT_EQ(ERROR_CLIENT_INIT_FAILED, ec);
        ASSERT_EQ("NOT_EXIST", writer._config.functionChain);
        opControl.set_canwrite(true);
        fakeAdapter->setTopicPermission(opControl);
        ec = writer.init(config);
        ASSERT_EQ(ERROR_CLIENT_INIT_INVALID_PARAMETERS, ec);
        ASSERT_EQ("HASH", writer._config.functionChain);
        fakeAdapter->setHasPermission(false);
    }
    DELETE_AND_SET_NULL(wa);
}

TEST_F(SwiftWriterAdapterTest, testCreateAndInitWriterImpl) {
    FakeSwiftAdminAdapter *fakeAdapter = new FakeSwiftAdminAdapter();
    SwiftAdminAdapterPtr adminAdapter(fakeAdapter);
    SwiftWriterAdapter *wa =
        new SwiftWriterAdapter(adminAdapter, SwiftRpcChannelManagerPtr(), ThreadPoolPtr(), BufferSizeLimiterPtr());
    {
        TopicInfo ti;
        SwiftWriterImplPtr retPtr;
        ti.set_needschema(true);
        ErrorCode ec = wa->createAndInitWriterImpl(ti, retPtr);
        auto *ptr = dynamic_cast<SwiftSchemaWriterImpl *>(retPtr.get());
        EXPECT_TRUE(ptr != nullptr);
        EXPECT_EQ(ERROR_NONE, ec);
    }
    {
        TopicInfo ti;
        SwiftWriterImplPtr retPtr;
        ti.set_needschema(false);
        wa->_config.schemaVersion = 1234;
        ErrorCode ec = wa->createAndInitWriterImpl(ti, retPtr);
        EXPECT_EQ(ERROR_CLIENT_INVALID_PARAMETERS, ec);
        wa->_config.schemaVersion = SwiftWriterConfig::DEFAULT_SCHEMA_VERSION;
    }
    {
        TopicInfo ti;
        SwiftWriterImplPtr retPtr;
        ti.set_needschema(false);
        ErrorCode ec = wa->createAndInitWriterImpl(ti, retPtr);
        auto *ptr = dynamic_cast<SwiftWriterImpl *>(retPtr.get());
        EXPECT_TRUE(ptr != nullptr);
        EXPECT_EQ(ERROR_NONE, ec);
    }
    {
        TopicInfo ti;
        SwiftWriterImplPtr retPtr;
        ti.set_needschema(false);
        wa->_config.functionChain = "not_exist";
        ErrorCode ec = wa->createAndInitWriterImpl(ti, retPtr);
        auto *ptr = dynamic_cast<SwiftWriterImpl *>(retPtr.get());
        EXPECT_TRUE(ptr != nullptr);
        EXPECT_EQ(ERROR_CLIENT_INIT_FAILED, ec);
    }
    DELETE_AND_SET_NULL(wa);
}

TEST_F(SwiftWriterAdapterTest, testCreateNextPhysicWriterImpl) {
    FakeSwiftAdminAdapter *fakeAdapter = new FakeSwiftAdminAdapter();
    SwiftAdminAdapterPtr adminAdapter(fakeAdapter);

    SwiftWriterAdapter *wa =
        new SwiftWriterAdapter(adminAdapter, SwiftRpcChannelManagerPtr(), ThreadPoolPtr(), BufferSizeLimiterPtr());
    SwiftWriterConfig config;
    SwiftWriterImplPtr retPtr;
    config.topicName = "test_topic";
    {
        fakeAdapter->setErrorCode(ERROR_ADMIN_TOPIC_NOT_EXISTED);
        ErrorCode ec = wa->createNextPhysicWriterImpl(retPtr);
        ASSERT_EQ(ERROR_ADMIN_TOPIC_NOT_EXISTED, ec);
    }
    {
        config.functionChain = "HASH";
        fakeAdapter->setErrorCode(ERROR_NONE);
        fakeAdapter->setNeedSchema(false);
        ErrorCode ec = wa->createNextPhysicWriterImpl(retPtr);
        ASSERT_EQ(ERROR_CLIENT_LOGIC_TOPIC_EMPTY_PHYSIC, ec);
    }
    {
        config.functionChain = "HASH";
        fakeAdapter->setErrorCode(ERROR_NONE);
        fakeAdapter->setNeedSchema(false);
        fakeAdapter->setTopicType(TOPIC_TYPE_LOGIC);
        fakeAdapter->setPhysicTopicLst({"cur"});
        ErrorCode ec = wa->init(config);
        ASSERT_EQ(ERROR_NONE, ec);
        ec = wa->createNextPhysicWriterImpl(retPtr);
        ASSERT_EQ(ERROR_CLIENT_PHYSIC_TOPIC_SWITCH_NOT_READY, ec);
    }
    {
        config.functionChain = "HASH";
        fakeAdapter->setErrorCode(ERROR_NONE);
        fakeAdapter->setCallCnt(0);
        fakeAdapter->setErrorCodeMap({{3, ERROR_ADMIN_TOPIC_NOT_EXISTED}});
        fakeAdapter->setNeedSchema(false);
        fakeAdapter->setTopicType(TOPIC_TYPE_LOGIC);
        fakeAdapter->setPhysicTopicLst({"cur", "next"});
        ErrorCode ec = wa->init(config);
        ec = wa->createNextPhysicWriterImpl(retPtr);
        ASSERT_EQ(ERROR_ADMIN_TOPIC_NOT_EXISTED, ec);
        fakeAdapter->setErrorCodeMap({});
    }
    {
        config.functionChain = "HASH";
        fakeAdapter->setErrorCode(ERROR_NONE);
        fakeAdapter->setCallCnt(0);
        fakeAdapter->setNeedSchema(false);
        fakeAdapter->setTopicType(TOPIC_TYPE_LOGIC_PHYSIC);
        fakeAdapter->setPhysicTopicLst({"cur"});
        ErrorCode ec = wa->init(config);
        fakeAdapter->setPhysicTopicLst({"cur", "next"});
        ec = wa->createNextPhysicWriterImpl(retPtr);
        ASSERT_EQ(ERROR_NONE, ec);
    }
    {
        config.functionChain = "HASH";
        fakeAdapter->setErrorCode(ERROR_NONE);
        fakeAdapter->setNeedSchema(false);
        fakeAdapter->setTopicType(TOPIC_TYPE_PHYSIC);
        ErrorCode ec = wa->createNextPhysicWriterImpl(retPtr);
        ASSERT_EQ(ERROR_CLIENT_INIT_INVALID_PARAMETERS, ec);
    }
    DELETE_AND_SET_NULL(wa);
}

TEST_F(SwiftWriterAdapterTest, testDoSwitch) {
    { // no data, create next writer failed
        FakeSwiftAdminAdapter *fakeAdapter = new FakeSwiftAdminAdapter();
        SwiftAdminAdapterPtr adminAdapter(fakeAdapter);
        SwiftWriterAdapter *wa =
            new SwiftWriterAdapter(adminAdapter, SwiftRpcChannelManagerPtr(), ThreadPoolPtr(), BufferSizeLimiterPtr());
        SwiftWriterConfig config;
        config.topicName = "cur";
        config.functionChain = "HASH";
        fakeAdapter->setErrorCode(ERROR_NONE);
        fakeAdapter->setNeedSchema(false);
        fakeAdapter->setTopicType(TOPIC_TYPE_LOGIC_PHYSIC);
        ErrorCode ec = wa->init(config);
        wa->_sendRequestThreadPtr.reset();
        ASSERT_EQ(ERROR_NONE, ec);

        wa->_isSwitching = true;
        ASSERT_FALSE(wa->doSwitch());
        ASSERT_TRUE(wa->_isSwitching.load());
        ASSERT_EQ("cur", wa->_writerImpl->getTopicName());
        DELETE_AND_SET_NULL(wa);
    }
    { // no data, success
        FakeSwiftAdminAdapter *fakeAdapter = new FakeSwiftAdminAdapter();
        SwiftAdminAdapterPtr adminAdapter(fakeAdapter);
        SwiftWriterAdapter *wa =
            new SwiftWriterAdapter(adminAdapter, SwiftRpcChannelManagerPtr(), ThreadPoolPtr(), BufferSizeLimiterPtr());
        SwiftWriterConfig config;
        config.functionChain = "HASH";
        fakeAdapter->setErrorCode(ERROR_NONE);
        fakeAdapter->setNeedSchema(false);
        fakeAdapter->setTopicType(TOPIC_TYPE_LOGIC_PHYSIC);
        fakeAdapter->setPhysicTopicLst({"cur"});
        ErrorCode ec = wa->init(config);
        wa->_sendRequestThreadPtr.reset();
        ASSERT_EQ(ERROR_NONE, ec);

        wa->_isSwitching = true;
        fakeAdapter->setPhysicTopicLst({"cur", "next"});
        ASSERT_TRUE(wa->doSwitch());
        ASSERT_FALSE(wa->_isSwitching.load());
        ASSERT_EQ("next", wa->_writerImpl->getTopicName());
        DELETE_AND_SET_NULL(wa);
    }
    { // with data, success
        FakeSwiftAdminAdapter *fakeAdapter = new FakeSwiftAdminAdapter();
        SwiftAdminAdapterPtr adminAdapter(fakeAdapter);
        ThreadPoolPtr mergeThreadPool(new ThreadPool(1, 128));
        ASSERT_TRUE(mergeThreadPool->start("merge_msg"));
        SwiftWriterAdapter *wa =
            new SwiftWriterAdapter(adminAdapter, SwiftRpcChannelManagerPtr(), mergeThreadPool, BufferSizeLimiterPtr());
        SwiftWriterConfig config;
        config.topicName = "cur";
        config.functionChain = "hashId2partId";
        config.compressThresholdInBytes = 10;
        fakeAdapter->setPartCount(1);
        fakeAdapter->setErrorCode(ERROR_NONE);
        fakeAdapter->setNeedSchema(false);
        fakeAdapter->setTopicType(TOPIC_TYPE_LOGIC_PHYSIC);
        fakeAdapter->setPhysicTopicLst({"cur"});
        ErrorCode ec = wa->init(config);
        ASSERT_EQ(ERROR_NONE, ec);
        wa->_sendRequestThreadPtr.reset();
        ASSERT_EQ("cur", wa->_writerImpl->getTopicName());
        // send some msg
        for (int i = 0; i < 10; i++) {
            MessageInfo msgInfo;
            msgInfo.data = string(100, 'a' + i);
            msgInfo.uint16Payload = i * 100;
            wa->write(msgInfo);
        }
        EXPECT_EQ(10, wa->_writerImpl->_writers[0]->_writeBuffer.getUnsendCount());
        wa->_isSwitching = true;
        fakeAdapter->setPhysicTopicLst({"cur", "next"});
        ASSERT_TRUE(wa->doSwitch());
        ASSERT_FALSE(wa->_isSwitching.load());
        ASSERT_EQ("next", wa->_writerImpl->getTopicName());
        EXPECT_EQ(10, wa->_writerImpl->_writers[0]->_writeBuffer.getUnsendCount());
        vector<MessageInfo> msgInfoVec;
        EXPECT_TRUE(wa->_writerImpl->_writers[0]->_writeBuffer.stealUnsendMessage(msgInfoVec));
        for (int i = 0; i < 10; i++) {
            EXPECT_EQ(string(100, 'a' + i), msgInfoVec[i].data);
            EXPECT_EQ(i * 100, msgInfoVec[i].uint16Payload);
            cout << i << ":" << msgInfoVec[i].data << endl;
        }
        DELETE_AND_SET_NULL(wa);
    }
}

TEST_F(SwiftWriterAdapterTest, testWrite) {
    SwiftWriterAdapter *wa = new SwiftWriterAdapter(
        SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr(), ThreadPoolPtr(), BufferSizeLimiterPtr());
    wa->_sendRequestThreadPtr.reset();
    FakeSwiftWriter *fakeWriter = new FakeSwiftWriter();
    wa->_writerImpl.reset(fakeWriter);
    ErrorCode ec;
    {
        fakeWriter->_callCnt = 0;
        fakeWriter->_errorCodeMap = {{1, ERROR_NONE}};
        MessageInfo msgInfo;
        msgInfo.data = "1";
        ec = wa->write(msgInfo);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ(1, fakeWriter->_callCnt);
        EXPECT_FALSE(wa->_isSwitching.load());
        EXPECT_EQ("1", fakeWriter->_lastMsgInfo.data);
    }
    {
        fakeWriter->_callCnt = 0;
        fakeWriter->_errorCodeMap = {{1, ERROR_TOPIC_SEALED}};
        wa->_topicInfo.set_topictype(TOPIC_TYPE_NORMAL);
        MessageInfo msgInfo;
        msgInfo.data = "2";
        ec = wa->write(msgInfo);
        EXPECT_EQ(ERROR_TOPIC_SEALED, ec);
        EXPECT_EQ(1, fakeWriter->_callCnt);
        EXPECT_FALSE(wa->_isSwitching.load());
        EXPECT_EQ("1", fakeWriter->_lastMsgInfo.data);
    }
    {
        fakeWriter->_callCnt = 0;
        fakeWriter->_errorCodeMap = {{1, ERROR_TOPIC_SEALED}, {2, ERROR_UNKNOWN}};
        wa->_topicInfo.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
        MessageInfo msgInfo;
        msgInfo.data = "3";

        ThreadPtr threadPtr = Thread::createThread([&wa]() {
            usleep(100 * 1000);
            wa->_isSwitching = false;
        });
        ASSERT_TRUE(threadPtr);
        ec = wa->write(msgInfo);
        threadPtr->join();
        EXPECT_EQ(ERROR_UNKNOWN, ec);
        EXPECT_EQ(2, fakeWriter->_callCnt);
        EXPECT_FALSE(wa->_isSwitching.load());
        EXPECT_EQ("1", fakeWriter->_lastMsgInfo.data);
    }
    {
        fakeWriter->_callCnt = 0;
        fakeWriter->_errorCodeMap = {{1, ERROR_TOPIC_SEALED}, {2, ERROR_NONE}};
        wa->_topicInfo.set_topictype(TOPIC_TYPE_LOGIC);
        MessageInfo msgInfo;
        msgInfo.data = "4";
        ThreadPtr threadPtr = Thread::createThread([&wa]() {
            usleep(100 * 1000);
            wa->_isSwitching = false;
        });
        ASSERT_TRUE(threadPtr);
        ec = wa->write(msgInfo);
        threadPtr->join();
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_FALSE(wa->_isSwitching.load());
        EXPECT_EQ("4", fakeWriter->_lastMsgInfo.data);
    }
    DELETE_AND_SET_NULL(wa);
}

TEST_F(SwiftWriterAdapterTest, testWrite_topicChanged) {
    FakeSwiftAdminAdapter *fakeAdapter = new FakeSwiftAdminAdapter();
    SwiftAdminAdapterPtr adminAdapter(fakeAdapter);
    SwiftWriterAdapter *wa =
        new SwiftWriterAdapter(adminAdapter, SwiftRpcChannelManagerPtr(), ThreadPoolPtr(), BufferSizeLimiterPtr());
    SwiftWriterConfig config;
    config.topicName = "lp_topic";
    config.functionChain = "HASH";
    fakeAdapter->setPartCount(1);
    fakeAdapter->setErrorCode(ERROR_NONE);
    fakeAdapter->setNeedSchema(false);
    fakeAdapter->setTopicType(TOPIC_TYPE_LOGIC_PHYSIC);
    ErrorCode ec = wa->init(config);
    wa->_sendRequestThreadPtr.reset();
    ASSERT_EQ(ERROR_NONE, ec);
    { // topicChanged
        fakeAdapter->_callCnt = 0;
        fakeAdapter->_errorCodeMap = {{1, ERROR_NONE}};
        MessageInfo msgInfo;
        msgInfo.data = "1";

        EXPECT_EQ(1, wa->_topicInfo.partitioncount());
        fakeAdapter->setPartCount(3);
        wa->_writerImpl->setTopicChanged(1234567);
        fakeAdapter->setPhysicTopicLst({"lp_topic-200-2"});
        ec = wa->write(msgInfo);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ(3, wa->_topicInfo.partitioncount());
        EXPECT_EQ(1, wa->_topicInfo.physictopiclst_size());
        EXPECT_EQ("lp_topic-200-2", wa->_topicInfo.physictopiclst(0));
        EXPECT_EQ(3, wa->_writerImpl->_partitionCount);
        EXPECT_EQ(1, fakeAdapter->_callCnt);
        EXPECT_FALSE(wa->_isSwitching);
    }
    DELETE_AND_SET_NULL(wa);
}

TEST_F(SwiftWriterAdapterTest, testStartSendRequestThread) {
    SwiftWriterAdapter wa((SwiftAdminAdapterPtr()), SwiftRpcChannelManagerPtr(), ThreadPoolPtr());
    ASSERT_TRUE(!wa._sendRequestThreadPtr);
    ASSERT_TRUE(wa.startSendRequestThread());
    ASSERT_TRUE(wa._sendRequestThreadPtr);
}

TEST_F(SwiftWriterAdapterTest, testSendRequestLoop) {
    FakeSwiftAdminAdapter *fakeAdapter = new FakeSwiftAdminAdapter();
    SwiftAdminAdapterPtr adminAdapter(fakeAdapter);
    BlockPoolPtr blockPool(new BlockPool(1000, 100));
    SwiftWriterAdapter *wa = new SwiftWriterAdapter(
        adminAdapter, SwiftRpcChannelManagerPtr(), ThreadPoolPtr(), BufferSizeLimiterPtr(), blockPool);
    SwiftWriterConfig config;
    config.functionChain = "HASH";
    fakeAdapter->setErrorCode(ERROR_NONE);
    fakeAdapter->setNeedSchema(false);
    fakeAdapter->setTopicType(TOPIC_TYPE_LOGIC_PHYSIC);
    fakeAdapter->setPhysicTopicLst({"cur"});
    ErrorCode ec = wa->init(config);
    wa->_sendRequestThreadPtr.reset();
    ASSERT_EQ(ERROR_NONE, ec);

    wa->_isSwitching = false;
    fakeAdapter->setPhysicTopicLst({"cur", "next"});
    wa->sendRequestLoop();
    ASSERT_FALSE(wa->_isSwitching.load());
    ASSERT_EQ("cur", wa->_writerImpl->getTopicName());

    wa->_isSwitching = true;
    fakeAdapter->setPhysicTopicLst({"cur", "next"});
    wa->sendRequestLoop();
    ASSERT_FALSE(wa->_isSwitching.load());
    ASSERT_EQ("next", wa->_writerImpl->getTopicName());

    wa->_isSwitching = false;
    fakeAdapter->setPhysicTopicLst({"cur", "next", "nextnext"});
    auto *singleWriter = wa->_writerImpl->insertSingleWriter(0);
    singleWriter->_writeBuffer._sealed = true;
    wa->sendRequestLoop();

    if (!wa->_isSwitching) {
        wa->sendRequestLoop();
    }
    ASSERT_TRUE(wa->_isSwitching.load());
    ASSERT_EQ("next", wa->_writerImpl->getTopicName());
    wa->sendRequestLoop();
    ASSERT_FALSE(wa->_isSwitching.load());
    ASSERT_EQ("nextnext", wa->_writerImpl->getTopicName());
    singleWriter = wa->_writerImpl->insertSingleWriter(0);
    MessageInfo msg;
    singleWriter->_writeBuffer._sealed = false; // let msg in
    EXPECT_EQ(ERROR_NONE, wa->write(msg));
    singleWriter->_writeBuffer._sealed = true;
    wa->sendRequestLoop();
    EXPECT_TRUE(wa->_isSwitching.load());
    EXPECT_EQ("nextnext", wa->_writerImpl->getTopicName());

    wa->_isSwitching = true;
    wa->_writerImpl = nullptr;
    wa->sendRequestLoop();
    ASSERT_TRUE(wa->_writerImpl == nullptr);
    DELETE_AND_SET_NULL(wa);
}

TEST_F(SwiftWriterAdapterTest, testDoTopicChanged) {
    FakeSwiftAdminAdapter *fakeAdapter = new FakeSwiftAdminAdapter();
    SwiftAdminAdapterPtr adminAdapter(fakeAdapter);
    SwiftWriterAdapter *wa =
        new SwiftWriterAdapter(adminAdapter, SwiftRpcChannelManagerPtr(), ThreadPoolPtr(), BufferSizeLimiterPtr());
    SwiftWriterConfig config;
    config.topicName = "lp";
    config.functionChain = "HASH";
    fakeAdapter->setPartCount(2);
    fakeAdapter->setErrorCode(ERROR_NONE);
    fakeAdapter->setNeedSchema(false);
    fakeAdapter->setTopicType(TOPIC_TYPE_LOGIC_PHYSIC);
    ErrorCode ec = wa->init(config);
    wa->_sendRequestThreadPtr.reset();
    ASSERT_EQ(ERROR_NONE, ec);
    fakeAdapter->setPhysicTopicLst({"cur"});

    // topic not changed
    fakeAdapter->setErrorCode(ERROR_NONE);
    fakeAdapter->setPartCount(4);
    ASSERT_FALSE(wa->_writerImpl->topicChanged());
    ec = wa->doTopicChanged();
    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_FALSE(wa->_writerImpl->topicChanged());
    ASSERT_EQ(2, wa->_writerImpl->_partitionCount);
    ASSERT_EQ(0, wa->_topicInfo.physictopiclst_size());

    // getTopicInfo failed
    fakeAdapter->setErrorCode(ERROR_UNKNOWN);
    wa->_writerImpl->setTopicChanged(1234567);
    fakeAdapter->setErrorCode(ERROR_UNKNOWN);
    ec = wa->doTopicChanged();
    ASSERT_EQ(ERROR_UNKNOWN, ec);
    ASSERT_TRUE(wa->_writerImpl->topicChanged());
    ASSERT_EQ(2, wa->_writerImpl->_partitionCount);
    ASSERT_EQ(0, wa->_topicInfo.physictopiclst_size());

    // success
    fakeAdapter->setErrorCode(ERROR_NONE);
    ec = wa->doTopicChanged();
    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_FALSE(wa->_writerImpl->topicChanged());
    ASSERT_EQ(4, wa->_writerImpl->_partitionCount);
    ASSERT_EQ(1, wa->_topicInfo.physictopiclst_size());
    ASSERT_EQ("cur", wa->_topicInfo.physictopiclst(0));
    DELETE_AND_SET_NULL(wa);
}

}; // namespace client
}; // namespace swift
