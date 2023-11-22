#include "swift/client/SwiftSchemaWriterImpl.h"

#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "autil/ThreadPool.h"
#include "swift/client/MessageInfo.h"
#include "swift/client/SwiftTransportClientCreator.h"
#include "swift/client/SwiftWriterConfig.h"
#include "swift/client/fake_client/FakeSwiftAdminAdapter.h"
#include "swift/common/Common.h"
#include "swift/common/SchemaWriter.h"
#include "swift/network/SwiftAdminAdapter.h"
#include "swift/network/SwiftRpcChannelManager.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/util/Block.h"
#include "swift/util/BlockPool.h"
#include "swift/util/MessageInfoUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace swift::protocol;
using namespace swift::util;
using namespace swift::common;
using namespace swift::network;
namespace swift {
namespace client {

class SwiftSchemaWriterImplTest : public TESTBASE {
public:
    void setUp() {
        FakeSwiftAdminAdapter *fakeAdapter = new FakeSwiftAdminAdapter;
        fakeAdapter->setPartCount(10);
        _topicInfo.set_partitioncount(10);
        _adapter.reset(fakeAdapter);
    }
    void tearDown() {
        SwiftTransportClientCreator::FAKE_CLIENT_AUTO_ASYNC_CALL = false;
        SwiftTransportClientCreator::_fakeDataMap.clear();
        _adapter.reset();
    }

private:
    SwiftAdminAdapterPtr _adapter;
    TopicInfo _topicInfo;
};

TEST_F(SwiftSchemaWriterImplTest, testInit2) {
    FakeSwiftAdminAdapter *fakeAdminAdapter = new FakeSwiftAdminAdapter();
    SwiftAdminAdapterPtr adminAdapter(fakeAdminAdapter);
    ErrorCode ec = ERROR_NONE;
    {
        fakeAdminAdapter->setErrorCode(ERROR_UNKNOWN);
        SwiftSchemaWriterImpl writer(adminAdapter, SwiftRpcChannelManagerPtr(), ThreadPoolPtr(), _topicInfo);
        SwiftWriterConfig config;
        config.topicName = "topic";
        ec = writer.init(config, BlockPoolPtr());
        EXPECT_EQ(ERROR_NONE, ec);
    }

    fakeAdminAdapter->setErrorCode(ERROR_NONE);
    uint32_t rightVersion, wrongVersion = 100;
    string schemaStr = R"json({"topic":"topic","fields":[{"name":"nid","type":"string"}]})json";
    RegisterSchemaResponse response;
    RegisterSchemaRequest request;
    request.set_topicname("topic");
    request.set_schema(schemaStr);
    fakeAdminAdapter->registerSchema(request, response);
    rightVersion = response.version();
    FakeSwiftAdminAdapter::SchemaCacheItem item(12345, "{invald schema}");
    fakeAdminAdapter->_schemaCache.insert(std::make_pair(wrongVersion, item));

    {
        fakeAdminAdapter->setErrorCode(ERROR_NONE);
        SwiftSchemaWriterImpl writer(adminAdapter, SwiftRpcChannelManagerPtr(), ThreadPoolPtr(), _topicInfo);
        SwiftWriterConfig config;
        config.topicName = "topic";
        config.schemaVersion = 10;
        ec = writer.init(config, BlockPoolPtr());
        EXPECT_EQ(ERROR_ADMIN_SCHEMA_NOT_FOUND, ec);
    }

    {
        SwiftSchemaWriterImpl writer(adminAdapter, SwiftRpcChannelManagerPtr(), ThreadPoolPtr(), _topicInfo);
        SwiftWriterConfig config;
        config.topicName = "topic";
        config.schemaVersion = wrongVersion;
        ec = writer.init(config, BlockPoolPtr());
        EXPECT_EQ(ERROR_CLIENT_LOAD_SCHEMA_FAIL, ec);
    }

    {
        fakeAdminAdapter->setErrorCode(ERROR_NONE);
        SwiftSchemaWriterImpl writer(adminAdapter, SwiftRpcChannelManagerPtr(), ThreadPoolPtr(), _topicInfo);
        SwiftWriterConfig config;
        config.topicName = "topic";
        config.schemaVersion = rightVersion;
        ec = writer.init(config, BlockPoolPtr());
        EXPECT_EQ(ERROR_NONE, ec);
    }

    { // use latest
        fakeAdminAdapter->setErrorCode(ERROR_NONE);
        SwiftSchemaWriterImpl writer(adminAdapter, SwiftRpcChannelManagerPtr(), ThreadPoolPtr(), _topicInfo);
        SwiftWriterConfig config;
        config.topicName = "topic";
        config.schemaVersion = 0;
        ec = writer.init(config, BlockPoolPtr());
        EXPECT_EQ(ERROR_NONE, ec);
    }
}

TEST_F(SwiftSchemaWriterImplTest, testGetSchemaWriter) {
    common::SchemaWriter *sw = NULL;
    FakeSwiftAdminAdapter *fakeAdminAdapter = new FakeSwiftAdminAdapter();
    SwiftAdminAdapterPtr adminAdapter(fakeAdminAdapter);
    SwiftSchemaWriterImpl writer(adminAdapter, SwiftRpcChannelManagerPtr(), ThreadPoolPtr(), _topicInfo);
    sw = writer.getSchemaWriter();
    EXPECT_TRUE(sw != NULL);
}

TEST_F(SwiftSchemaWriterImplTest, testWrite) {
    FakeSwiftAdminAdapter *fakeAdminAdapter = new FakeSwiftAdminAdapter();
    SwiftAdminAdapterPtr adminAdapter(fakeAdminAdapter);
    uint32_t version = 0;
    string schemaStr = R"json({"topic":"topic","fields":[{"name":"nid","type":"string"}]})json";
    RegisterSchemaResponse response;
    RegisterSchemaRequest request;
    request.set_topicname("topic");
    request.set_schema(schemaStr);
    fakeAdminAdapter->registerSchema(request, response);
    version = response.version();
    fakeAdminAdapter->setErrorCode(ERROR_NONE);
    SwiftSchemaWriterImpl writer(adminAdapter, SwiftRpcChannelManagerPtr(), ThreadPoolPtr(), _topicInfo);
    SwiftWriterConfig config;
    config.topicName = "topic";
    config.schemaVersion = version;
    ErrorCode ec = writer.init(config, BlockPoolPtr());
    EXPECT_EQ(ERROR_NONE, ec);

    //能写普通消息
    MessageInfo msgInfo = MessageInfoUtil::constructMsgInfo("data", 5, 0, 0, 1);
    EXPECT_EQ(ERROR_NONE, writer.write(msgInfo));

    //用schema writer构造消息，OK
    common::SchemaWriter *sw = writer.getSchemaWriter();
    EXPECT_TRUE(sw != NULL);
    sw->setField("nid", "100");
    EXPECT_FALSE(sw->_fieldMap["nid"].isNone);
    msgInfo = MessageInfoUtil::constructMsgInfo(sw->toString(), 5, 0, 0, 1);
    EXPECT_EQ(ERROR_NONE, writer.write(msgInfo));
    EXPECT_FALSE(sw->_fieldMap["nid"].isNone);
    sw->reset();
    EXPECT_TRUE(sw->_fieldMap["nid"].isNone);
}

} // namespace client
} // namespace swift
