#include "swift/client/SwiftSchemaReaderImpl.h"

#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/TimeUtility.h"
#include "swift/client/MessageInfo.h"
#include "swift/client/SwiftReaderConfig.h"
#include "swift/client/SwiftSinglePartitionReader.h"
#include "swift/client/SwiftTransportAdapter.h"
#include "swift/client/TransportClosure.h"
#include "swift/client/fake_client/FakeClientHelper.h"
#include "swift/client/fake_client/FakeSwiftAdminAdapter.h"
#include "swift/client/fake_client/FakeSwiftTransportAdapter.h"
#include "swift/client/fake_client/FakeSwiftTransportClient.h"
#include "swift/common/Common.h"
#include "swift/common/FieldSchema.h"
#include "swift/common/SchemaWriter.h"
#include "swift/network/SwiftAdminAdapter.h"
#include "swift/network/SwiftRpcChannelManager.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/util/Atomic.h"
#include "swift/util/MessageUtil.h"
#include "unittest/unittest.h"

namespace swift {
namespace common {
class SchemaReader;
} // namespace common
} // namespace swift

using namespace std;
using namespace autil;
using namespace swift::protocol;
using namespace swift::common;
using namespace swift::util;
using namespace swift::network;

namespace swift {
namespace client {

class SwiftSchemaReaderImplTest : public TESTBASE {
public:
    void setUp() { _topicInfo.set_partitioncount(1); }
    void tearDown() {}

private:
    TopicInfo _topicInfo;
};

TEST_F(SwiftSchemaReaderImplTest, testInit) {
    FakeSwiftAdminAdapter *fakeAdminAdapter = new FakeSwiftAdminAdapter();
    SwiftAdminAdapterPtr adminAdapter(fakeAdminAdapter);

    ErrorCode ec = ERROR_NONE;
    {
        SwiftSchemaReaderImpl reader(adminAdapter, SwiftRpcChannelManagerPtr(), _topicInfo);
        SwiftReaderConfig config;
        config.topicName = "topic";
        std::vector<uint32_t> pids;
        pids.push_back(2);
        config.readPartVec = pids;
        ec = reader.init(config);
        EXPECT_EQ(ERROR_CLIENT_INVALID_PARTITIONID, ec);
        EXPECT_EQ(0, reader._schemaReaderCache.size());
    }
    {
        SwiftSchemaReaderImpl reader(adminAdapter, SwiftRpcChannelManagerPtr(), _topicInfo);
        SwiftReaderConfig config;
        config.topicName = "topic";
        std::vector<uint32_t> pids;
        pids.push_back(0);
        config.readPartVec = pids;
        ec = reader.init(config);
        EXPECT_EQ(ERROR_ADMIN_SCHEMA_NOT_FOUND, ec);
        EXPECT_EQ(0, reader._schemaReaderCache.size());
    }
    {
        uint32_t wrongVersion = 100;
        FakeSwiftAdminAdapter::SchemaCacheItem item(12345, "{topic invald schema}");
        fakeAdminAdapter->_schemaCache.insert(std::make_pair(wrongVersion, item));
        SwiftSchemaReaderImpl reader(adminAdapter, SwiftRpcChannelManagerPtr(), _topicInfo);
        SwiftReaderConfig config;
        config.topicName = "topic";
        std::vector<uint32_t> pids;
        pids.push_back(0);
        config.readPartVec = pids;
        ec = reader.init(config);
        EXPECT_EQ(ERROR_CLIENT_LOAD_SCHEMA_FAIL, ec);
        EXPECT_EQ(0, reader._schemaReaderCache.size());
        fakeAdminAdapter->_schemaCache.clear();
    }
    {
        string schemaStr = R"json({"topic":"topic","fields":[{"name":"nid","type":"string"}]})json";
        RegisterSchemaResponse response;
        RegisterSchemaRequest request;
        request.set_topicname("topic");
        request.set_schema(schemaStr);
        fakeAdminAdapter->registerSchema(request, response);
        SwiftSchemaReaderImpl reader(adminAdapter, SwiftRpcChannelManagerPtr(), _topicInfo);
        SwiftReaderConfig config;
        config.topicName = "topic";
        std::vector<uint32_t> pids;
        pids.push_back(0);
        config.readPartVec = pids;
        ec = reader.init(config);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ(1, reader._schemaReaderCache.size());
    }
}

TEST_F(SwiftSchemaReaderImplTest, testScmRead) {
    FakeSwiftAdminAdapter *fakeAdminAdapter = new FakeSwiftAdminAdapter();
    SwiftAdminAdapterPtr adminAdapter(fakeAdminAdapter);

    string schemaStr = R"json({"topic":"topic","fields":[{"name":"nid","type":"string"}]})json";
    RegisterSchemaResponse response;
    RegisterSchemaRequest request;
    request.set_topicname("topic");
    request.set_schema(schemaStr);
    fakeAdminAdapter->registerSchema(request, response);

    SwiftSchemaReaderImpl reader(adminAdapter, SwiftRpcChannelManagerPtr(), _topicInfo);
    SwiftReaderConfig config;
    config.topicName = "topic";
    std::vector<uint32_t> pids;
    pids.push_back(0);
    config.readPartVec = pids;

    SwiftSinglePartitionReader *singleReader =
        new SwiftSinglePartitionReader(adminAdapter, SwiftRpcChannelManagerPtr(), 0, config);
    reader._readers.push_back(singleReader);
    reader._exceedLimitVec.push_back(false);
    reader._timestamps.push_back(-1);
    FakeSwiftTransportClient *fakeClient =
        FakeClientHelper::prepareTransportClientForReader("test", 0, true, 5, *singleReader);
    auto *minMsgIdAdapter =
        dynamic_cast<FakeSwiftTransportAdapter<TRT_GETMINMESSAGEIDBYTIME> *>(singleReader->_msgIdTransportAdapter);
    FakeSwiftTransportClient *minMsgIdClient = minMsgIdAdapter->getFakeTransportClient();

    Message msg;
    msg.set_msgid(0);
    msg.set_timestamp(100);
    msg.set_data("invalid data");
    fakeClient->_messageVec->push_back(msg);
    minMsgIdClient->_messageVec->push_back(msg);

    common::SchemaWriter sw;
    sw.loadSchema(schemaStr);
    sw.setField("nid", "12345");
    msg.set_msgid(1);
    msg.set_timestamp(101);
    const string &data = sw.toString();

    MessageInfo msgInfo;
    msgInfo.dataType = protocol::DATA_TYPE_SCHEMA;
    msgInfo.data = data;
    util::MessageUtil::writeDataHead(msgInfo);
    msg.set_data(msgInfo.data);
    fakeClient->_messageVec->push_back(msg);
    minMsgIdClient->_messageVec->push_back(msg);
    sw.reset();

    msg.set_msgid(2);
    msg.set_timestamp(102);
    msgInfo.dataType = protocol::DATA_TYPE_FIELDFILTER;
    msgInfo.data = "xxxx";
    util::MessageUtil::writeDataHead(msgInfo);
    msg.set_data(msgInfo.data);
    fakeClient->_messageVec->push_back(msg);
    minMsgIdClient->_messageVec->push_back(msg);

    int64_t timestamp;
    {
        ErrorCode ec = reader.read(timestamp, msg);
        EXPECT_EQ(101, timestamp);
        EXPECT_EQ(ERROR_CLIENT_SCHEMA_MSG_INVALID, ec);
    }
    {
        ErrorCode ec = reader.read(timestamp, msg);
        EXPECT_EQ(102, timestamp);
        EXPECT_EQ(ERROR_NONE, ec);
    }
    {
        ErrorCode ec = reader.read(timestamp, msg);
        EXPECT_EQ(103, timestamp);
        EXPECT_EQ(ERROR_NONE, ec);
    }
    {
        ErrorCode ec = reader.read(timestamp, msg);
        EXPECT_EQ(103, timestamp);
        EXPECT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, ec);
    }
    {
        Messages msgs;
        reader.seekByTimestamp(100, true);
        ErrorCode ec = reader.read(timestamp, msgs);
        EXPECT_EQ(ERROR_CLIENT_SCHEMA_MSG_INVALID, ec);
        EXPECT_EQ(3, msgs.msgs_size());
        EXPECT_EQ(protocol::DATA_TYPE_UNKNOWN, msgs.msgs(0).datatype());
        EXPECT_EQ(protocol::DATA_TYPE_SCHEMA, msgs.msgs(1).datatype());
        EXPECT_EQ(protocol::DATA_TYPE_FIELDFILTER, msgs.msgs(2).datatype());
    }
    {
        Messages msgs;
        reader.seekByTimestamp(101, true);
        ErrorCode ec = reader.read(timestamp, msgs);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ(2, msgs.msgs_size());
        EXPECT_EQ(protocol::DATA_TYPE_SCHEMA, msgs.msgs(0).datatype());
        EXPECT_EQ(protocol::DATA_TYPE_FIELDFILTER, msgs.msgs(1).datatype());
    }
    {
        Messages msgs;
        ErrorCode ec = reader.read(timestamp, msgs);
        EXPECT_EQ(ERROR_CLIENT_NO_MORE_MESSAGE, ec);
    }
}

TEST_F(SwiftSchemaReaderImplTest, testGetSchemaReader) {
    // SWIFT_ROOT_LOG_SETLEVEL(INFO);
    FakeSwiftAdminAdapter *fakeAdminAdapter = new FakeSwiftAdminAdapter();
    SwiftAdminAdapterPtr adminAdapter(fakeAdminAdapter);
    uint32_t rightVersion;
    string schemaStr = R"json({"topic":"topic","fields":[{"name":"nid","type":"string"}]})json";
    RegisterSchemaResponse response;
    RegisterSchemaRequest request;
    request.set_topicname("topic");
    request.set_schema(schemaStr);
    fakeAdminAdapter->registerSchema(request, response);
    rightVersion = response.version();

    SwiftSchemaReaderImpl reader(adminAdapter, SwiftRpcChannelManagerPtr(), _topicInfo);
    ErrorCode ec = ERROR_NONE;
    {
        string data;
        SchemaReader *sr = reader.getSchemaReader(data.c_str(), ec);
        EXPECT_TRUE(NULL == sr);
        EXPECT_EQ(ERROR_CLIENT_SCHEMA_READ_VERSION, ec);
    }
    { // data invalid
        string data("invalid data");
        SchemaReader *sr = reader.getSchemaReader(data.c_str(), ec);
        EXPECT_TRUE(NULL == sr);
        EXPECT_EQ(ERROR_ADMIN_SCHEMA_NOT_FOUND, ec);
    }
    { // get schema fail
        common::SchemaWriter sw;
        sw.loadSchema(schemaStr);
        sw.setField("nid", "12345");
        sw.setVersion(100);
        string data(sw.toString());
        data.insert(0, sizeof(HeadMeta), 0);
        SchemaReader *sr = reader.getSchemaReader(data.c_str(), ec);
        EXPECT_TRUE(NULL == sr);
        EXPECT_EQ(ERROR_ADMIN_SCHEMA_NOT_FOUND, ec);
    }
    {
        common::SchemaWriter sw;
        sw.loadSchema(schemaStr);
        sw.setVersion(rightVersion);
        sw.setField("nid", "12345");
        string data(sw.toString());
        data.insert(0, sizeof(HeadMeta), 0);
        SchemaReader *sr = reader.getSchemaReader(data.c_str(), ec);
        EXPECT_TRUE(NULL != sr);
        EXPECT_EQ(ERROR_NONE, ec);
    }
    { // get schema from cache
        Message msg;
        common::SchemaWriter sw;
        sw.loadSchema(schemaStr);
        sw.setVersion(rightVersion);
        sw.setField("nid", "12345");
        string data(sw.toString());
        data.insert(0, sizeof(HeadMeta), 0);
        fakeAdminAdapter->_schemaCache[rightVersion].second = "invalid schema";
        SchemaReader *sr = reader.getSchemaReader(data.c_str(), ec);
        EXPECT_TRUE(NULL != sr);
        EXPECT_EQ(ERROR_NONE, ec);
    }
    { // schema load error
        Message msg;
        common::SchemaWriter sw;
        sw.loadSchema(schemaStr);
        sw.setVersion(rightVersion);
        sw.setField("nid", "12345");
        string data(sw.toString());
        data.insert(0, sizeof(HeadMeta), 0);
        fakeAdminAdapter->_schemaCache[rightVersion].second = "invalid schema";
        for (auto &iter : reader._schemaReaderCache) {
            DELETE_AND_SET_NULL(iter.second);
        }
        reader._schemaReaderCache.clear();
        reader._schemaCache.clear();
        SchemaReader *sr = reader.getSchemaReader(data.c_str(), ec);
        EXPECT_TRUE(NULL == sr);
        EXPECT_EQ(ERROR_CLIENT_LOAD_SCHEMA_FAIL, ec);
    }
}

TEST_F(SwiftSchemaReaderImplTest, testGetSchema) {
    // SWIFT_ROOT_LOG_SETLEVEL(INFO);
    FakeSwiftAdminAdapter *fakeAdminAdapter = new FakeSwiftAdminAdapter();
    SwiftAdminAdapterPtr adminAdapter(fakeAdminAdapter);
    int32_t rightVersion;
    string schemaStr = R"json({"fields":[{"name":"nid","type":"string"}],"topic":"topic"})json";
    RegisterSchemaResponse response;
    RegisterSchemaRequest request;
    request.set_topicname("topic");
    request.set_schema(schemaStr);
    fakeAdminAdapter->registerSchema(request, response);
    rightVersion = response.version();

    SwiftSchemaReaderImpl reader(adminAdapter, SwiftRpcChannelManagerPtr(), _topicInfo);
    {
        int32_t retVersion = 0;
        string schema;
        ErrorCode ec = reader.getSchema(10, retVersion, schema);
        EXPECT_EQ(ERROR_ADMIN_SCHEMA_NOT_FOUND, ec);
        EXPECT_EQ(0, reader._schemaCache.size());
        EXPECT_EQ(0, retVersion);
        EXPECT_EQ(0, schema.size());
    }
    {
        int32_t retVersion = 0;
        string schema;
        ErrorCode ec = reader.getSchema(0, retVersion, schema);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ(1, reader._schemaCache.size());
        EXPECT_EQ(rightVersion, retVersion);
        EXPECT_EQ(schemaStr, schema);
    }
    {
        int32_t retVersion = 0;
        string schema;
        ErrorCode ec = reader.getSchema(rightVersion, retVersion, schema);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ(1, reader._schemaCache.size());
        EXPECT_EQ(rightVersion, retVersion);
        EXPECT_EQ(schemaStr, schema);
    }
}

} // namespace client
} // namespace swift
