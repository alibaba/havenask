#include <algorithm>
#include <cstdint>
#include <deque>
#include <iostream>
#include <map>
#include <memory>
#include <stddef.h>
#include <string>
#include <unistd.h>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/Span.h"
#include "autil/TimeUtility.h"
#include "swift/client/MessageInfo.h"
#include "swift/client/MessageWriteBuffer.h"
#include "swift/client/SingleSwiftWriter.h"
#include "swift/client/SwiftMultiReader.h"
#include "swift/client/SwiftReader.h"
#include "swift/client/SwiftReaderAdapter.h"
#include "swift/client/SwiftReaderConfig.h"
#include "swift/client/SwiftReaderImpl.h"
#include "swift/client/SwiftSchemaReaderImpl.h"
#include "swift/client/SwiftSchemaWriterImpl.h"
#include "swift/client/SwiftTransportClientCreator.h"
#include "swift/client/SwiftWriter.h"
#include "swift/client/SwiftWriterAdapter.h"
#include "swift/client/SwiftWriterConfig.h"
#include "swift/client/SwiftWriterImpl.h"
#include "swift/client/fake_client/FakeSwiftAdminAdapter.h"
#include "swift/client/fake_client/FakeSwiftClient.h"
#include "swift/common/FieldGroupReader.h"
#include "swift/common/FieldGroupWriter.h"
#include "swift/common/MessageInfo.h"
#include "swift/common/SchemaReader.h"
#include "swift/common/SchemaWriter.h"
#include "swift/network/SwiftAdminAdapter.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/FBMessageWriter.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "unittest/unittest.h"

using namespace std;
using namespace swift::protocol;
using namespace swift::common;
using namespace swift::network;

namespace swift {
namespace client {

class SwiftClientTest : public TESTBASE {
    void tearDown() {
        SwiftTransportClientCreator::FAKE_CLIENT_AUTO_ASYNC_CALL = false;
        SwiftTransportClientCreator::_fakeDataMap.clear();
    }
};

TEST_F(SwiftClientTest, testSetFbRecycleBuffer) {
    FakeSwiftClient client;
    EXPECT_EQ(FBMessageWriter::FB_RECYCLE_BUFFER_SIZE, FBMessageWriter::getRecycleBufferSize());
    ASSERT_TRUE(ERROR_NONE == client.initByConfigStr("zkPath=abc;fbWriterRecycleSizeKb=100"));
    EXPECT_EQ(100 * 1024, FBMessageWriter::getRecycleBufferSize());
}

TEST_F(SwiftClientTest, testCreateReader) {
    FakeSwiftClient client;
    ASSERT_TRUE(ERROR_NONE == client.initByConfigStr("zkPath=abc"));
    {
        // filter error
        ErrorInfo errorInfo;
        string configStr = "topicName=topic;from=2;to=1";

        SwiftReader *swiftReader = client.createReader(configStr, &errorInfo);
        EXPECT_TRUE(!swiftReader);
        swiftReader = client.createReader(configStr, &errorInfo);
        EXPECT_TRUE(!swiftReader);
        EXPECT_EQ(ERROR_CLIENT_INVALID_PARAMETERS, errorInfo.errcode());
    }
    {
        // not init
        ErrorInfo errorInfo;
        client.clear(); // reset client
        string configStr = "topicName=topic;from=1;to=2";
        SwiftReader *swiftReader = client.createReader(configStr, &errorInfo);
        EXPECT_TRUE(!swiftReader);
        swiftReader = client.createReader(configStr, &errorInfo);
        EXPECT_TRUE(!swiftReader);
        EXPECT_EQ(ERROR_CLIENT_NOT_INITED, errorInfo.errcode());
    }
    {
        // create success
        ErrorInfo errorInfo;
        ASSERT_TRUE(ERROR_NONE == client.initByConfigStr("zkPath=abc"));
        string configStr = "topicName=topic;from=1;to=2";
        SwiftReader *swiftReader = client.createReader(configStr, &errorInfo);
        EXPECT_TRUE(swiftReader);
        DELETE_AND_SET_NULL(swiftReader);
    }
    {
        SwiftAdminAdapterPtr adapter = client.getAdminAdapter();
        FakeSwiftAdminAdapter *fakeAdapter = dynamic_cast<FakeSwiftAdminAdapter *>(adapter.get());
        fakeAdapter->setErrorCode(ERROR_ADMIN_TOPIC_NOT_EXISTED);
        ErrorInfo errorInfo;
        string configStr = "topicName=topic;from=1;to=2";
        SwiftReader *swiftReader = client.createReader(configStr, &errorInfo);
        EXPECT_EQ(ERROR_ADMIN_TOPIC_NOT_EXISTED, errorInfo.errcode());
        EXPECT_TRUE(!swiftReader);
    }
    {
        // create with authority
        ErrorInfo errorInfo;
        ASSERT_TRUE(ERROR_NONE == client.initByConfigStr("zkPath=abc;username=xx;passwd=xx1"));
        string configStr = "topicName=topic;from=1;to=2";
        SwiftReader *swiftReader = client.createReader(configStr, &errorInfo);
        EXPECT_TRUE(swiftReader);
        SwiftReaderAdapter *adapter = dynamic_cast<SwiftReaderAdapter *>(swiftReader);
        EXPECT_TRUE(adapter);
        EXPECT_EQ("", adapter->_readerConfig.accessId);
        EXPECT_EQ("", adapter->_readerConfig.accessKey);
        DELETE_AND_SET_NULL(swiftReader);
    }
    {
        // create with authority
        ErrorInfo errorInfo;
        ASSERT_TRUE(ERROR_NONE == client.initByConfigStr("zkPath=abc;username=xx;passwd=xx1"));
        string configStr = "topicName=topic;from=1;to=2;accessId=my;accessKey=my1";
        SwiftReader *swiftReader = client.createReader(configStr, &errorInfo);
        EXPECT_TRUE(swiftReader);
        SwiftReaderAdapter *adapter = dynamic_cast<SwiftReaderAdapter *>(swiftReader);
        EXPECT_TRUE(adapter);
        EXPECT_EQ("my", adapter->_readerConfig.accessId);
        EXPECT_EQ("my1", adapter->_readerConfig.accessKey);
        DELETE_AND_SET_NULL(swiftReader);
    }
}

TEST_F(SwiftClientTest, testCreateMultiReader) {
    {
        // create fail, one fail
        ErrorInfo errorInfo;
        FakeSwiftClient client;
        ASSERT_TRUE(ERROR_NONE == client.initByConfigStr("zkPath=zk1"));
        string configStr = "topicName=topic1;zkPath=zk1;from=10;to=20##topicName=topic2;from=22;zkPath=zk2;to=222";
        SwiftReader *swiftReader = client.createReader(configStr, &errorInfo);
        EXPECT_FALSE(swiftReader);
    }
    {
        // create fail, one fail
        ErrorInfo errorInfo;
        FakeSwiftClient client;
        ASSERT_TRUE(ERROR_NONE == client.initByConfigStr("zkPath=zk1##zkPath=zk2"));
        string configStr = "topicName=topic1;zkPath=zk2;from=10;to=20##topicName=topic2;from=22;zkPath=zk3;to=222";
        SwiftReader *swiftReader = client.createReader(configStr, &errorInfo);
        EXPECT_FALSE(swiftReader);
    }
    {
        // create success, two
        ErrorInfo errorInfo;
        FakeSwiftClient client;
        ASSERT_TRUE(ERROR_NONE == client.initByConfigStr("zkPath=zk1"));
        string configStr = "topicName=topic1;zkPath=zk1;from=10;to=20##topicName=topic2;from=22;zkPath=zk1;to=222";
        SwiftReader *swiftReader = client.createReader(configStr, &errorInfo);
        EXPECT_TRUE(swiftReader);
        EXPECT_EQ(2, dynamic_cast<SwiftMultiReader *>(swiftReader)->size());
        EXPECT_EQ(SwiftMultiReader::RO_DEFAULT, dynamic_cast<SwiftMultiReader *>(swiftReader)->_readerOrder);
        DELETE_AND_SET_NULL(swiftReader);
    }
    {
        // create success, two, with authority
        ErrorInfo errorInfo;
        FakeSwiftClient client;
        ASSERT_TRUE(ERROR_NONE == client.initByConfigStr("zkPath=zk1;username=xx;passwd=xx1"));
        string configStr = "topicName=topic1;zkPath=zk1;from=10;to=20##topicName=topic2;from=22;zkPath=zk1;to=222";
        SwiftReader *swiftReader = client.createReader(configStr, &errorInfo);
        EXPECT_TRUE(swiftReader);
        SwiftMultiReader *multiReader = dynamic_cast<SwiftMultiReader *>(swiftReader);
        EXPECT_EQ(2, multiReader->size());
        for (auto adapter : multiReader->_readerAdapterVec) {
            EXPECT_TRUE(adapter);
            EXPECT_EQ("", adapter->_readerConfig.accessId);
            EXPECT_EQ("", adapter->_readerConfig.accessKey);
        }
        DELETE_AND_SET_NULL(swiftReader);
    }
    {
        // create success, two
        ErrorInfo errorInfo;
        FakeSwiftClient client;
        ASSERT_TRUE(ERROR_NONE == client.initByConfigStr("zkPath=zk1##zkPath=zk2;"));
        string configStr = "topicName=topic1;zkPath=zk2;from=10;to=20##topicName=topic2;from=22;zkPath=zk1;to=222";
        SwiftReader *swiftReader = client.createReader(configStr, &errorInfo);
        EXPECT_TRUE(swiftReader);
        EXPECT_EQ(2, dynamic_cast<SwiftMultiReader *>(swiftReader)->size());
        DELETE_AND_SET_NULL(swiftReader);
    }
    {
        // create success, two, with authority
        ErrorInfo errorInfo;
        FakeSwiftClient client;
        ASSERT_TRUE(ERROR_NONE ==
                    client.initByConfigStr("zkPath=zk1;username=xx1;passwd=xx11##zkPath=zk2;username=xx;passwd=xx1"));
        string configStr = "topicName=topic1;zkPath=zk2;from=10;to=20##topicName=topic2;from=22;zkPath=zk1;to=222";
        SwiftReader *swiftReader = client.createReader(configStr, &errorInfo);
        EXPECT_TRUE(swiftReader);
        SwiftMultiReader *multiReader = dynamic_cast<SwiftMultiReader *>(swiftReader);
        EXPECT_EQ(2, multiReader->size());
        auto adapter1 = multiReader->_readerAdapterVec[0];
        EXPECT_TRUE(adapter1);
        EXPECT_EQ("", adapter1->_readerConfig.accessId);
        EXPECT_EQ("", adapter1->_readerConfig.accessKey);
        auto adapter2 = multiReader->_readerAdapterVec[1];
        EXPECT_TRUE(adapter2);
        EXPECT_EQ("", adapter2->_readerConfig.accessId);
        EXPECT_EQ("", adapter2->_readerConfig.accessKey);
        DELETE_AND_SET_NULL(swiftReader);
    }
    {
        // create success, two, set read order first
        ErrorInfo errorInfo;
        FakeSwiftClient client;
        ASSERT_TRUE(ERROR_NONE == client.initByConfigStr("zkPath=zk1##zkPath=zk2"));
        string configStr = "topicName=topic1;zkPath=zk2;from=10;to=20;multiReaderOrder=sequence##topicName=topic2;from="
                           "22;zkPath=zk1;to=222";
        SwiftReader *swiftReader = client.createReader(configStr, &errorInfo);
        EXPECT_TRUE(swiftReader);
        EXPECT_EQ(2, dynamic_cast<SwiftMultiReader *>(swiftReader)->size());
        EXPECT_EQ(SwiftMultiReader::RO_SEQ, dynamic_cast<SwiftMultiReader *>(swiftReader)->_readerOrder);
        DELETE_AND_SET_NULL(swiftReader);
    }
    {
        // create success, two, set read order second
        ErrorInfo errorInfo;
        FakeSwiftClient client;
        ASSERT_TRUE(ERROR_NONE == client.initByConfigStr("zkPath=zk1##zkPath=zk2"));
        string configStr = "topicName=topic1;zkPath=zk2;from=10;to=20##topicName=topic2;from=22;zkPath=zk1;to=222;"
                           "multiReaderOrder=sequence";
        SwiftReader *swiftReader = client.createReader(configStr, &errorInfo);
        EXPECT_TRUE(swiftReader);
        EXPECT_EQ(2, dynamic_cast<SwiftMultiReader *>(swiftReader)->size());
        EXPECT_EQ(SwiftMultiReader::RO_SEQ, dynamic_cast<SwiftMultiReader *>(swiftReader)->_readerOrder);
        DELETE_AND_SET_NULL(swiftReader);
    }
    {
        // create fail, read order not illege
        ErrorInfo errorInfo;
        FakeSwiftClient client;
        ASSERT_TRUE(ERROR_NONE == client.initByConfigStr("zkPath=zk1##zkPath=zk2"));
        string configStr = "topicName=topic1;zkPath=zk2;from=10;to=20##topicName=topic2;from=22;zkPath=zk1;to=222;"
                           "multiReaderOrder=notExist";
        SwiftReader *swiftReader = client.createReader(configStr, &errorInfo);
        EXPECT_FALSE(swiftReader);
    }
    {
        // create fail, zk not right
        ErrorInfo errorInfo;
        FakeSwiftClient client;
        ASSERT_TRUE(ERROR_NONE == client.initByConfigStr("zkPath=zk1##zkPath=zk2"));
        string configStr = "topicName=topic1;zkPath=zk3;from=10;to=20##topicName=topic2;from=22;zkPath=zk4;to=222";
        SwiftReader *swiftReader = client.createReader(configStr, &errorInfo);
        EXPECT_FALSE(swiftReader);
    }
    {
        // create fail, multi reader config should have zk
        ErrorInfo errorInfo;
        FakeSwiftClient client;
        ASSERT_TRUE(ERROR_NONE == client.initByConfigStr("zkPath=zk1##zkPath=zk2"));
        string configStr = "topicName=topic1;from=10;to=20;zkPath=zk1##topicName=topic2;from=22;to=222";
        SwiftReader *swiftReader = client.createReader(configStr, &errorInfo);
        EXPECT_FALSE(swiftReader);
    }
    {
        // create success
        ErrorInfo errorInfo;
        FakeSwiftClient client;
        ASSERT_TRUE(ERROR_NONE == client.initByConfigStr("zkPath=zk2#zk3"));
        string configStr = "topicName=topic1#topic2;from=1;to=2";
        SwiftReader *swiftReader = client.createReader(configStr, &errorInfo);
        EXPECT_TRUE(swiftReader);
        DELETE_AND_SET_NULL(swiftReader);
    }
    {
        // create fail, cannot mix use ## and #
        ErrorInfo errorInfo;
        FakeSwiftClient client;
        ASSERT_TRUE(ERROR_NONE == client.initByConfigStr("zkPath=zk1##zkPath=zk2"));
        string configStr = "topicName=topic1#topic2;from=1;to=2";
        SwiftReader *swiftReader = client.createReader(configStr, &errorInfo);
        EXPECT_FALSE(swiftReader);
    }
    {
        // create fail, cannot mix use ## and #
        ErrorInfo errorInfo;
        FakeSwiftClient client;
        ASSERT_TRUE(ERROR_NONE == client.initByConfigStr("zkPath=zk1#zk2"));
        string configStr = "topicName=topic1##topicName=topic2;from=1;to=2";
        SwiftReader *swiftReader = client.createReader(configStr, &errorInfo);
        EXPECT_FALSE(swiftReader);
    }
    {
        // create fail, size not equal #
        ErrorInfo errorInfo;
        FakeSwiftClient client;
        ASSERT_TRUE(ERROR_NONE == client.initByConfigStr("zkPath=zk2#zk2"));
        string configStr = "topicName=topic1#topic2#topic3;from=1;to=2";
        SwiftReader *swiftReader = client.createReader(configStr, &errorInfo);
        EXPECT_FALSE(swiftReader);
    }
    {
        // create fail, size not equal #
        ErrorInfo errorInfo;
        FakeSwiftClient client;
        ASSERT_TRUE(ERROR_NONE == client.initByConfigStr("zkPath=zk2#zk2#zk3"));
        string configStr = "topicName=topic1#topic2;from=1;to=2";
        SwiftReader *swiftReader = client.createReader(configStr, &errorInfo);
        EXPECT_FALSE(swiftReader);
    }
}

TEST_F(SwiftClientTest, testInitMultiClient) {
    FakeSwiftClient client;
    ErrorInfo errorInfo;
    ASSERT_TRUE(ERROR_NONE != client.initByConfigStr(""));
    ASSERT_TRUE(ERROR_NONE != client.initByConfigStr("zkPath=abc##zkPath="));
    ASSERT_TRUE(ERROR_NONE == client.initByConfigStr("zkPath=abc##zkPath=abcd"));
    ASSERT_TRUE(client._adminAdapterMap.size() == 2);
    ASSERT_TRUE(ERROR_NONE == client.initByConfigStr("[{\"zkPath\":\"abc\"},{\"zkPath\":\"abcd\"}]"));
    ASSERT_TRUE(client._adminAdapterMap.size() == 2);
    ASSERT_TRUE(ERROR_NONE != client.initByConfigStr("[{\"zkPath\":\"\"},{\"zkPath\":\"abcd\"}]"));
}

TEST_F(SwiftClientTest, testCreateSingleWriter) {
    {
        FakeSwiftClient client;
        ASSERT_TRUE(ERROR_NONE == client.initByConfigStr("zkPath=abc;username=xx;passwd=xx1"));
        {
            ErrorInfo errorInfo;
            string configStr = "topicName";
            SwiftWriter *swiftWriter = client.createWriter(configStr, &errorInfo);
            EXPECT_EQ(ERROR_CLIENT_INVALID_PARAMETERS, errorInfo.errcode());
            EXPECT_TRUE(!swiftWriter);
        }
        {
            ErrorInfo errorInfo;
            string configStr = "";
            SwiftWriter *swiftWriter = client.createWriter(configStr, &errorInfo);
            EXPECT_EQ(ERROR_CLIENT_INVALID_PARAMETERS, errorInfo.errcode());
            EXPECT_TRUE(!swiftWriter);
        }
        {
            string configStr = "topicName=abc";
            ErrorInfo errorInfo;
            SwiftWriter *swiftWriter = client.createWriter(configStr, &errorInfo);
            EXPECT_EQ(ERROR_NONE, errorInfo.errcode());
            EXPECT_TRUE(swiftWriter);
            SwiftWriterAdapter *adapter = dynamic_cast<SwiftWriterAdapter *>(swiftWriter);
            EXPECT_TRUE(adapter);
            EXPECT_EQ("", adapter->_config.accessId);
            EXPECT_EQ("", adapter->_config.accessKey);
            EXPECT_EQ("abc", swiftWriter->getTopicName());
        }
        {
            string configStr = "topicName=abc;accessId=my;accessKey=my1";
            ErrorInfo errorInfo;
            SwiftWriter *swiftWriter = client.createWriter(configStr, &errorInfo);
            EXPECT_EQ(ERROR_NONE, errorInfo.errcode());
            EXPECT_TRUE(swiftWriter);
            EXPECT_EQ("abc", swiftWriter->getTopicName());
            SwiftWriterAdapter *adapter = dynamic_cast<SwiftWriterAdapter *>(swiftWriter);
            EXPECT_TRUE(adapter);
            EXPECT_EQ("my", adapter->_config.accessId);
            EXPECT_EQ("my1", adapter->_config.accessKey);
        }

        {
            SwiftAdminAdapterPtr adapter = client.getAdminAdapter();
            FakeSwiftAdminAdapter *fakeAdapter = dynamic_cast<FakeSwiftAdminAdapter *>(adapter.get());
            fakeAdapter->setErrorCode(ERROR_ADMIN_TOPIC_NOT_EXISTED);
            string configStr = "topicName=abc";
            ErrorInfo errorInfo;
            SwiftWriter *swiftWriter = client.createWriter(configStr, &errorInfo);
            EXPECT_EQ(ERROR_ADMIN_TOPIC_NOT_EXISTED, errorInfo.errcode());
            EXPECT_TRUE(!swiftWriter);
        }
    }
}

TEST_F(SwiftClientTest, testCreateMultiWriter) {
    FakeSwiftClient client;
    ASSERT_TRUE(ERROR_NONE == client.initByConfigStr("zkPath=b##zkPath=a"));
    {
        string configStr = "topicName=abc";
        ErrorInfo errorInfo;
        SwiftWriter *swiftWriter = client.createWriter(configStr, &errorInfo);
        EXPECT_EQ(ERROR_CLIENT_INVALID_PARAMETERS, errorInfo.errcode());
        EXPECT_TRUE(!swiftWriter);
    }
    {
        string configStr = "topicName=abc;zkPath=aa";
        ErrorInfo errorInfo;
        SwiftWriter *swiftWriter = client.createWriter(configStr, &errorInfo);
        EXPECT_EQ(ERROR_CLIENT_INVALID_PARAMETERS, errorInfo.errcode());
        EXPECT_TRUE(!swiftWriter);
    }
    {
        string configStr = "topicName=abc;zkPath=a";
        ErrorInfo errorInfo;
        SwiftWriter *swiftWriter = client.createWriter(configStr, &errorInfo);
        EXPECT_EQ(ERROR_NONE, errorInfo.errcode());
        EXPECT_TRUE(swiftWriter);
        SwiftWriterAdapter *writer = dynamic_cast<SwiftWriterAdapter *>(swiftWriter);
        ASSERT_TRUE(writer);
    }
    {
        string configStr = "topicName=abc;zkPath=a##topicName=abc;zkPath=b";
        ErrorInfo errorInfo;
        SwiftWriter *swiftWriter = client.createWriter(configStr, &errorInfo);
        EXPECT_EQ(ERROR_CLIENT_INVALID_PARAMETERS, errorInfo.errcode());
        EXPECT_FALSE(swiftWriter);
    }
    {
        string configStr = "{\"topicName\":\"abc\"}";
        ErrorInfo errorInfo;
        SwiftWriter *swiftWriter = client.createWriter(configStr, &errorInfo);
        EXPECT_EQ(ERROR_CLIENT_INVALID_PARAMETERS, errorInfo.errcode());
        EXPECT_TRUE(!swiftWriter);
    }
    {
        string configStr = "[{\"topicName\":\"abc\",\"zkPath\":\"abc\"}]";
        ErrorInfo errorInfo;
        SwiftWriter *swiftWriter = client.createWriter(configStr, &errorInfo);
        EXPECT_EQ(ERROR_CLIENT_INVALID_PARAMETERS, errorInfo.errcode());
        EXPECT_TRUE(!swiftWriter);
    }
    {
        string configStr = "[{\"topicName\":\"abc\",\"zkPath\":\"a\"},"
                           "{\"topicName\":\"abc\",\"zkPath\":\"b\"}]";
        ErrorInfo errorInfo;
        SwiftWriter *swiftWriter = client.createWriter(configStr, &errorInfo);
        EXPECT_EQ(ERROR_CLIENT_INVALID_PARAMETERS, errorInfo.errcode());
        EXPECT_FALSE(swiftWriter);
    }
    {
        string configStr = "[{\"topicName\":\"abc\",\"zkPath\":\"a\"}]";
        ErrorInfo errorInfo;
        SwiftWriter *swiftWriter = client.createWriter(configStr, &errorInfo);
        EXPECT_EQ(ERROR_NONE, errorInfo.errcode());
        EXPECT_TRUE(swiftWriter);
        SwiftWriterAdapter *writer = dynamic_cast<SwiftWriterAdapter *>(swiftWriter);
        ASSERT_TRUE(writer);
        DELETE_AND_SET_NULL(swiftWriter);
    }
}

// 测试带schema的读写客户端，先后写入两个schema版本的数据，然后用reader读出并解析出来
TEST_F(SwiftClientTest, testSchemaWriterReader) {
    FakeSwiftClient client;
    string topicName("topic_test");
    ASSERT_TRUE(ERROR_NONE == client.initByConfigStr("zkPath=abc;retryInterval=10;retryTime=1"));
    SwiftAdminAdapterPtr adminPtr = client.getAdminAdapter();
    string schemaStr1(
        R"json({"topic":"topic_test","fields":[{"name":"CMD","type":"string"},{"name":"timestamp","type":"string"},{"name":"price","type":"string"},{"name":"title","type":"string"},{"name":"phone_series","subFields":[{"name":"iphone","type":"string"},{"name":"huawei","type":"string"},{"name":"xiaomi","type":"string"}]},{"name":"image_path","type":"string"}]})json");
    string schemaStr2(
        R"json({"topic":"topic_test","fields":[{"name":"CMD","type":"string"},{"name":"nid","type":"string"},{"name":"available_shop","type":"string"}]})json");
    RegisterSchemaResponse response;
    RegisterSchemaRequest request;
    request.set_topicname(topicName);
    request.set_schema(schemaStr1);
    EXPECT_EQ(ERROR_NONE, adminPtr->registerSchema(request, response));
    int32_t version1 = response.version();
    sleep(1);
    request.set_schema(schemaStr2);
    EXPECT_EQ(ERROR_NONE, adminPtr->registerSchema(request, response));
    int32_t version2 = response.version();
    cout << version1 << " " << version2 << endl;

    { // normal writer
        ErrorInfo errorInfo;
        string configStr("topicName=" + topicName);
        SwiftWriter *writer = client.createWriter(configStr, &errorInfo);
        ASSERT_TRUE(writer);
        EXPECT_EQ(topicName, writer->getTopicName());
        EXPECT_EQ(ERROR_NONE, errorInfo.errcode());
        SwiftWriterAdapter *writerAdapter = dynamic_cast<SwiftWriterAdapter *>(writer);
        SwiftWriterImpl *impl = dynamic_cast<SwiftWriterImpl *>(writerAdapter->_writerImpl.get());
        SwiftSchemaWriterImpl *simpl = dynamic_cast<SwiftSchemaWriterImpl *>(writerAdapter->_writerImpl.get());
        EXPECT_FALSE(impl == NULL);
        EXPECT_TRUE(simpl == NULL);
        DELETE_AND_SET_NULL(writer);
    }

    { // schema writer fail, normal topic cannot have schema params
        ErrorInfo errorInfo;
        string configStr("topicName=" + topicName + ";schemaVersion=" + std::to_string(version1));
        SwiftWriter *writer = client.createWriter(configStr, &errorInfo);
        ASSERT_FALSE(writer);
        EXPECT_EQ(ERROR_CLIENT_INVALID_PARAMETERS, errorInfo.errcode());
    }

    uint32_t curTime = TimeUtility::currentTimeInSeconds();
    string cmd[3] = {"add", "update_fields", "delete"};

    { // schema writer, by schema version
        string configStr("topicName=" + topicName + ";schemaVersion=" + std::to_string(version1));
        FakeSwiftAdminAdapter *fakeAdminAdapter = dynamic_cast<FakeSwiftAdminAdapter *>(adminPtr.get());
        fakeAdminAdapter->setNeedSchema(true);
        ErrorInfo errorInfo;
        SwiftWriter *writer = client.createWriter(configStr, &errorInfo);
        ASSERT_TRUE(writer);
        EXPECT_EQ(topicName, writer->getTopicName());
        EXPECT_EQ(ERROR_NONE, errorInfo.errcode());
        SwiftWriterAdapter *simpl = dynamic_cast<SwiftWriterAdapter *>(writer);
        EXPECT_FALSE(simpl == NULL);
        SchemaWriter *sw = writer->getSchemaWriter();

        const uint16_t msgCount = 5;
        for (uint16_t index = 0; index < msgCount; index++) {
            EXPECT_TRUE(sw->setField("CMD", cmd[index % 3]));
            EXPECT_TRUE(sw->setField("timestamp", std::to_string(curTime + index)));
            EXPECT_TRUE(sw->setField("price", std::to_string(index)));
            EXPECT_TRUE(sw->setSubField("phone_series", "iphone", std::to_string(index) + "_mac"));
            EXPECT_TRUE(sw->setSubField("phone_series", "xiaomi", std::to_string(index) + "_redMi"));
            EXPECT_TRUE(sw->setField("phone_series", "mainse doc"));
            EXPECT_TRUE(sw->setField("title", std::to_string(index) + " doc"));
            MessageInfo msgInfo;
            msgInfo.dataType = protocol::DATA_TYPE_SCHEMA;
            const string &data = sw->toString();
            msgInfo.data = data;
            writer->write(msgInfo);
            sw->reset();
        }

        SingleSwiftWriter *singleWriter = simpl->_writerImpl->getSingleWriter(0);
        ASSERT_TRUE(singleWriter);
        EXPECT_EQ(msgCount, singleWriter->getUnsendMessageCount());
        EXPECT_EQ(msgCount, singleWriter->_writeBuffer._writeQueue.size());
        SchemaReader reader;
        EXPECT_TRUE(reader.loadSchema(schemaStr1));
        uint16_t index = 0;
        for (const MessageInfo *info : singleWriter->_writeBuffer._writeQueue) {
            const auto &fields = reader.parseFromString(info->data);
            // expect header
            int32_t vv = 0;
            EXPECT_TRUE(SchemaReader::readVersion(info->data.c_str(), vv));
            EXPECT_EQ(version1, vv);
            // expect body
            EXPECT_EQ(6, fields.size());
            EXPECT_EQ("CMD", fields[0].key.to_string());
            EXPECT_EQ(cmd[index % 3], fields[0].value.to_string());
            EXPECT_EQ("timestamp", fields[1].key.to_string());
            EXPECT_EQ(std::to_string(curTime + index), fields[1].value.to_string());
            EXPECT_EQ("price", fields[2].key.to_string());
            EXPECT_EQ(std::to_string(index), fields[2].value.to_string());
            EXPECT_EQ("title", fields[3].key.to_string());
            EXPECT_EQ(std::to_string(index) + " doc", fields[3].value.to_string());
            EXPECT_EQ("phone_series", fields[4].key.to_string());
            EXPECT_EQ(3, fields[4].subFields.size());
            EXPECT_EQ("iphone", fields[4].subFields[0].key.to_string());
            EXPECT_EQ(std::to_string(index) + "_mac", fields[4].subFields[0].value.to_string());
            EXPECT_EQ("huawei", fields[4].subFields[1].key.to_string());
            EXPECT_TRUE(fields[4].subFields[1].value.empty());
            EXPECT_EQ("xiaomi", fields[4].subFields[2].key.to_string());
            EXPECT_EQ(std::to_string(index) + "_redMi", fields[4].subFields[2].value.to_string());
            EXPECT_EQ("image_path", fields[5].key.to_string());
            EXPECT_TRUE(fields[5].value.empty());
            index++;
        }
        DELETE_AND_SET_NULL(writer);
    }

    { // schema writer
        FakeSwiftAdminAdapter *fakeAdminAdapter = dynamic_cast<FakeSwiftAdminAdapter *>(adminPtr.get());
        fakeAdminAdapter->setNeedSchema(true);
        ErrorInfo errorInfo;
        string configStr("topicName=" + topicName + ";schemaVersion=" + std::to_string(version2));
        SwiftWriter *writer = client.createWriter(configStr, &errorInfo);
        ASSERT_TRUE(writer);
        EXPECT_EQ(ERROR_NONE, errorInfo.errcode());
        SwiftWriterAdapter *simpl = dynamic_cast<SwiftWriterAdapter *>(writer);
        EXPECT_FALSE(simpl == NULL);
        SchemaWriter *sw = writer->getSchemaWriter();

        const uint16_t msgCount = 5;
        string cmd[3] = {"add", "update_fields", "delete"};
        for (uint16_t index = 5; index < msgCount + 5; index++) {
            EXPECT_TRUE(sw->setField("CMD", cmd[index % 3]));
            EXPECT_TRUE(sw->setField("nid", std::to_string(curTime + index)));
            EXPECT_TRUE(sw->setField("available_shop", std::to_string(index) + "_shop"));
            MessageInfo msgInfo;
            msgInfo.dataType = protocol::DATA_TYPE_SCHEMA;
            const string &data = sw->toString();
            msgInfo.data = data;
            writer->write(msgInfo);
            sw->reset();
        }

        SingleSwiftWriter *singleWriter = simpl->_writerImpl->getSingleWriter(0);
        ASSERT_TRUE(singleWriter);
        EXPECT_EQ(msgCount, singleWriter->getUnsendMessageCount());
        EXPECT_EQ(msgCount, singleWriter->_writeBuffer._writeQueue.size());
        SchemaReader reader;
        EXPECT_TRUE(reader.loadSchema(schemaStr2));
        uint16_t index = 5;
        for (const MessageInfo *info : singleWriter->_writeBuffer._writeQueue) {
            const auto &fields = reader.parseFromString(info->data);
            // expect version
            int32_t vv = 0;
            EXPECT_TRUE(SchemaReader::readVersion(info->data.c_str(), vv));
            EXPECT_EQ(version2, vv);
            // expect body
            EXPECT_EQ(3, fields.size());
            EXPECT_EQ("CMD", fields[0].key.to_string());
            EXPECT_EQ(cmd[index % 3], fields[0].value.to_string());
            EXPECT_EQ("nid", fields[1].key.to_string());
            EXPECT_EQ(std::to_string(curTime + index), fields[1].value.to_string());
            EXPECT_EQ("available_shop", fields[2].key.to_string());
            EXPECT_EQ(std::to_string(index) + "_shop", fields[2].value.to_string());
            index++;
        }
        DELETE_AND_SET_NULL(writer);
    }

    { // schema writer, use latest schema
        FakeSwiftAdminAdapter *fakeAdminAdapter = dynamic_cast<FakeSwiftAdminAdapter *>(adminPtr.get());
        fakeAdminAdapter->setNeedSchema(true);
        ErrorInfo errorInfo;
        string configStr("topicName=" + topicName + ";schemaVersion=0");
        SwiftWriter *writer = client.createWriter(configStr, &errorInfo);
        ASSERT_TRUE(writer);
        EXPECT_EQ(ERROR_NONE, errorInfo.errcode());
        SwiftWriterAdapter *simpl = dynamic_cast<SwiftWriterAdapter *>(writer);
        EXPECT_FALSE(simpl == NULL);
        SchemaWriter *sw = writer->getSchemaWriter();

        const uint16_t msgCount = 5;
        string cmd[3] = {"add", "update_fields", "delete"};
        for (uint16_t index = 10; index < msgCount + 10; index++) {
            EXPECT_TRUE(sw->setField("CMD", cmd[index % 3]));
            EXPECT_TRUE(sw->setField("nid", std::to_string(curTime + index)));
            EXPECT_TRUE(sw->setField("available_shop", std::to_string(index) + "_shop"));

            MessageInfo msgInfo;
            msgInfo.dataType = protocol::DATA_TYPE_SCHEMA;
            const string &data = sw->toString();
            msgInfo.data = data;
            writer->write(msgInfo);
            sw->reset();
        }

        SingleSwiftWriter *singleWriter = simpl->_writerImpl->getSingleWriter(0);
        ASSERT_TRUE(singleWriter);
        EXPECT_EQ(msgCount, singleWriter->getUnsendMessageCount());
        EXPECT_EQ(msgCount, singleWriter->_writeBuffer._writeQueue.size());
        SchemaReader reader;
        EXPECT_TRUE(reader.loadSchema(schemaStr2));
        uint16_t index = 10;
        for (const MessageInfo *info : singleWriter->_writeBuffer._writeQueue) {
            const auto &fields = reader.parseFromString(info->data);
            // expect version
            int32_t vv = 0;
            EXPECT_TRUE(SchemaReader::readVersion(info->data.c_str(), vv));
            EXPECT_EQ(version2, vv);
            // expect body
            EXPECT_EQ(3, fields.size());
            EXPECT_EQ("CMD", fields[0].key.to_string());
            EXPECT_EQ(cmd[index % 3], fields[0].value.to_string());
            EXPECT_EQ("nid", fields[1].key.to_string());
            EXPECT_EQ(std::to_string(curTime + index), fields[1].value.to_string());
            EXPECT_EQ("available_shop", fields[2].key.to_string());
            EXPECT_EQ(std::to_string(index) + "_shop", fields[2].value.to_string());
            index++;
        }
        DELETE_AND_SET_NULL(writer);
    }
    { // schema reader
        // SWIFT_ROOT_LOG_SETLEVEL(INFO);
        string configStr("topicName=" + topicName);
        SwiftTransportClientCreator::FAKE_CLIENT_AUTO_ASYNC_CALL = true;
        ErrorInfo errorInfo;
        SwiftReader *reader = client.createReader(configStr, &errorInfo);
        EXPECT_TRUE(reader);
        EXPECT_EQ(ERROR_NONE, errorInfo.errcode());

        SwiftReaderAdapter *rAdapter = dynamic_cast<SwiftReaderAdapter *>(reader);
        SwiftSchemaReaderImpl *simpl = dynamic_cast<SwiftSchemaReaderImpl *>(rAdapter->getSwiftReader().get());
        EXPECT_TRUE(simpl != NULL);

        ErrorCode ec = ERROR_NONE;
        uint32_t index = 0;
        while (ec == ERROR_NONE) {
            Message msg;
            ec = reader->read(msg);
            if (ec != ERROR_NONE) {
                break;
            }
            EXPECT_EQ(protocol::DATA_TYPE_SCHEMA, msg.datatype());
            SchemaReader *sr = reader->getSchemaReader(msg.data().c_str(), ec);
            EXPECT_TRUE(sr != NULL);
            const vector<SchemaReaderField> &fields = sr->parseFromString(msg.data());
            if (index < 5) {
                // expect version
                int32_t vv = 0;
                EXPECT_TRUE(SchemaReader::readVersion(msg.data().c_str(), vv));
                EXPECT_EQ(version1, vv);
                // expect body
                EXPECT_EQ(6, fields.size());
                EXPECT_EQ("CMD", fields[0].key.to_string());
                EXPECT_EQ(cmd[index % 3], fields[0].value.to_string());
                EXPECT_EQ("timestamp", fields[1].key.to_string());
                EXPECT_EQ(std::to_string(curTime + index), fields[1].value.to_string());
                EXPECT_EQ("price", fields[2].key.to_string());
                EXPECT_EQ(std::to_string(index), fields[2].value.to_string());
                EXPECT_EQ("title", fields[3].key.to_string());
                EXPECT_EQ(std::to_string(index) + " doc", fields[3].value.to_string());
                EXPECT_EQ("phone_series", fields[4].key.to_string());
                EXPECT_EQ(3, fields[4].subFields.size());
                EXPECT_EQ("iphone", fields[4].subFields[0].key.to_string());
                EXPECT_EQ(std::to_string(index) + "_mac", fields[4].subFields[0].value.to_string());
                EXPECT_EQ("huawei", fields[4].subFields[1].key.to_string());
                EXPECT_TRUE(fields[4].subFields[1].value.empty());
                EXPECT_EQ("xiaomi", fields[4].subFields[2].key.to_string());
                EXPECT_EQ(std::to_string(index) + "_redMi", fields[4].subFields[2].value.to_string());
                EXPECT_EQ("image_path", fields[5].key.to_string());
                EXPECT_TRUE(fields[5].value.empty());
            } else {
                // expect version
                int32_t vv = 0;
                EXPECT_TRUE(SchemaReader::readVersion(msg.data().c_str(), vv));
                EXPECT_EQ(version2, vv);
                // expect body
                EXPECT_EQ(3, fields.size());
                EXPECT_EQ("CMD", fields[0].key.to_string());
                EXPECT_EQ(cmd[index % 3], fields[0].value.to_string());
                EXPECT_EQ("nid", fields[1].key.to_string());
                EXPECT_EQ(std::to_string(curTime + index), fields[1].value.to_string());
                EXPECT_EQ("available_shop", fields[2].key.to_string());
                EXPECT_EQ(std::to_string(index) + "_shop", fields[2].value.to_string());
            }
            ++index;
        }
        EXPECT_EQ(ec, ERROR_CLIENT_NO_MORE_MESSAGE);
        EXPECT_EQ(15, index);
        DELETE_AND_SET_NULL(reader);
    }
}

void printFields(const vector<SchemaReaderField> &fields) {
    for (const auto &field : fields) {
        if (0 != field.subFields.size()) {
            cout << field.key.to_string() << " {" << endl;
            for (const auto &sub : field.subFields) {
                cout << sub.key.to_string() << " = " << sub.value.to_string() << endl;
            }
            cout << "}" << endl;
        } else {
            cout << field.key.to_string() << " = " << field.value.to_string() << endl;
        }
    }
    cout << endl;
}

// 测试带schema的读写客户端，交叉写入多个schema版本的数据，
// 其中有两个schema是解析数据的时候动态去admin取到的
TEST_F(SwiftClientTest, testSchemaWriterReaderCross) {
    // SWIFT_ROOT_LOG_SETLEVEL(INFO);
    FakeSwiftClient client;
    ASSERT_TRUE(ERROR_NONE == client.initByConfigStr("zkPath=abc"));
    SwiftAdminAdapterPtr adminPtr = client.getAdminAdapter();

    vector<string> schemaStrs;
    vector<int32_t> versions;
    schemaStrs.reserve(3);
    schemaStrs.push_back(
        R"json({"topic":"topic2","fields":[{"name":"CMD","type":"string"},{"name":"timestamp","type":"string"},{"name":"phone_series","subFields":[{"name":"iphone","type":"string"},{"name":"xiaomi","type":"string"}]}]})json");
    schemaStrs.push_back(
        R"json({"topic":"topic2","fields":[{"name":"CMD","type":"string"},{"name":"nid","type":"string"},{"name":"available_shop","type":"string"}]})json");
    schemaStrs.push_back(
        R"json({"topic":"topic2","fields":[{"name":"CMD","type":"string"},{"name":"shop_id","type":"string"}]})json");
    versions.resize(3);
    RegisterSchemaResponse response;
    RegisterSchemaRequest request;
    request.set_topicname("topic2");
    for (int index = 0; index < 3; ++index) {
        request.set_schema(schemaStrs[index]);
        EXPECT_EQ(ERROR_NONE, adminPtr->registerSchema(request, response));
        versions[index] = response.version();
    }

    uint32_t curTime = TimeUtility::currentTimeInSeconds();
    string cmd[3] = {"add", "update_fields", "delete"};

    { // schema writer
        SwiftTransportClientCreator::FAKE_CLIENT_AUTO_ASYNC_CALL = true;
        FakeSwiftAdminAdapter *fakeAdminAdapter = dynamic_cast<FakeSwiftAdminAdapter *>(adminPtr.get());
        fakeAdminAdapter->setNeedSchema(true);
        ErrorInfo errorInfo;
        vector<SwiftWriter *> writers;
        for (uint16_t index = 0; index < 3; ++index) {
            string configStr("topicName=topic2;mode=sync;schemaVersion=" + std::to_string(versions[index]));
            SwiftWriter *writer = client.createWriter(configStr, &errorInfo);
            ASSERT_TRUE(writer);
            EXPECT_EQ(ERROR_NONE, errorInfo.errcode());
            writers.push_back(writer);
        }
        const uint16_t msgCount = 10;
        for (uint16_t index = 0; index < msgCount; index++) {
            SwiftWriter *writer = writers[index % 3];
            SchemaWriter *sw = writer->getSchemaWriter();
            EXPECT_TRUE(sw->setField("CMD", cmd[index % 3]));
            if (0 == index % 3) {
                EXPECT_TRUE(sw->setField("timestamp", std::to_string(curTime + index)));
                EXPECT_TRUE(sw->setSubField("phone_series", "iphone", std::to_string(index) + "_mac"));
                EXPECT_TRUE(sw->setSubField("phone_series", "xiaomi", std::to_string(index) + "_redMi"));
            } else if (1 == index % 3) {
                EXPECT_TRUE(sw->setField("nid", std::to_string(index)));
                EXPECT_TRUE(sw->setField("available_shop", std::to_string(index) + "_shop"));
            } else if (2 == index % 3) {
                EXPECT_TRUE(sw->setField("shop_id", "222_shopid"));
            } else {
                // impossible;
            }
            MessageInfo msgInfo;
            msgInfo.dataType = protocol::DATA_TYPE_SCHEMA;
            const string &data = sw->toString();
            msgInfo.data = data;
            writer->write(msgInfo);
            sw->reset();
        }
        for (uint16_t index = 0; index < 3; ++index) {
            DELETE_AND_SET_NULL(writers[index]);
        }
    }
    // SWIFT_ROOT_LOG_SETLEVEL(INFO);
    { // schema reader
        SwiftTransportClientCreator::FAKE_CLIENT_AUTO_ASYNC_CALL = true;
        ErrorInfo errorInfo;
        string configStr("topicName=topic2");
        SwiftReader *reader = client.createReader(configStr, &errorInfo);
        EXPECT_TRUE(reader);
        EXPECT_EQ(ERROR_NONE, errorInfo.errcode());

        ErrorCode ec = ERROR_NONE;
        uint32_t index = 0;
        while (ec == ERROR_NONE) {
            Message msg;
            ec = reader->read(msg);
            if (ec != ERROR_NONE) {
                break;
            }
            EXPECT_EQ(protocol::DATA_TYPE_SCHEMA, msg.datatype());
            SchemaReader *sr = reader->getSchemaReader(msg.data().c_str(), ec);
            EXPECT_TRUE(sr != NULL);
            const vector<SchemaReaderField> &fields = sr->parseFromString(msg.data());
            // printFields(fields);

            // expect version
            int32_t vv = 0;
            EXPECT_TRUE(SchemaReader::readVersion(msg.data().c_str(), vv));
            EXPECT_EQ(versions[index % 3], vv);

            // expect body
            EXPECT_EQ("CMD", fields[0].key.to_string());
            EXPECT_EQ(cmd[index % 3], fields[0].value.to_string());
            if (0 == index % 3) {
                EXPECT_EQ(3, fields.size());
                EXPECT_EQ("timestamp", fields[1].key.to_string());
                EXPECT_EQ(std::to_string(curTime + index), fields[1].value.to_string());
                EXPECT_EQ("phone_series", fields[2].key.to_string());
                EXPECT_EQ(2, fields[2].subFields.size());
                EXPECT_EQ("iphone", fields[2].subFields[0].key.to_string());
                EXPECT_EQ(std::to_string(index) + "_mac", fields[2].subFields[0].value.to_string());
                EXPECT_EQ("xiaomi", fields[2].subFields[1].key.to_string());
                EXPECT_EQ(std::to_string(index) + "_redMi", fields[2].subFields[1].value.to_string());
            } else if (1 == index % 3) {
                EXPECT_EQ(3, fields.size());
                EXPECT_EQ("nid", fields[1].key.to_string());
                EXPECT_EQ(std::to_string(index), fields[1].value.to_string());
                EXPECT_EQ("available_shop", fields[2].key.to_string());
                EXPECT_EQ(std::to_string(index) + "_shop", fields[2].value.to_string());
            } else if (2 == index % 3) {
                EXPECT_EQ(2, fields.size());
                EXPECT_EQ("shop_id", fields[1].key.to_string());
                EXPECT_EQ("222_shopid", fields[1].value.to_string());
            } else {
                // impossible;
            }

            ++index;
        }
        EXPECT_EQ(ec, ERROR_CLIENT_NO_MORE_MESSAGE);
        EXPECT_EQ(10, index);
        DELETE_AND_SET_NULL(reader);
    }
}

TEST_F(SwiftClientTest, testSchemaRWFilter) {
    FakeSwiftClient client;
    string topicName("topic_RWFilter");
    ASSERT_TRUE(ERROR_NONE == client.initByConfigStr("zkPath=abc"));
    SwiftAdminAdapterPtr adminPtr = client.getAdminAdapter();
    FakeSwiftAdminAdapter *fakeAdminAdapter = dynamic_cast<FakeSwiftAdminAdapter *>(adminPtr.get());
    fakeAdminAdapter->setNeedSchema(true);
    // SWIFT_ROOT_LOG_SETLEVEL(INFO);
    { // field group writer
        ErrorInfo errorInfo;
        string configStr("topicName=" + topicName);
        SwiftWriter *writer = client.createWriter(configStr, &errorInfo);
        ASSERT_TRUE(writer);
        EXPECT_EQ(ERROR_NONE, errorInfo.errcode());
        SwiftWriterAdapter *writerAdapter = dynamic_cast<SwiftWriterAdapter *>(writer);
        SwiftWriterImpl *impl = dynamic_cast<SwiftWriterImpl *>(writerAdapter->_writerImpl.get());
        SwiftSchemaWriterImpl *simpl = dynamic_cast<SwiftSchemaWriterImpl *>(writerAdapter->_writerImpl.get());
        EXPECT_FALSE(impl == NULL);
        EXPECT_FALSE(simpl == NULL);
        FieldGroupWriter fg;
        for (int i = 0; i < 5; i++) {
            fg.addProductionField("hh_field_" + std::to_string(i), "hh_value_" + std::to_string(i), true);
            MessageInfo msgInfo;
            msgInfo.dataType = protocol::DATA_TYPE_FIELDFILTER;
            const string &data = fg.toString();
            msgInfo.data = data;
            writer->write(msgInfo);
            fg.reset();
        }
        DELETE_AND_SET_NULL(writer);
    }

    string schemaStr(
        R"json({"topic":"topic_RWFilter","fields":[{"name":"CMD","type":"string"},{"name":"nid","type":"string"}]})json");
    RegisterSchemaResponse response;
    RegisterSchemaRequest request;
    request.set_topicname(topicName);
    request.set_schema(schemaStr);
    EXPECT_EQ(ERROR_NONE, adminPtr->registerSchema(request, response));
    int32_t version = response.version();
    string cmd[3] = {"add", "update_fields", "delete"};

    { // schema writer, by schema version
        string configStr("topicName=" + topicName + ";schemaVersion=" + std::to_string(version));
        ErrorInfo errorInfo;
        SwiftWriter *writer = client.createWriter(configStr, &errorInfo);
        ASSERT_TRUE(writer);
        EXPECT_EQ(ERROR_NONE, errorInfo.errcode());
        SwiftWriterAdapter *simpl = dynamic_cast<SwiftWriterAdapter *>(writer);
        EXPECT_FALSE(simpl == NULL);
        SchemaWriter *sw = writer->getSchemaWriter();

        FieldGroupWriter fgw;
        for (uint16_t index = 5; index < 10;) {
            {
                EXPECT_TRUE(sw->setField("CMD", cmd[index % 3]));
                EXPECT_TRUE(sw->setField("nid", std::to_string(index)));
                MessageInfo msgInfo;
                msgInfo.dataType = protocol::DATA_TYPE_SCHEMA;
                const string &data = sw->toString();
                msgInfo.data = data;
                writer->write(msgInfo);
                sw->reset();
            }
            ++index;
            {
                fgw.addProductionField("aa_field_" + std::to_string(index), "aa_value_" + std::to_string(index), false);
                fgw.addProductionField("bb_field_" + std::to_string(index), "bb_value_" + std::to_string(index), true);
                MessageInfo msgInfo;
                msgInfo.dataType = protocol::DATA_TYPE_FIELDFILTER;
                const string &data = fgw.toString();
                msgInfo.data = data;
                writer->write(msgInfo);
                fgw.reset();
            }
            ++index;
        }
        DELETE_AND_SET_NULL(writer);
    }

    { // schema reader
        SwiftTransportClientCreator::FAKE_CLIENT_AUTO_ASYNC_CALL = true;
        string configStr("topicName=" + topicName);
        ErrorInfo errorInfo;
        SwiftReader *reader = client.createReader(configStr, &errorInfo);
        EXPECT_TRUE(reader);
        EXPECT_EQ(ERROR_NONE, errorInfo.errcode());

        SwiftReaderAdapter *rAdapter = dynamic_cast<SwiftReaderAdapter *>(reader);
        SwiftSchemaReaderImpl *simpl = dynamic_cast<SwiftSchemaReaderImpl *>(rAdapter->getSwiftReader().get());
        EXPECT_TRUE(simpl != NULL);

        ErrorCode ec = ERROR_NONE;
        int index = 0;
        while (true) {
            Message msg;
            ec = reader->read(msg);
            if (ERROR_CLIENT_NO_MORE_MESSAGE == ec) {
                break;
            }
            if (protocol::DATA_TYPE_FIELDFILTER == msg.datatype()) {
                FieldGroupReader fgr;
                fgr.fromProductionString(msg.data());
                for (size_t i = 0; i < fgr.getFieldSize(); ++i) {
                    const Field *field = fgr.getField(i);
                    cout << field->name.to_string() << " = " << field->value.to_string() << " [" << field->isUpdated
                         << "]" << endl;
                }
                if (index < 5) {
                    EXPECT_EQ(1, fgr.getFieldSize());
                    const Field *field = fgr.getField(0);
                    EXPECT_EQ("hh_field_" + std::to_string(index), field->name.to_string());
                    EXPECT_EQ("hh_value_" + std::to_string(index), field->value.to_string());
                    EXPECT_TRUE(field->isUpdated);
                } else {
                    EXPECT_EQ(2, fgr.getFieldSize());
                    const Field *field1 = fgr.getField(0);
                    EXPECT_EQ("aa_field_" + std::to_string(index), field1->name.to_string());
                    EXPECT_EQ("aa_value_" + std::to_string(index), field1->value.to_string());
                    EXPECT_FALSE(field1->isUpdated);
                    const Field *field2 = fgr.getField(1);
                    EXPECT_EQ("bb_field_" + std::to_string(index), field2->name.to_string());
                    EXPECT_EQ("bb_value_" + std::to_string(index), field2->value.to_string());
                    EXPECT_TRUE(field2->isUpdated);
                }
            } else if (protocol::DATA_TYPE_SCHEMA == msg.datatype()) {
                SchemaReader *sr = reader->getSchemaReader(msg.data().c_str(), ec);
                EXPECT_TRUE(sr != NULL);
                const vector<SchemaReaderField> &fields = sr->parseFromString(msg.data());
                printFields(fields);

                EXPECT_EQ(2, fields.size());
                EXPECT_EQ("CMD", fields[0].key.to_string());
                EXPECT_EQ(cmd[index % 3], fields[0].value.to_string());
                EXPECT_EQ("nid", fields[1].key.to_string());
                EXPECT_EQ(std::to_string(index), fields[1].value.to_string());
            } else {
                cout << msg.ShortDebugString() << endl;
                EXPECT_TRUE(false);
            }
            ++index;
        }
        EXPECT_EQ(11, index);
        EXPECT_EQ(ec, ERROR_CLIENT_NO_MORE_MESSAGE);
        DELETE_AND_SET_NULL(reader);
    }
}

} // namespace client
} // namespace swift
