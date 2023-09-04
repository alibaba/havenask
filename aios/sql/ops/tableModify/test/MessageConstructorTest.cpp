#include "sql/ops/tableModify/MessageConstructor.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "autil/HashFunctionBase.h"
#include "autil/RoutingHashFunction.h"
#include "autil/Span.h"
#include "autil/legacy/RapidJsonCommon.h"
#include "autil/mem_pool/Pool.h"
#include "navi/common.h"
#include "navi/log/NaviLoggerProvider.h"
#include "sql/common/TableDistribution.h"
#include "sql/common/common.h"
#include "sql/ops/test/OpTestBase.h"
#include "swift/common/Common.h"
#include "swift/common/FieldGroupReader.h"
#include "unittest/unittest.h"

using namespace autil;
using namespace std;
using namespace navi;
using namespace swift::common;

namespace sql {

class MessageConstructorTest : public OpTestBase {
public:
    void setUp();
    void tearDown();

private:
    autil::mem_pool::Pool _pool;
};

void MessageConstructorTest::setUp() {}

void MessageConstructorTest::tearDown() {}

void checkKV(const FieldGroupReader &reader, const string &key, const string &expect) {
    auto *field = reader.getField(key);
    ASSERT_NE(nullptr, field);
    ASSERT_EQ(expect, field->value.to_string());
    ASSERT_TRUE(field->isUpdated);
}

TEST_F(MessageConstructorTest, testGetHashStrVec) {
    string inputJsonStr = R"json({
    "$modify_time" : "1657274727",
    "$title" : "hello world",
    "$buyer_id" : 12,
    "$flag" : true
})json";
    AutilPoolAllocator allocator(&_pool);
    SimpleDocument modifyExprsDoc(&allocator);
    modifyExprsDoc.Parse(inputJsonStr.c_str());
    vector<string> hashStrVec;
    {
        // hashFields not exist
        NaviLoggerProvider provider("WARN");
        vector<string> hashFields = {"not_exist", "buyer_id"};
        ASSERT_FALSE(MessageConstructor::getHashStrVec(modifyExprsDoc, hashFields, hashStrVec));
        auto traces = provider.getTrace("");
        CHECK_TRACE_COUNT(1, "hash field [$not_exist] not exist.", traces);
    }

    {
        // hash value failed
        NaviLoggerProvider provider("WARN");
        vector<string> hashFields = {"flag"};
        ASSERT_FALSE(MessageConstructor::getHashStrVec(modifyExprsDoc, hashFields, hashStrVec));
        auto traces = provider.getTrace("");
        CHECK_TRACE_COUNT(1, "parse hash value failed. field [$flag], value [true]", traces);
    }

    {
        // success
        NaviLoggerProvider provider("WARN");
        vector<string> hashFields = {"title", "buyer_id"};
        vector<string> hashStrVec;
        ASSERT_TRUE(MessageConstructor::getHashStrVec(modifyExprsDoc, hashFields, hashStrVec));
        ASSERT_EQ(2, hashStrVec.size());
        ASSERT_EQ("hello world", hashStrVec[0]);
        ASSERT_EQ("12", hashStrVec[1]);
    }
}

TEST_F(MessageConstructorTest, testCreateHashFunc) {
    {
        // validate failed
        NaviLoggerProvider provider("WARN");
        HashMode hashMode;
        hashMode._hashFunction = "HASH";
        ASSERT_FALSE(MessageConstructor::createHashFunc(hashMode));
        auto traces = provider.getTrace("");
        CHECK_TRACE_COUNT(1, "validate hash mode failed.", traces);
    }
    {
        // hash func failed
        NaviLoggerProvider provider("WARN");
        HashMode hashMode;
        hashMode._hashFields.push_back("buyer_id");
        hashMode._hashFunction = "NOT_EXIST";
        ASSERT_FALSE(MessageConstructor::createHashFunc(hashMode));
        auto traces = provider.getTrace("");
        CHECK_TRACE_COUNT(1, "invalid hash function", traces);
    }
    {
        HashMode hashMode;
        hashMode._hashFields.push_back("buyer_id");
        hashMode._hashFunction = "HASH";
        ASSERT_TRUE(MessageConstructor::createHashFunc(hashMode));
    }
    {
        HashMode hashMode;
        hashMode._hashFields = {"buyer_id", "pk"};
        hashMode._hashFunction = "ROUTING_HASH";
        auto func = MessageConstructor::createHashFunc(hashMode);
        ASSERT_TRUE(func);
        auto routeFunc = dynamic_cast<RoutingHashFunction *>(func.get());
        ASSERT_TRUE(routeFunc);
    }
}

TEST_F(MessageConstructorTest, testGenPayloadMap) {
    string pkField = "buyer_id";
    vector<map<string, string>> hashVals = {{{"buyer_id", "12"}}};
    map<string, uint16_t> payloadMap;
    {
        NaviLoggerProvider provider("WARN");
        HashMode hashMode;
        ASSERT_FALSE(MessageConstructor::genPayloadMap(hashMode, pkField, {}, payloadMap));
        auto traces = provider.getTrace("");
        CHECK_TRACE_COUNT(1, "hash values map is empty.", traces);
    }
    {
        NaviLoggerProvider provider("WARN");
        HashMode hashMode;
        ASSERT_FALSE(MessageConstructor::genPayloadMap(hashMode, pkField, hashVals, payloadMap));
        auto traces = provider.getTrace("");
        CHECK_TRACE_COUNT(1, "validate hash mode failed.", traces);
    }
    {
        NaviLoggerProvider provider("WARN");
        HashMode hashMode;
        hashMode._hashFields = {"not_exist"};
        hashMode._hashFunction = "HASH";
        ASSERT_FALSE(MessageConstructor::genPayloadMap(hashMode, pkField, hashVals, payloadMap));
        auto traces = provider.getTrace("");
        CHECK_TRACE_COUNT(1, "hash field [not_exist] not found or is empty.", traces);
    }
    {
        NaviLoggerProvider provider("WARN");
        HashMode hashMode;
        string pkField = "not_exist";
        hashMode._hashFields = {"buyer_id"};
        hashMode._hashFunction = "HASH";
        ASSERT_FALSE(MessageConstructor::genPayloadMap(hashMode, pkField, hashVals, payloadMap));
        auto traces = provider.getTrace("");
        CHECK_TRACE_COUNT(1, "pk field [not_exist] not found or is empty.", traces);
    }
    {
        NaviLoggerProvider provider("WARN");
        HashMode hashMode;
        string pkField = "buyer_id";
        hashMode._hashFields = {"buyer_id"};
        hashMode._hashFunction = "HASH";
        map<string, uint16_t> payloadMap;
        ASSERT_TRUE(MessageConstructor::genPayloadMap(hashMode, pkField, hashVals, payloadMap));
        ASSERT_EQ(1, payloadMap.size());
        ASSERT_EQ(9091, payloadMap["12"]);
    }
    {
        NaviLoggerProvider provider("WARN");
        HashMode hashMode;
        string pkField = "pk";
        hashMode._hashFields = {"buyer_id"};
        hashMode._hashFunction = "HASH";
        map<string, uint16_t> payloadMap;
        vector<map<string, string>> hashVals
            = {{{"buyer_id", "12"}, {"pk", "1"}}, {{"buyer_id", "22"}, {"pk", "2"}}};
        ASSERT_TRUE(MessageConstructor::genPayloadMap(hashMode, pkField, hashVals, payloadMap));
        ASSERT_EQ(2, payloadMap.size());
        HashFunctionBasePtr func = MessageConstructor::createHashFunc(hashMode);
        ASSERT_TRUE(func);
        vector<string> expected = {"12"};
        EXPECT_EQ(func->getHashId(expected), payloadMap["1"]);
        expected = {"22"};
        EXPECT_EQ(func->getHashId(expected), payloadMap["2"]);
    }
    {
        NaviLoggerProvider provider("WARN");
        HashMode hashMode;
        string pkField = "pk";
        hashMode._hashFields = {"buyer_id", "pk"};
        hashMode._hashFunction = "ROUTING_HASH";
        map<string, uint16_t> payloadMap;
        vector<map<string, string>> hashVals
            = {{{"buyer_id", "12"}, {"pk", "1"}}, {{"buyer_id", "22"}, {"pk", "2"}}};
        ASSERT_TRUE(MessageConstructor::genPayloadMap(hashMode, pkField, hashVals, payloadMap));
        ASSERT_EQ(2, payloadMap.size());
        std::map<std::string, std::string> params;
        HashFunctionBasePtr func = MessageConstructor::createHashFunc(hashMode);
        ASSERT_TRUE(func);
        vector<string> expected = {"12", "1"};
        EXPECT_EQ(func->getHashId(expected), payloadMap["1"]);
        expected = {"22", "2"};
        EXPECT_EQ(func->getHashId(expected), payloadMap["2"]);
    }
}

TEST_F(MessageConstructorTest, testGenAddMessageData) {
    {
        NaviLoggerProvider provider("ERROR");
        string inputJsonStr = R"json({
            "flag" : true,
            "$modify_time" : "1657274727",
            "$title" : "hello world",
            "$buyer_id" : 12
        })json";
        AutilPoolAllocator allocator(&_pool);
        SimpleDocument modifyExprsDoc(&allocator);
        modifyExprsDoc.Parse(inputJsonStr.c_str());
        string pkField = "buyer_id";
        string outMsgData;
        ASSERT_FALSE(
            MessageConstructor::genAddMessageData(modifyExprsDoc, pkField, "", outMsgData));
        auto traces = provider.getTrace("");
        CHECK_TRACE_COUNT(1, "value type is not string or int , key[\"flag\"] value[true]", traces);
    }
    {
        NaviLoggerProvider provider("WARN");
        string inputJsonStr = R"json({
            "$modify_time" : "1657274727",
            "$title" : "hello world",
            "$buyer_id" : ""
        })json";
        AutilPoolAllocator allocator(&_pool);
        SimpleDocument modifyExprsDoc(&allocator);
        modifyExprsDoc.Parse(inputJsonStr.c_str());
        string pkField = "buyer_id";
        string outMsgData;
        ASSERT_FALSE(
            MessageConstructor::genAddMessageData(modifyExprsDoc, pkField, "", outMsgData));
        auto traces = provider.getTrace("");
        CHECK_TRACE_COUNT(1, "pk field [buyer_id] is empty.", traces);
    }
    {
        NaviLoggerProvider provider("WARN");
        string inputJsonStr = R"json({
            "$modify_time" : "1657274727",
            "$title" : "hello world",
            "$buyer_id" : 12
        })json";
        AutilPoolAllocator allocator(&_pool);
        SimpleDocument modifyExprsDoc(&allocator);
        modifyExprsDoc.Parse(inputJsonStr.c_str());
        string pkField = "pk";
        string outMsgData;
        ASSERT_FALSE(
            MessageConstructor::genAddMessageData(modifyExprsDoc, pkField, "", outMsgData));
        auto traces = provider.getTrace("");
        CHECK_TRACE_COUNT(1, "not pk field [pk] found.", traces);
    }
    {
        NaviLoggerProvider provider("WARN");
        string inputJsonStr = R"json({
            "$modify_time" : "1657274727",
            "$title" : "hello world",
            "$buyer_id" : 12
        })json";
        AutilPoolAllocator allocator(&_pool);
        SimpleDocument modifyExprsDoc(&allocator);
        modifyExprsDoc.Parse(inputJsonStr.c_str());
        string pkField = "buyer_id";
        string outMsgData;
        ASSERT_TRUE(
            MessageConstructor::genAddMessageData(modifyExprsDoc, pkField, "123", outMsgData));
        FieldGroupReader reader;
        ASSERT_TRUE(reader.fromProductionString(outMsgData));
        ASSERT_EQ(5, reader.getFieldSize());
        ASSERT_NO_FATAL_FAILURE(checkKV(reader, MESSAGE_KEY_CMD, MESSAGE_CMD_ADD));
        ASSERT_NO_FATAL_FAILURE(checkKV(reader, "modify_time", "1657274727"));
        ASSERT_NO_FATAL_FAILURE(checkKV(reader, "title", "hello world"));
        ASSERT_NO_FATAL_FAILURE(checkKV(reader, "buyer_id", "12"));
        ASSERT_NO_FATAL_FAILURE(checkKV(reader, DML_REQUEST_ID_FIELD_NAME, "123"));
    }
    {
        NaviLoggerProvider provider("WARN");
        string inputJsonStr = R"json({
            "$modify_time" : "1657274727",
            "$title" : "hello world",
            "$buyer_id" : 12,
            "$dml_request_id" : "abcc"
        })json";
        AutilPoolAllocator allocator(&_pool);
        SimpleDocument modifyExprsDoc(&allocator);
        modifyExprsDoc.Parse(inputJsonStr.c_str());
        string pkField = "buyer_id";
        string outMsgData;
        ASSERT_TRUE(
            MessageConstructor::genAddMessageData(modifyExprsDoc, pkField, "123", outMsgData));
        FieldGroupReader reader;
        ASSERT_TRUE(reader.fromProductionString(outMsgData));
        ASSERT_EQ(5, reader.getFieldSize());
        ASSERT_NO_FATAL_FAILURE(checkKV(reader, MESSAGE_KEY_CMD, MESSAGE_CMD_ADD));
        ASSERT_NO_FATAL_FAILURE(checkKV(reader, "modify_time", "1657274727"));
        ASSERT_NO_FATAL_FAILURE(checkKV(reader, "title", "hello world"));
        ASSERT_NO_FATAL_FAILURE(checkKV(reader, "buyer_id", "12"));
        ASSERT_NO_FATAL_FAILURE(checkKV(reader, DML_REQUEST_ID_FIELD_NAME, "abcc"));
    }
}

TEST_F(MessageConstructorTest, testConstructAddMessageData) {
    {
        NaviLoggerProvider provider("WARN");
        string inputJsonStr = R"json({
            "$modify_time" : "1657274727",
            "$title" : "hello world",
            "$buyer_id" : 12
        })json";
        AutilPoolAllocator allocator(&_pool);
        SimpleDocument modifyExprsDoc(&allocator);
        modifyExprsDoc.Parse(inputJsonStr.c_str());
        HashMode hashMode;
        string pkField = "buyer_id";
        string traceId = "aaaaa";
        hashMode._hashFields = {"buyer_id"};
        hashMode._hashFunction = "HASH";
        vector<pair<uint16_t, string>> outMessages;
        ASSERT_TRUE(MessageConstructor::constructAddMessage(
            modifyExprsDoc, hashMode, pkField, traceId, outMessages));

        HashFunctionBasePtr func = MessageConstructor::createHashFunc(hashMode);
        ASSERT_TRUE(func);
        ASSERT_EQ(1, outMessages.size());
        vector<string> expected({"12"});
        ASSERT_EQ(func->getHashId(expected), outMessages[0].first);
        FieldGroupReader reader;
        ASSERT_TRUE(reader.fromProductionString(outMessages[0].second));
        ASSERT_EQ(5, reader.getFieldSize());
        ASSERT_NO_FATAL_FAILURE(checkKV(reader, MESSAGE_KEY_CMD, MESSAGE_CMD_ADD));
        ASSERT_NO_FATAL_FAILURE(checkKV(reader, "modify_time", "1657274727"));
        ASSERT_NO_FATAL_FAILURE(checkKV(reader, "title", "hello world"));
        ASSERT_NO_FATAL_FAILURE(checkKV(reader, "buyer_id", "12"));
        ASSERT_NO_FATAL_FAILURE(checkKV(reader, DML_REQUEST_ID_FIELD_NAME, traceId));
    }
}

TEST_F(MessageConstructorTest, testGenUpdateMessages) {
    {
        NaviLoggerProvider provider("WARN");
        string inputJsonStr = R"json({
            "$flag" : true,
            "$modify_time" : "1657274727",
            "$title" : "hello world"
        })json";
        AutilPoolAllocator allocator(&_pool);
        SimpleDocument modifyExprsDoc(&allocator);
        modifyExprsDoc.Parse(inputJsonStr.c_str());
        map<string, uint16_t> pkMaps({{"12", 123}, {"22", 456}});
        string pkField = "buyer_id";
        vector<pair<uint16_t, string>> outMessages;
        ASSERT_FALSE(MessageConstructor::genUpdateMessages(
            modifyExprsDoc, pkMaps, pkField, "", outMessages));
        auto traces = provider.getTrace("");
        CHECK_TRACE_COUNT(
            1, "value type is not string or int , key[\"$flag\"] value[true]", traces);
    }
    {
        NaviLoggerProvider provider("WARN");
        string inputJsonStr = R"json({
            "$modify_time" : "1657274727",
            "$title" : "hello world"
        })json";
        AutilPoolAllocator allocator(&_pool);
        SimpleDocument modifyExprsDoc(&allocator);
        modifyExprsDoc.Parse(inputJsonStr.c_str());
        map<string, uint16_t> pkMaps({{"12", 123}, {"22", 456}});
        string pkField = "buyer_id";
        string traceId = "aaaaa";
        vector<pair<uint16_t, string>> outMessages;
        ASSERT_TRUE(MessageConstructor::genUpdateMessages(
            modifyExprsDoc, pkMaps, pkField, traceId, outMessages));
        ASSERT_EQ(2, outMessages.size());
        {
            ASSERT_EQ(123, outMessages[0].first);
            FieldGroupReader reader;
            ASSERT_TRUE(reader.fromProductionString(outMessages[0].second));
            ASSERT_EQ(5, reader.getFieldSize());
            ASSERT_NO_FATAL_FAILURE(checkKV(reader, MESSAGE_KEY_CMD, MESSAGE_CMD_UPDATE));
            ASSERT_NO_FATAL_FAILURE(checkKV(reader, "modify_time", "1657274727"));
            ASSERT_NO_FATAL_FAILURE(checkKV(reader, "title", "hello world"));
            ASSERT_NO_FATAL_FAILURE(checkKV(reader, "buyer_id", "12"));
            ASSERT_NO_FATAL_FAILURE(checkKV(reader, DML_REQUEST_ID_FIELD_NAME, traceId));
        }
        {
            ASSERT_EQ(456, outMessages[1].first);
            FieldGroupReader reader;
            ASSERT_TRUE(reader.fromProductionString(outMessages[1].second));
            ASSERT_EQ(5, reader.getFieldSize());
            ASSERT_NO_FATAL_FAILURE(checkKV(reader, MESSAGE_KEY_CMD, MESSAGE_CMD_UPDATE));
            ASSERT_NO_FATAL_FAILURE(checkKV(reader, "modify_time", "1657274727"));
            ASSERT_NO_FATAL_FAILURE(checkKV(reader, "title", "hello world"));
            ASSERT_NO_FATAL_FAILURE(checkKV(reader, "buyer_id", "22"));
            ASSERT_NO_FATAL_FAILURE(checkKV(reader, DML_REQUEST_ID_FIELD_NAME, traceId));
        }
    }
    {
        NaviLoggerProvider provider("WARN");
        string inputJsonStr = R"json({
            "$modify_time" : "1657274727",
            "$title" : "hello world",
            "$dml_request_id" : "abcc"
        })json";
        AutilPoolAllocator allocator(&_pool);
        SimpleDocument modifyExprsDoc(&allocator);
        modifyExprsDoc.Parse(inputJsonStr.c_str());
        map<string, uint16_t> pkMaps({{"12", 123}, {"22", 456}});
        string pkField = "buyer_id";
        string traceId = "aaaaa";
        vector<pair<uint16_t, string>> outMessages;
        ASSERT_TRUE(MessageConstructor::genUpdateMessages(
            modifyExprsDoc, pkMaps, pkField, traceId, outMessages));
        ASSERT_EQ(2, outMessages.size());
        {
            ASSERT_EQ(123, outMessages[0].first);
            FieldGroupReader reader;
            ASSERT_TRUE(reader.fromProductionString(outMessages[0].second));
            ASSERT_EQ(5, reader.getFieldSize());
            ASSERT_NO_FATAL_FAILURE(checkKV(reader, MESSAGE_KEY_CMD, MESSAGE_CMD_UPDATE));
            ASSERT_NO_FATAL_FAILURE(checkKV(reader, "modify_time", "1657274727"));
            ASSERT_NO_FATAL_FAILURE(checkKV(reader, "title", "hello world"));
            ASSERT_NO_FATAL_FAILURE(checkKV(reader, "buyer_id", "12"));
            ASSERT_NO_FATAL_FAILURE(checkKV(reader, DML_REQUEST_ID_FIELD_NAME, "abcc"));
        }
        {
            ASSERT_EQ(456, outMessages[1].first);
            FieldGroupReader reader;
            ASSERT_TRUE(reader.fromProductionString(outMessages[1].second));
            ASSERT_EQ(5, reader.getFieldSize());
            ASSERT_NO_FATAL_FAILURE(checkKV(reader, MESSAGE_KEY_CMD, MESSAGE_CMD_UPDATE));
            ASSERT_NO_FATAL_FAILURE(checkKV(reader, "modify_time", "1657274727"));
            ASSERT_NO_FATAL_FAILURE(checkKV(reader, "title", "hello world"));
            ASSERT_NO_FATAL_FAILURE(checkKV(reader, "buyer_id", "22"));
            ASSERT_NO_FATAL_FAILURE(checkKV(reader, DML_REQUEST_ID_FIELD_NAME, "abcc"));
        }
    }
}

TEST_F(MessageConstructorTest, testConstructUpdateMessageData) {
    {
        NaviLoggerProvider provider("WARN");
        string inputJsonStr = R"json({
            "$modify_time" : "1657274727",
            "$title" : "hello world"
        })json";
        AutilPoolAllocator allocator(&_pool);
        SimpleDocument modifyExprsDoc(&allocator);
        modifyExprsDoc.Parse(inputJsonStr.c_str());
        HashMode hashMode;
        string pkField = "pk";
        string traceId = "aaaaaa";
        hashMode._hashFields = {"buyer_id"};
        hashMode._hashFunction = "HASH";
        map<string, uint16_t> payloadMap;
        vector<map<string, string>> hashVals
            = {{{"buyer_id", "12"}, {"pk", "1"}}, {{"buyer_id", "22"}, {"pk", "2"}}};
        vector<pair<uint16_t, string>> outMessages;
        ASSERT_TRUE(MessageConstructor::constructUpdateMessage(
            modifyExprsDoc, hashMode, pkField, hashVals, traceId, outMessages));
        HashFunctionBasePtr func = MessageConstructor::createHashFunc(hashMode);
        ASSERT_TRUE(func);

        ASSERT_EQ(2, outMessages.size());
        {
            vector<string> expected({"12"});
            ASSERT_EQ(func->getHashId(expected), outMessages[0].first);
            FieldGroupReader reader;
            ASSERT_TRUE(reader.fromProductionString(outMessages[0].second));
            ASSERT_EQ(5, reader.getFieldSize());
            ASSERT_NO_FATAL_FAILURE(checkKV(reader, MESSAGE_KEY_CMD, MESSAGE_CMD_UPDATE));
            ASSERT_NO_FATAL_FAILURE(checkKV(reader, "modify_time", "1657274727"));
            ASSERT_NO_FATAL_FAILURE(checkKV(reader, "title", "hello world"));
            ASSERT_NO_FATAL_FAILURE(checkKV(reader, "pk", "1"));
            ASSERT_NO_FATAL_FAILURE(checkKV(reader, DML_REQUEST_ID_FIELD_NAME, traceId));
        }
        {
            vector<string> expected({"22"});
            ASSERT_EQ(func->getHashId(expected), outMessages[1].first);
            FieldGroupReader reader;
            ASSERT_TRUE(reader.fromProductionString(outMessages[1].second));
            ASSERT_EQ(5, reader.getFieldSize());
            ASSERT_NO_FATAL_FAILURE(checkKV(reader, MESSAGE_KEY_CMD, MESSAGE_CMD_UPDATE));
            ASSERT_NO_FATAL_FAILURE(checkKV(reader, "modify_time", "1657274727"));
            ASSERT_NO_FATAL_FAILURE(checkKV(reader, "title", "hello world"));
            ASSERT_NO_FATAL_FAILURE(checkKV(reader, "pk", "2"));
            ASSERT_NO_FATAL_FAILURE(checkKV(reader, DML_REQUEST_ID_FIELD_NAME, traceId));
        }
    }
}

TEST_F(MessageConstructorTest, testGenDeleteMessageDataFailed_notFindPk) {
    NaviLoggerProvider provider("ERROR");
    vector<map<string, string>> hashVals
        = {{{"buyer_id", "12"}, {"pk", "1"}}, {{"buyer_id", "22"}, {"pk", "2"}}};
    map<string, uint16_t> pkMaps({{"3", 123}, {"2", 456}});
    string pkField = "pk";
    vector<pair<uint16_t, string>> outMessages;
    ASSERT_FALSE(MessageConstructor::genDeleteMessages(hashVals, pkMaps, pkField, "", outMessages));
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "not find pk [1] in pkMaps [2:456;3:123;]", traces);
}

TEST_F(MessageConstructorTest, testGenDeleteMessageDataFailed_invalidPk) {
    NaviLoggerProvider provider("ERROR");
    vector<map<string, string>> hashVals
        = {{{"buyer_id", "12"}, {"pk", ""}}, {{"buyer_id", "22"}, {"pk", "2"}}};
    map<string, uint16_t> pkMaps({{"1", 123}, {"2", 456}});
    string pkField = "pk";
    vector<pair<uint16_t, string>> outMessages;
    ASSERT_FALSE(MessageConstructor::genDeleteMessages(hashVals, pkMaps, pkField, "", outMessages));
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "invalid pk field [buyer_id:12;pk:;]", traces);
}

TEST_F(MessageConstructorTest, testGenDeleteMessageData) {
    vector<map<string, string>> hashVals
        = {{{"buyer_id", "12"}, {"pk", "1"}}, {{"buyer_id", "22"}, {"pk", "2"}}};
    map<string, uint16_t> pkMaps({{"1", 123}, {"2", 456}});
    string pkField = "pk";
    string traceId = "aaaaa";
    vector<pair<uint16_t, string>> outMessages;
    ASSERT_TRUE(
        MessageConstructor::genDeleteMessages(hashVals, pkMaps, pkField, traceId, outMessages));
    ASSERT_EQ(2, outMessages.size());
    ASSERT_EQ(123, outMessages[0].first);
    {
        FieldGroupReader reader;
        ASSERT_TRUE(reader.fromProductionString(outMessages[0].second));
        ASSERT_EQ(4, reader.getFieldSize());
        ASSERT_NO_FATAL_FAILURE(checkKV(reader, MESSAGE_KEY_CMD, MESSAGE_CMD_DELETE));
        ASSERT_NO_FATAL_FAILURE(checkKV(reader, "pk", "1"));
        ASSERT_NO_FATAL_FAILURE(checkKV(reader, "buyer_id", "12"));
        ASSERT_NO_FATAL_FAILURE(checkKV(reader, DML_REQUEST_ID_FIELD_NAME, traceId));
    }
    ASSERT_EQ(456, outMessages[1].first);
    {
        FieldGroupReader reader;
        ASSERT_TRUE(reader.fromProductionString(outMessages[1].second));
        ASSERT_EQ(4, reader.getFieldSize());
        ASSERT_NO_FATAL_FAILURE(checkKV(reader, MESSAGE_KEY_CMD, MESSAGE_CMD_DELETE));
        ASSERT_NO_FATAL_FAILURE(checkKV(reader, "pk", "2"));
        ASSERT_NO_FATAL_FAILURE(checkKV(reader, "buyer_id", "22"));
        ASSERT_NO_FATAL_FAILURE(checkKV(reader, DML_REQUEST_ID_FIELD_NAME, traceId));
    }
}

TEST_F(MessageConstructorTest, testConstructDeleteMessageData) {
    {
        NaviLoggerProvider provider("WARN");
        HashMode hashMode;
        string pkField = "pk";
        string traceId = "aaaa";
        hashMode._hashFields = {"buyer_id", "pk"};
        hashMode._hashFunction = "ROUTING_HASH";
        map<string, uint16_t> payloadMap;
        vector<map<string, string>> hashVals
            = {{{"buyer_id", "12"}, {"pk", "1"}}, {{"buyer_id", "22"}, {"pk", "2"}}};
        vector<pair<uint16_t, string>> outMessages;
        ASSERT_TRUE(MessageConstructor::constructDeleteMessage(
            hashMode, pkField, hashVals, traceId, outMessages));
        HashFunctionBasePtr func = MessageConstructor::createHashFunc(hashMode);
        ASSERT_TRUE(func);
        ASSERT_EQ(2, outMessages.size());

        {
            vector<string> expected = {"12", "1"};
            ASSERT_EQ(func->getHashId(expected), outMessages[0].first);
            FieldGroupReader reader;
            ASSERT_TRUE(reader.fromProductionString(outMessages[0].second));
            ASSERT_EQ(4, reader.getFieldSize());
            ASSERT_NO_FATAL_FAILURE(checkKV(reader, MESSAGE_KEY_CMD, MESSAGE_CMD_DELETE));
            ASSERT_NO_FATAL_FAILURE(checkKV(reader, "pk", "1"));
            ASSERT_NO_FATAL_FAILURE(checkKV(reader, "buyer_id", "12"));
            ASSERT_NO_FATAL_FAILURE(checkKV(reader, DML_REQUEST_ID_FIELD_NAME, traceId));
        }
        {
            vector<string> expected = {"22", "2"};
            ASSERT_EQ(func->getHashId(expected), outMessages[1].first);
            FieldGroupReader reader;
            ASSERT_TRUE(reader.fromProductionString(outMessages[1].second));
            ASSERT_EQ(4, reader.getFieldSize());
            ASSERT_NO_FATAL_FAILURE(checkKV(reader, MESSAGE_KEY_CMD, MESSAGE_CMD_DELETE));
            ASSERT_NO_FATAL_FAILURE(checkKV(reader, "pk", "2"));
            ASSERT_NO_FATAL_FAILURE(checkKV(reader, "buyer_id", "22"));
            ASSERT_NO_FATAL_FAILURE(checkKV(reader, DML_REQUEST_ID_FIELD_NAME, traceId));
        }
    }
}

TEST_F(MessageConstructorTest, testConstructMessage) {
    HashMode hashMode;
    hashMode._hashFields.push_back("buyer_id");
    hashMode._hashFunction = "HASH";
    string pkField = "buyer_id";
    string traceId = "aaaa";
    vector<map<string, string>> hashVals = {{{"buyer_id", "12"}}};
    string msgType = TABLE_OPERATION_INSERT;
    string inputJsonStr = R"json({
    "$modify_time" : "1657274727",
    "$title" : "hello world",
    "$buyer_id" : 12
})json";
    vector<pair<uint16_t, string>> outMessages;
    ASSERT_TRUE(MessageConstructor::constructMessage(
        hashMode, pkField, hashVals, msgType, traceId, &_pool, inputJsonStr, outMessages));
    ASSERT_EQ(1, outMessages.size());
    ASSERT_EQ(9091, outMessages[0].first);
    FieldGroupReader reader;
    ASSERT_TRUE(reader.fromProductionString(outMessages[0].second));
    ASSERT_EQ(5, reader.getFieldSize());
    ASSERT_NO_FATAL_FAILURE(checkKV(reader, MESSAGE_KEY_CMD, MESSAGE_CMD_ADD));
    ASSERT_NO_FATAL_FAILURE(checkKV(reader, "modify_time", "1657274727"));
    ASSERT_NO_FATAL_FAILURE(checkKV(reader, "title", "hello world"));
    ASSERT_NO_FATAL_FAILURE(checkKV(reader, "buyer_id", "12"));
    ASSERT_NO_FATAL_FAILURE(checkKV(reader, DML_REQUEST_ID_FIELD_NAME, traceId));
}

} // namespace sql
