#include "swift/common/FieldSchema.h"

#include <bitset>
#include <iostream>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/TimeUtility.h"
#include "autil/legacy/fast_jsonizable_dec.h"
#include "swift/config/ResourceReader.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;

namespace swift {
namespace common {

class FieldSchemaTest : public TESTBASE {
public:
    void setUp() { _workDir = GET_TEST_DATA_PATH() + "schema_topic/schema"; }
    void tearDown() {}

private:
    string _workDir;
};

void assertFieldItemEqual(const FieldItem &left, const FieldItem &right) {
    EXPECT_EQ(left.name, right.name);
    EXPECT_EQ(left.type, right.type);
    EXPECT_EQ(left.subFields.size(), right.subFields.size());
    for (size_t index = 0; index < left.subFields.size(); ++index) {
        EXPECT_EQ(left.subFields[index].name, right.subFields[index].name);
        EXPECT_EQ(left.subFields[index].type, right.subFields[index].type);
    }
}

TEST_F(FieldSchemaTest, testLoadSchema) {
    config::ResourceReader resReader(_workDir);
    string content;
    EXPECT_TRUE(resReader.getFileContent(content, "schema.json"));
    FieldSchema schema;
    schema.fromJsonString(content);

    cout << schema.topicName << endl;
    for (const auto &field : schema.fields) {
        cout << field.name << " " << field.type << endl;
        if (0 != field.subFields.size()) {
            cout << "  {" << endl;
            for (const auto &subField : field.subFields) {
                cout << "    " << subField.name << " " << subField.type << endl;
            }
            cout << "  }" << endl;
        }
    }

    EXPECT_EQ("mainse", schema.topicName);
    EXPECT_EQ(6, schema.fields.size());
    assertFieldItemEqual(FieldItem("CMD", "string"), schema.fields[0]);
    assertFieldItemEqual(FieldItem("timestamp", "string"), schema.fields[1]);
    assertFieldItemEqual(FieldItem("price", "string"), schema.fields[2]);
    assertFieldItemEqual(FieldItem("title", "string"), schema.fields[3]);
    FieldItem phoneItem("phone_series", "");
    phoneItem.addSubField("iphone", "string");
    phoneItem.addSubField("huawei", "string");
    phoneItem.addSubField("xiaomi", "string");
    EXPECT_EQ(3, schema.fields[4].subFields.size());
    assertFieldItemEqual(phoneItem, schema.fields[4]);
    assertFieldItemEqual(FieldItem("image_path", "string"), schema.fields[5]);

    string expectSchemaStr(
        R"json({"fields":[{"name":"CMD","type":"string"},{"name":"timestamp","type":"string"},{"name":"price","type":"string"},{"name":"title","type":"string"},{"name":"phone_series","subFields":[{"name":"iphone","type":"string"},{"name":"huawei","type":"string"},{"name":"xiaomi","type":"string"}]},{"name":"image_path","type":"string"}],"topic":"mainse"})json");
    const string &schemaStr = schema.toJsonString();
    EXPECT_EQ(expectSchemaStr, schemaStr);
}

TEST_F(FieldSchemaTest, testAddSchema) {
    FieldSchema schema;
    string expect(R"json({"topic":"","fields":[]})json");
    string result = FastToJsonString(schema);
    EXPECT_EQ(expect, result);

    schema.setTopicName("mainse_offline_full");
    EXPECT_TRUE(schema.addField("name", "string"));
    EXPECT_TRUE(schema.addField("age", "string"));
    EXPECT_TRUE(schema.addField("salary", "string"));
    EXPECT_FALSE(schema.addField("name", "string")); // add mutiple time
    EXPECT_FALSE(schema.addField("name", "string")); // add mutiple time, type change
    EXPECT_FALSE(schema.addField("age", "string"));  // add mutiple time
    EXPECT_FALSE(schema.addField("blood", "float")); // type not support
    result = schema.toJsonString();
    expect =
        R"json({"fields":[{"name":"name","type":"string"},{"name":"age","type":"string"},{"name":"salary","type":"string"}],"topic":"mainse_offline_full"})json";
    EXPECT_EQ(expect, result);

    FieldItem item("placed_lived");
    EXPECT_TRUE(item.addSubField("Manhaton", "string"));
    EXPECT_TRUE(item.addSubField("Hangzhou", "string"));
    EXPECT_TRUE(item.addSubField("Shangrao", "string"));
    EXPECT_TRUE(schema.addField(item));
    result = schema.toJsonString();
    expect =
        R"json({"fields":[{"name":"name","type":"string"},{"name":"age","type":"string"},{"name":"salary","type":"string"},{"name":"placed_lived","subFields":[{"name":"Manhaton","type":"string"},{"name":"Hangzhou","type":"string"},{"name":"Shangrao","type":"string"}]}],"topic":"mainse_offline_full"})json";
    EXPECT_EQ(expect, result);

    EXPECT_TRUE(schema.addField("school"));
    EXPECT_TRUE(schema.addSubField("school", "Zhejiang university"));
    EXPECT_TRUE(schema.addSubField("school", "Qinghua university"));
    EXPECT_TRUE(schema.addSubField("school", "MIT university", "string"));
    EXPECT_FALSE(schema.addSubField("school", "MIT university", "string")); // repeat
    EXPECT_FALSE(schema.addSubField("school", "HUST university", "int"));   // type not support
    result = schema.toJsonString();
    expect =
        R"json({"fields":[{"name":"name","type":"string"},{"name":"age","type":"string"},{"name":"salary","type":"string"},{"name":"placed_lived","subFields":[{"name":"Manhaton","type":"string"},{"name":"Hangzhou","type":"string"},{"name":"Shangrao","type":"string"}]},{"name":"school","subFields":[{"name":"Zhejiang university","type":"string"},{"name":"Qinghua university","type":"string"},{"name":"MIT university","type":"string"}]}],"topic":"mainse_offline_full"})json";
    EXPECT_EQ(expect, result);
}

TEST_F(FieldSchemaTest, testIsValid) {
    FieldSchema schema;
    EXPECT_FALSE(schema.isValid());

    string schemaStr(R"json({"topic":"","fields":[]})json");
    schema.fromJsonString(schemaStr);
    EXPECT_FALSE(schema.isValid());

    schema.setTopicName("mainse");
    EXPECT_FALSE(schema.isValid());

    schema.addField("aaa");
    EXPECT_TRUE(schema.isValid());
}

TEST_F(FieldSchemaTest, testFromJsonString) {
    string schemaStr(
        R"json({"topic":"mainse","fields":[{"name":"CMD","type":"string"},{"name":"timestamp","type":"string"},{"name":"price","type":"string"},{"name":"title","type":"string"},{"name":"phone_series","subFields":[{"name":"iphone","type":"string"},{"name":"huawei","type":"string"},{"name":"xiaomi","type":"string"}]},{"name":"image_path","type":"string"}]})json");
    FieldSchema schema;
    EXPECT_TRUE(schema.fromJsonString(schemaStr));

    schemaStr =
        R"json({"topic":"mainse","fields":[{"name":"CMD","type":"string"},{"name":"timestamp","type":"string"},{"name":"price","type":"string"},{"name":"title","type":"string"},{"name":"phone_series","subFields":["name":"iphone","type":{"name":"huawei","type":"string"},{"name":"xiaomi","type":"string"}]},{"name":"image_path","type":"string"}]})json";
    EXPECT_FALSE(schema.fromJsonString(schemaStr));
}

TEST_F(FieldSchemaTest, testFromAndToJson) {
    string schemaStr(
        R"json({"fields":[{"name":"CMD","type":"string"},{"name":"timestamp","type":"string"},{"name":"price","type":"string"},{"name":"title","type":"string"},{"name":"phone_series","subFields":[{"name":"iphone","type":"string"},{"name":"huawei","type":"string"},{"name":"xiaomi","type":"string"}]},{"name":"image_path","type":"string"}],"topic":"mainse"})json");
    FieldSchema schema;
    EXPECT_TRUE(schema.fromJsonString(schemaStr));
    const string &result = schema.toJsonString();
    EXPECT_EQ(schemaStr, result);
}

TEST_F(FieldSchemaTest, testCalcVersion) {
    string schemaStr(
        R"json({"fields":[{"name":"CMD","type":"string"},{"name":"timestamp","type":"string"},{"name":"price","type":"string"},{"name":"title","type":"string"},{"name":"phone_series","subFields":[{"name":"iphone","type":"string"},{"name":"huawei","type":"string"},{"name":"xiaomi","type":"string"}]},{"name":"image_path","type":"string"}],"topic":"mainse"})json");
    FieldSchema schema;
    EXPECT_TRUE(schema.fromJsonString(schemaStr));
    uint32_t version = schema.calcVersion();
    EXPECT_EQ(2657765422, version);

    // 1. schema 格式变化，version不变
    string schema1(R"json(
{"topic":"mainse",
"fields":[
{"name":"CMD", "type":"string"},
{"name":"timestamp", "type":"string"},
{"name":"price", "type":"string"},
{"name":"title", "type":"string"},
{"name":"phone_series",
"subFields":[
    {"name":"iphone", "type":"string"},
    {"name":"huawei", "type":"string"},
    {"name":"xiaomi", "type":"string"}
]},
{"name":"image_path", "type":"string"}]}
)json");
    EXPECT_TRUE(schema.fromJsonString(schema1));
    uint32_t version1 = schema.calcVersion();
    EXPECT_EQ(version, version1);

    // 2. CMD跟timestamp位置变化，version变化
    string schema2(R"json(
{"topic":"mainse",
"fields":[
{"name":"timestamp", "type":"string"},
{"name":"CMD", "type":"string"},
{"name":"price", "type":"string"},
{"name":"title", "type":"string"},
{"name":"phone_series",
"subFields":[
    {"name":"iphone", "type":"string"},
    {"name":"huawei", "type":"string"},
    {"name":"xiaomi", "type":"string"}
]},
{"name":"image_path", "type":"string"}]}
)json");
    EXPECT_TRUE(schema.fromJsonString(schema2));
    uint32_t version2 = schema.calcVersion();
    EXPECT_EQ(3540537468, version2);
    EXPECT_TRUE(version2 != version);

    // 3. sub字段位置变化，version变化
    string schema3(R"json(
{"topic":"mainse",
"fields":[
{"name":"CMD", "type":"string"},
{"name":"timestamp", "type":"string"},
{"name":"price", "type":"string"},
{"name":"title", "type":"string"},
{"name":"phone_series",
"subFields":[
    {"name":"iphone", "type":"string"},
    {"name":"xiaomi", "type":"string"},
    {"name":"huawei", "type":"string"}
]},
{"name":"image_path", "type":"string"}]}
)json");
    EXPECT_TRUE(schema.fromJsonString(schema3));
    uint32_t version3 = schema.calcVersion();
    EXPECT_EQ(157498290, version3);
    EXPECT_TRUE(version != version3);
    //类型后续支持
    // 4. 主字段类型变化，version变化
    // 5. sub字段类型变化，version变化
}

TEST_F(FieldSchemaTest, testSchemaHeader) {
    { // default
        SchemaHeader header;
        EXPECT_EQ(0, header.pack);
        EXPECT_EQ(0, header.meta.checkval);
        EXPECT_EQ(0, header.meta.dataType);
        EXPECT_EQ(0, header.meta.reserve);
        EXPECT_EQ(0, header.version);
        char buf[8];
        header.toBuf(buf);
        for (int i = 0; i < 8; ++i) {
            EXPECT_EQ(0, buf[i]);
        }
        SchemaHeader result;
        result.fromBuf(buf);
        EXPECT_TRUE(header == result);
    }
    {
        SchemaHeader header;
        EXPECT_EQ(0, header.pack);
        EXPECT_EQ(0, header.meta.checkval);
        EXPECT_EQ(0, header.meta.dataType);
        EXPECT_EQ(0, header.meta.reserve);
        EXPECT_EQ(0, header.version);
        header.meta.dataType = swift::protocol::DATA_TYPE_SCHEMA;
        char buf[8];
        header.toBuf(buf);
        cout << buf << endl;
        SchemaHeader result;
        result.fromBuf(buf);
        cout << "check: " << header.meta.checkval << ", dataType: " << int(header.meta.dataType)
             << ", reserve: " << header.meta.reserve << ", version: " << header.version << endl;
        cout << header.pack << " " << bitset<sizeof(unsigned int) * 8>(header.pack) << endl;
        EXPECT_TRUE(header == result);
    }
    {
        SchemaHeader header;
        EXPECT_EQ(0, header.pack);
        EXPECT_EQ(0, header.meta.checkval);
        EXPECT_EQ(0, header.meta.dataType);
        EXPECT_EQ(0, header.meta.reserve);
        EXPECT_EQ(0, header.version);
        header.meta.dataType = swift::protocol::DATA_TYPE_FIELDFILTER;
        char buf[8];
        header.toBuf(buf);
        cout << buf << endl;
        SchemaHeader result;
        result.fromBuf(buf);
        cout << "check: " << header.meta.checkval << ", dataType: " << int(header.meta.dataType)
             << ", reserve: " << header.meta.reserve << ", version: " << header.version << endl;
        cout << header.pack << " " << bitset<sizeof(unsigned int) * 8>(header.pack) << endl;
        EXPECT_TRUE(header == result);
    }
    {
        SchemaHeader header(199);
        EXPECT_EQ(0, header.pack);
        EXPECT_EQ(0, header.meta.checkval);
        EXPECT_EQ(0, header.meta.dataType);
        EXPECT_EQ(0, header.meta.reserve);
        EXPECT_EQ(199, header.version);
        char buf[8];
        header.toBuf(buf);
        cout << buf << endl;
        SchemaHeader result;
        result.fromBuf(buf);
        cout << "check: " << header.meta.checkval << ", dataType: " << int(header.meta.dataType)
             << ", reserve: " << header.meta.reserve << ", version: " << header.version << endl;
        cout << header.pack << " " << bitset<sizeof(unsigned int) * 8>(header.pack) << endl;
        EXPECT_TRUE(header == result);
    }
    {
        SchemaHeader header(100);
        header.meta.checkval = 314342;
        header.meta.dataType = swift::protocol::DATA_TYPE_SCHEMA;
        header.meta.reserve = 12;
        EXPECT_EQ(314342, header.meta.checkval);
        EXPECT_EQ(swift::protocol::DATA_TYPE_SCHEMA, header.meta.dataType);
        EXPECT_EQ(12, header.meta.reserve);
        char buf[8];
        header.toBuf(buf);
        cout << buf << endl;
        SchemaHeader result;
        result.fromBuf(buf);
        cout << "check: " << header.meta.checkval << ", dataType: " << int(header.meta.dataType)
             << ", reserve: " << header.meta.reserve << ", version: " << header.version << endl;
        EXPECT_EQ(header.meta.checkval, result.meta.checkval);
        EXPECT_EQ(header.meta.dataType, result.meta.dataType);
        EXPECT_EQ(header.meta.reserve, result.meta.reserve);
        uint32_t pp = header.pack >> 20;
        pp &= 0b111;
        cout << header.pack << " " << bitset<sizeof(unsigned int) * 8>(header.pack) << " " << pp << endl;
        EXPECT_TRUE(header == result);
    }
}

TEST_F(FieldSchemaTest, testGetCheckVal) {
    {
        uint32_t check = 0xFFFFFFFF;
        cout << check << endl;
        uint32_t actual = SchemaHeader::getCheckval(check);
        SchemaHeader head;
        head.meta.checkval = check;
        EXPECT_EQ(actual, head.meta.checkval);
        EXPECT_FALSE(check == actual);
    }
    {
        uint32_t check = 0;
        cout << check << endl;
        uint32_t actual = SchemaHeader::getCheckval(check);
        SchemaHeader head;
        head.meta.checkval = check;
        EXPECT_EQ(actual, head.meta.checkval);
        EXPECT_EQ(check, head.meta.checkval);
    }
    {
        uint32_t check = 0xFFFFF;
        cout << check << endl;
        uint32_t actual = SchemaHeader::getCheckval(check);
        SchemaHeader head;
        head.meta.checkval = check;
        EXPECT_EQ(actual, head.meta.checkval);
        EXPECT_EQ(check, head.meta.checkval);
    }
}

}; // namespace common
}; // namespace swift
