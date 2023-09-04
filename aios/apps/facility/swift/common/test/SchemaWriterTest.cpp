#include "swift/common/SchemaWriter.h"

#include <iostream>
#include <stdint.h>
#include <string>
#include <unordered_map>
#include <utility>

#include "autil/TimeUtility.h"
#include "swift/common/FieldSchema.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;

namespace swift {
namespace common {

class SchemaWriterTest : public TESTBASE {
public:
    void setUp() {}
    void tearDown() {}
};

TEST_F(SchemaWriterTest, testGenerateFieldMap) {
    SchemaWriter writer;
    writer.generateFieldMap();
    EXPECT_EQ(0, writer._fieldMap.size());

    writer._schema.addField("f1");
    FieldItem item("f2");
    item.addSubField("f2-1");
    writer._schema.addField(item);
    writer.generateFieldMap();

    EXPECT_EQ(2, writer._fieldMap.size());
    auto iter = writer._fieldMap.find("f1");
    EXPECT_TRUE(writer._fieldMap.end() != iter);
    EXPECT_TRUE(iter->second.isNone);
    EXPECT_TRUE(iter->second.value.empty());
    EXPECT_EQ(0, iter->second.subFields.size());

    iter = writer._fieldMap.find("f2");
    EXPECT_TRUE(writer._fieldMap.end() != iter);
    EXPECT_TRUE(iter->second.isNone);
    EXPECT_EQ(1, iter->second.subFields.size());
    EXPECT_TRUE(iter->second.subFields.end() != iter->second.subFields.find("f2-1"));
    EXPECT_TRUE(iter->second.subFields["f2-1"]->isNone);
    EXPECT_TRUE(iter->second.subFields["f2-1"]->value.empty());
}

TEST_F(SchemaWriterTest, testLoadSchema) {
    string schemaStr;
    SchemaWriter writer;
    EXPECT_EQ(0, writer._version);
    EXPECT_FALSE(writer.loadSchema(schemaStr));

    // miss fields
    schemaStr = R"json({"topic":"mainse","others":"test"})json";
    EXPECT_FALSE(writer.loadSchema(schemaStr));

    // fields invalid
    schemaStr = R"json({"topic":"mainse","fields":"test"})json";
    EXPECT_FALSE(writer.loadSchema(schemaStr));

    // miss topic
    schemaStr = R"json({"fields":[{"name":"f1","type":"string"}]})json";
    EXPECT_FALSE(writer.loadSchema(schemaStr));

    // invalid type
    schemaStr = R"json({"topic":"mainse","fields":[{"name":"f1","type":"int"}]})json";
    EXPECT_FALSE(writer.loadSchema(schemaStr));
    EXPECT_EQ(0, writer._version);
    // invalid sub type
    schemaStr = R"json(
{"topic":"mainse",
"fields":[
{"name":"f2","hasSubFields":true,
"subFields":[{"name":"f2-1","type":"int"}]}
]
}
)json";
    EXPECT_FALSE(writer.loadSchema(schemaStr));

    // invalid json
    schemaStr = R"json({"topic":"mainse","fields":[{"name":"f1","type":"string"]})json";
    EXPECT_FALSE(writer.loadSchema(schemaStr));

    // valid
    schemaStr = R"json({"topic":"mainse","fields":[{"name":"f1","type":"string"}]})json";
    EXPECT_TRUE(writer.loadSchema(schemaStr));
    EXPECT_EQ(-651112405, writer._version);
    EXPECT_EQ(1, writer._fieldMap.size());
    auto iter = writer._fieldMap.find("f1");
    EXPECT_TRUE(writer._fieldMap.end() != iter);
    EXPECT_TRUE(iter->second.value.empty());

    // valid sub
    schemaStr = R"json(
{"topic":"mainse",
"fields":[
{"name":"f2","hasSubFields":true,
"subFields":[{"name":"f2-1","type":"string"}]}
]
}
)json";
    EXPECT_TRUE(writer.loadSchema(schemaStr));
    EXPECT_EQ(-1301904512, writer._version);
    EXPECT_EQ(1, writer._fieldMap.size());
    iter = writer._fieldMap.find("f2");
    EXPECT_TRUE(writer._fieldMap.end() != iter);
    EXPECT_EQ(1, iter->second.subFields.size());
    EXPECT_TRUE(iter->second.isNone);
    auto subIter = iter->second.subFields.find("f2-1");
    EXPECT_TRUE(iter->second.subFields.end() != subIter);
    EXPECT_TRUE(subIter->second->isNone);
}

TEST_F(SchemaWriterTest, testGetVersion) {
    SchemaWriter writer;
    string schemaStr = R"json({"topic":"mainse","fields":[{"name":"f1","type":"string"}]})json";
    EXPECT_TRUE(writer.loadSchema(schemaStr));
    int32_t version = writer._schema.calcVersion();
    EXPECT_EQ(version, writer.getVersion());
    writer.setVersion(-10);
    EXPECT_EQ(-10, writer.getVersion());
}

TEST_F(SchemaWriterTest, testSetVersion) {
    SchemaWriter writer;
    writer.setVersion(100);
    EXPECT_EQ(100, writer._version);
}

TEST_F(SchemaWriterTest, testSmallFuncs) {
    SchemaWriter writer;
    string schemaStr = R"json(
{"topic":"mainse",
"fields":[
{"name":"f1","type":"string"},
{"name":"f2","hasSubFields":true,
"subFields":[{"name":"f2-1","type":"string"}]}
]
}
)json";
    EXPECT_TRUE(writer.loadSchema(schemaStr));
    {
        EXPECT_TRUE(writer.setField("f1", "11"));
        EXPECT_TRUE(writer.setField("f2", "22"));
        EXPECT_FALSE(writer.setField("f3", "33"));
        EXPECT_FALSE(writer.setField("f2-1", "21"));

        EXPECT_TRUE(writer.setSubField("f2", "f2-1", "21"));
        EXPECT_FALSE(writer.setSubField("f1", "f2-1", "212"));
        EXPECT_FALSE(writer.setSubField("f2", "f2-2", "213"));

        EXPECT_EQ(2, writer._fieldMap.size());
        EXPECT_FALSE(writer._fieldMap["f1"].isNone);
        EXPECT_EQ("11", writer._fieldMap["f1"].value);
        EXPECT_EQ(0, writer._fieldMap["f1"].subFields.size());
        EXPECT_FALSE(writer._fieldMap["f2"].isNone);
        EXPECT_EQ("22", writer._fieldMap["f2"].value);
        EXPECT_EQ(1, writer._fieldMap["f2"].subFields.size());
        EXPECT_FALSE(writer._fieldMap["f2"].subFields["f2-1"]->isNone);
        EXPECT_EQ("21", writer._fieldMap["f2"].subFields["f2-1"]->value);

        const string &result = writer.toString();
        cout << "result: " << result << endl;
        EXPECT_EQ(16, result.size()); // in case of change

        writer.reset();
        EXPECT_EQ(2, writer._fieldMap.size());
        EXPECT_TRUE(writer._fieldMap["f1"].isNone);
        EXPECT_TRUE(writer._fieldMap["f2"].isNone);
        EXPECT_EQ(1, writer._fieldMap["f2"].subFields.size());
        EXPECT_TRUE(writer._fieldMap["f2"].subFields["f2-1"]->isNone);
    }
    {
        EXPECT_TRUE(writer.setSubField("f2", "f2-1", "21"));
        EXPECT_EQ(2, writer._fieldMap.size());
        EXPECT_TRUE(writer._fieldMap["f1"].isNone);
        EXPECT_EQ(0, writer._fieldMap["f1"].subFields.size());
        EXPECT_TRUE(writer._fieldMap["f2"].isNone);
        EXPECT_EQ(1, writer._fieldMap["f2"].subFields.size());
        EXPECT_FALSE(writer._fieldMap["f2"].subFields["f2-1"]->isNone);

        const string &result = writer.toString();
        cout << "result: " << result << endl;
        EXPECT_EQ(10, result.size()); // in case of change
    }
}

}; // namespace common
}; // namespace swift
