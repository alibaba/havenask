#include "swift/common/SchemaReader.h"

#include <fstream>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Span.h"
#include "autil/TimeUtility.h"
#include "swift/common/FieldSchema.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;

namespace swift {
namespace common {

class SchemaReaderTest : public TESTBASE {
public:
    void setUp() {}
    void tearDown() {}
};

TEST_F(SchemaReaderTest, testGenerateFieldMeta) {
    SchemaReader reader;
    reader.generateFieldMeta();
    EXPECT_EQ(0, reader._fieldMeta.size());

    reader._schema.addField("f1");
    FieldItem item("f2");
    item.addSubField("f2-1");
    reader._schema.addField(item);
    reader.generateFieldMeta();

    EXPECT_EQ(2, reader._fieldMeta.size());
    string f1("f1"), f2("f2"), f21("f2-1");
    EXPECT_EQ(StringView("f1"), reader._fieldMeta[0].key);
    EXPECT_EQ(StringView("f2"), reader._fieldMeta[1].key);
    EXPECT_EQ(1, reader._fieldMeta[1].subKeys.size());
    EXPECT_EQ(StringView("f2-1"), reader._fieldMeta[1].subKeys[0]);
}

TEST_F(SchemaReaderTest, testLoadSchema) {
    // SWIFT_ROOT_LOG_SETLEVEL(INFO);
    string schemaStr;
    SchemaReader reader;
    EXPECT_FALSE(reader.loadSchema(schemaStr));

    // miss fields
    schemaStr = R"json({"topic":"mainse","others":"test"})json";
    EXPECT_FALSE(reader.loadSchema(schemaStr));

    // fields invalid
    schemaStr = R"json({"topic":"mainse","fields":"test"})json";
    EXPECT_FALSE(reader.loadSchema(schemaStr));

    // miss topic
    schemaStr = R"json({"fields":[{"name":"f1","type":"string"}]})json";
    EXPECT_FALSE(reader.loadSchema(schemaStr));

    // invalid type
    schemaStr = R"json({"topic":"mainse","fields":[{"name":"f1","type":"int"}]})json";
    EXPECT_FALSE(reader.loadSchema(schemaStr));

    // invalid sub type
    schemaStr = R"json(
{"topic":"mainse",
"fields":[
{"name":"f2","hasSubFields":true,
"subFields":[{"name":"f2-1","type":"int"}]}
]
}
)json";
    EXPECT_FALSE(reader.loadSchema(schemaStr));

    // invalid json
    schemaStr = R"json({"topic":"mainse","fields":[{"name":"f1","type":"string"]})json";
    EXPECT_FALSE(reader.loadSchema(schemaStr));

    // valid
    schemaStr = R"json({"topic":"mainse","fields":[{"name":"f1","type":"string"}]})json";
    EXPECT_TRUE(reader.loadSchema(schemaStr));
    EXPECT_EQ(1, reader._fieldMeta.size());
    EXPECT_EQ(StringView("f1"), reader._fieldMeta[0].key);

    // valid sub
    schemaStr = R"json(
{"topic":"mainse",
"fields":[
{"name":"f2","hasSubFields":true,
"subFields":[{"name":"f2-1","type":"string"}]}
]
}
)json";
    EXPECT_TRUE(reader.loadSchema(schemaStr));
    EXPECT_EQ(1, reader._fieldMeta.size());
    EXPECT_EQ(StringView("f2"), reader._fieldMeta[0].key);
    EXPECT_EQ(1, reader._fieldMeta[0].subKeys.size());
    EXPECT_EQ(StringView("f2-1"), reader._fieldMeta[0].subKeys[0]);
}

TEST_F(SchemaReaderTest, testReadVersion) {
    SchemaReader reader;
    {
        char *data = NULL;
        int32_t version = 1000;
        EXPECT_FALSE(reader.readVersion(data, version));
    }
    {
        string data(sizeof(SchemaHeader), 'a');
        int32_t version = 1000;
        EXPECT_TRUE(reader.readVersion(data.c_str(), version));
        uint32_t check = *(uint32_t *)"aaaa";
        EXPECT_EQ(check, version);
    }
    {
        string data(sizeof(SchemaHeader) + 1, 'a');
        int32_t version = 1000;
        EXPECT_TRUE(reader.readVersion(data.c_str(), version));
        int32_t check = *(uint32_t *)"aaaa";
        EXPECT_EQ(check, version);
    }
    {
        string data(sizeof(SchemaHeader), 0);
        int32_t version = 1000;
        EXPECT_FALSE(reader.readVersion(data.c_str(), version));
        EXPECT_EQ(0, version);
    }
}

}; // namespace common
}; // namespace swift
