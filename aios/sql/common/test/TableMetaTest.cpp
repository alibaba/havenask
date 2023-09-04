#include "sql/common/TableMeta.h"

#include <iosfwd>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "sql/common/IndexInfo.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;

namespace sql {

class TableMetaTest : public TESTBASE {};

TEST_F(TableMetaTest, testExtractIndexInfos) {
    string metaStr = R"json({
        "field_meta": [
            {
                "field_type": "TEXT",
                "field_name": "$index2",
                "index_name": "index2",
                "index_type": "TEXT2"
            },
            {
                "field_type": "int32",
                "field_name": "$count"
            }
        ]
    })json";
    TableMeta meta;
    FastFromJsonString(meta, metaStr);
    ASSERT_EQ(2, meta.fieldsMeta.size());
    ASSERT_EQ("TEXT2", meta.fieldsMeta[0].indexType);
    ASSERT_EQ("index2", meta.fieldsMeta[0].indexName);
    ASSERT_EQ("$index2", meta.fieldsMeta[0].fieldName);
    ASSERT_EQ("TEXT", meta.fieldsMeta[0].fieldType);

    ASSERT_EQ("", meta.fieldsMeta[1].indexType);
    ASSERT_EQ("", meta.fieldsMeta[1].indexName);
    ASSERT_EQ("$count", meta.fieldsMeta[1].fieldName);
    ASSERT_EQ("int32", meta.fieldsMeta[1].fieldType);

    map<string, IndexInfo> indexInfos;
    meta.extractIndexInfos(indexInfos);
    ASSERT_EQ(1, indexInfos.size());
    ASSERT_TRUE(indexInfos.find("$index2") != indexInfos.end());
    ASSERT_EQ("index2", indexInfos["$index2"].name);
    ASSERT_EQ("TEXT2", indexInfos["$index2"].type);
}

TEST_F(TableMetaTest, testGetPkIndexName) {
    string metaStr = R"json({
        "field_meta": [
            {
                "field_type": "TEXT",
                "field_name": "index2",
                "index_name": "index2",
                "index_type": "TEXT2"
            },
            {
                "field_type": "int32",
                "field_name": "pk",
                "index_name": "pk",
                "index_type": "primarykey64"
            }
        ]
    })json";
    TableMeta meta;
    FastFromJsonString(meta, metaStr);
    ASSERT_EQ("pk", meta.getPkIndexName());
}

} // namespace sql
