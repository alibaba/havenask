#include "sql/common/IndexPartitionSchemaConverter.h"

#include <iostream>
#include <memory>
#include <string>

#include "autil/Log.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/base/Status.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/legacy_schema_adapter.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/util/PathUtil.h"
#include "iquan/common/catalog/TableDef.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace autil::legacy;
using namespace fslib::util;
namespace sql {

class IndexPartitionSchemaConverterTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
    void convert(const std::string &schemaPath, const std::string &tableModelPath);
    void convertTabletSchema(const std::string &schemaPath, const std::string &tableModelPath);

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(ha3, IndexPartitionSchemaConverterTest);

void IndexPartitionSchemaConverterTest::setUp() {}

void IndexPartitionSchemaConverterTest::tearDown() {}

void IndexPartitionSchemaConverterTest::convert(const std::string &schemaPath,
                                                const std::string &tableModelPath) {
    // read index partition schema
    string indexPartitionSchemaStr = FileUtil::readFile(schemaPath);
    indexlib::config::IndexPartitionSchemaPtr schemaPtr;
    FastFromJsonString(schemaPtr, indexPartitionSchemaStr);

    // convert to iquan schema
    iquan::TableDef tableDef;
    IndexPartitionSchemaConverter::convert(
        make_shared<indexlib::config::LegacySchemaAdapter>(schemaPtr), tableDef);
    std::cout << FastToJsonString(tableDef) << std::endl;
    // expected iquan schema
    string iquanSchemaStr = FileUtil::readFile(tableModelPath);
    iquan::TableDef expectedTableDef;
    FastFromJsonString(expectedTableDef, iquanSchemaStr);

    ASSERT_EQ(FastToJsonString(expectedTableDef), FastToJsonString(tableDef));
}

void IndexPartitionSchemaConverterTest::convertTabletSchema(const std::string &schemaPath,
                                                            const std::string &tableModelPath) {
    const auto &dir = indexlib::util::PathUtil::GetParentDirPath(schemaPath);
    const auto &fileName = indexlib::util::PathUtil::GetFileName(schemaPath);
    std::shared_ptr<indexlibv2::config::TabletSchema> schemaPtr
        = indexlibv2::framework::TabletSchemaLoader::LoadSchema(dir, fileName);
    ASSERT_TRUE(schemaPtr);
    auto status
        = indexlibv2::framework::TabletSchemaLoader::ResolveSchema(nullptr, "", schemaPtr.get());
    ASSERT_TRUE(status.IsOK());

    // convert to iquan schema
    iquan::TableDef tableDef;
    IndexPartitionSchemaConverter::convert(schemaPtr, tableDef);
    std::cout << FastToJsonString(tableDef) << std::endl;
    // expected iquan schema
    string iquanSchemaStr = FileUtil::readFile(tableModelPath);
    iquan::TableDef expectedTableDef;
    FastFromJsonString(expectedTableDef, iquanSchemaStr);

    ASSERT_EQ(FastToJsonString(expectedTableDef), FastToJsonString(tableDef));
}

TEST_F(IndexPartitionSchemaConverterTest, testConvertSimple) {
    std::string schemaPath = GET_PRIVATE_TEST_DATA_PATH_WITHIN_TEST()
                             + "testSchema/modified_schema/index_partition_schema.json";
    std::string tableModelPath = GET_PRIVATE_TEST_DATA_PATH_WITHIN_TEST()
                                 + "testSchema/modified_schema/iquan_table_schema.json";
    ASSERT_NO_FATAL_FAILURE(convert(schemaPath, tableModelPath));
}

TEST_F(IndexPartitionSchemaConverterTest, testConvertKV) {
    std::string schemaPath
        = GET_PRIVATE_TEST_DATA_PATH_WITHIN_TEST() + "testSchema/modified_schema/kv_schema.json";
    std::string tableModelPath
        = GET_PRIVATE_TEST_DATA_PATH_WITHIN_TEST() + "testSchema/modified_schema/kv_table.json";
    ASSERT_NO_FATAL_FAILURE(convert(schemaPath, tableModelPath));
}

TEST_F(IndexPartitionSchemaConverterTest, testConvertSimpleTabletSchema) {
    std::string schemaPath = GET_PRIVATE_TEST_DATA_PATH_WITHIN_TEST()
                             + "testSchema/modified_schema/tablet_schema.json";
    std::string tableModelPath = GET_PRIVATE_TEST_DATA_PATH_WITHIN_TEST()
                                 + "testSchema/modified_schema/iquan_tablet_schema.json";
    ASSERT_NO_FATAL_FAILURE(convertTabletSchema(schemaPath, tableModelPath));
}

TEST_F(IndexPartitionSchemaConverterTest, testConvertKVTabletSchema) {
    std::string schemaPath
        = GET_PRIVATE_TEST_DATA_PATH_WITHIN_TEST() + "testSchema/modified_schema/kv_schema.json";
    std::string tableModelPath
        = GET_PRIVATE_TEST_DATA_PATH_WITHIN_TEST() + "testSchema/modified_schema/kv_table.json";
    ASSERT_NO_FATAL_FAILURE(convertTabletSchema(schemaPath, tableModelPath));
}

TEST_F(IndexPartitionSchemaConverterTest, testConvertKKV) {
    std::string schemaPath
        = GET_PRIVATE_TEST_DATA_PATH_WITHIN_TEST() + "testSchema/modified_schema/kkv_schema.json";
    std::string tableModelPath
        = GET_PRIVATE_TEST_DATA_PATH_WITHIN_TEST() + "testSchema/modified_schema/kkv_table.json";
    ASSERT_NO_FATAL_FAILURE(convert(schemaPath, tableModelPath));
}


} // namespace sql
