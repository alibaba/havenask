#include "iquan/common/catalog/InspectDef.h"

#include <string>

#include "autil/Log.h"
#include "autil/legacy/exception.h"
#include "iquan/common/IquanException.h"
#include "iquan/common/Status.h"
#include "iquan/common/Utils.h"
#include "unittest/unittest.h"

using namespace testing;

namespace iquan {

class InspectDefTest : public TESTBASE {
private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(iquan, InspectDefTest);

// test suit of Ha3ClusterDef
TEST_F(InspectDefTest, testInspectTableDef) {
    // normal case
    {
        std::string normalInspectTableStr = R"json(
{
    "catalog_name" : "test",
    "database_name" : "phone",
    "table_name" : "phone_table"
}
)json";

        InspectTableDef inspectTableDef;
        Status status = Utils::fromJson(inspectTableDef, normalInspectTableStr);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
        ASSERT_EQ("test", inspectTableDef.catalogName);
        ASSERT_EQ("phone", inspectTableDef.databaseName);
        ASSERT_EQ("phone_table", inspectTableDef.tableName);
    }

    // bad case
    {
        std::string normalInspectTableStr = R"json(
{
    "database_name" : "phone",
    "table_name" : "phone_table"
}
)json";

        InspectTableDef inspectTableDef;
        Status status = Utils::fromJson(inspectTableDef, normalInspectTableStr);
        ASSERT_FALSE(status.ok());
    }
}

TEST_F(InspectDefTest, testInspectFunctionDef) {
    // normal case
    {
        std::string normalInspectFunctionStr = R"json(
{
    "catalog_name" : "test",
    "database_name" : "phone",
    "function_name" : "phone_function"
}
)json";

        InspectFunctionDef inspectFunctionDef;
        Status status = Utils::fromJson(inspectFunctionDef, normalInspectFunctionStr);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
        ASSERT_EQ("test", inspectFunctionDef.catalogName);
        ASSERT_EQ("phone", inspectFunctionDef.databaseName);
        ASSERT_EQ("phone_function", inspectFunctionDef.functionName);
    }

    // bad case
    {
        std::string normalInspectFunctionStr = R"json(
{
    "catalog_name" : "test",
    "database_name" : "phone"
}
)json";

        InspectFunctionDef inspectFunctionDef;
        Status status = Utils::fromJson(inspectFunctionDef, normalInspectFunctionStr);
        ASSERT_FALSE(status.ok());
    }
}

TEST_F(InspectDefTest, testInspectDataBaseDef) {
    // normal case
    {
        std::string normalInspectDataBaseStr = R"json(
{
    "catalog_name" : "test",
    "database_name" : "phone"
}
)json";

        InspectDataBaseDef databaseDef;
        Status status = Utils::fromJson(databaseDef, normalInspectDataBaseStr);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
        ASSERT_EQ("test", databaseDef.catalogName);
        ASSERT_EQ("phone", databaseDef.databaseName);
    }

    // bad case
    {
        std::string normalInspectDataBaseStr = R"json(
{
    "database_name" : "phone"
}
)json";

        InspectDataBaseDef databaseDef;
        Status status = Utils::fromJson(databaseDef, normalInspectDataBaseStr);
        ASSERT_FALSE(status.ok());
    }
}

TEST_F(InspectDefTest, testInspectCatalogDef) {
    // normal case
    {
        std::string normalInspectCatalogStr = R"json(
{
    "catalog_name" : "test"
}
)json";

        InspectCatalogDef catalogDef;
        Status status = Utils::fromJson(catalogDef, normalInspectCatalogStr);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
        ASSERT_EQ("test", catalogDef.catalogName);
    }

    // bad case
    {
        std::string normalInspectCatalogStr = R"json(
{
}
)json";

        InspectCatalogDef catalogDef;
        Status status = Utils::fromJson(catalogDef, normalInspectCatalogStr);
        ASSERT_FALSE(status.ok());
    }
}

} // namespace iquan
