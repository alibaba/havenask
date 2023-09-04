#include "sql/resource/SqlConfigResource.h"

#include <iosfwd>
#include <string>

#include "autil/Log.h"
#include "navi/tester/NaviResourceHelper.h"
#include "sql/resource/SqlConfig.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace sql {

class SqlConfigResourceTest : public TESTBASE {
public:
    void setUp() override;
    void tearDown() override;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(sql, SqlConfigResourceTest);

void SqlConfigResourceTest::setUp() {}

void SqlConfigResourceTest::tearDown() {}

TEST_F(SqlConfigResourceTest, testSimple) {
    string sqlConfigStr = string(
        R"json({"iquan_sql_config":{"catalog_name":"default1", "db_name":"default2"}})json");
    navi::NaviResourceHelper naviRes;
    naviRes.config(SqlConfigResource::RESOURCE_ID, sqlConfigStr);
    SqlConfigResource *sqlConfigResource = nullptr;
    ASSERT_TRUE(naviRes.getOrCreateRes(sqlConfigResource));
    ASSERT_EQ("default1", sqlConfigResource->_sqlConfig.catalogName);
    ASSERT_EQ("default2", sqlConfigResource->_sqlConfig.dbName);
}
} // namespace sql
