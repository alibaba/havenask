#include "sql/config/TableWriteConfig.h"

#include <iosfwd>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/FileSystem.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
namespace sql {

class TableWriteConfigTest : public TESTBASE {
public:
    void setUp() override;
    void tearDown() override;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(config, TableWriteConfigTest);

void TableWriteConfigTest::setUp() {}

void TableWriteConfigTest::tearDown() {}

TEST_F(TableWriteConfigTest, testSimple) {
    string fileName = GET_PRIVATE_TEST_DATA_PATH_WITHIN_TEST() + "/table_write_config.json";
    string content;
    ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::readFile(fileName, content));
    TableWriteConfig walConfig;
    FastFromJsonString(walConfig, content);
    ASSERT_EQ(2, walConfig.zoneNames.size());
    ASSERT_EQ("a", walConfig.zoneNames[0]);
    ASSERT_EQ("b", walConfig.zoneNames[1]);
}

} // end namespace sql
