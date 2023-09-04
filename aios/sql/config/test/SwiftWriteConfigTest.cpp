#include "sql/config/SwiftWriteConfig.h"

#include <iosfwd>
#include <map>
#include <string>

#include "autil/Log.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/FileSystem.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
namespace sql {

class SwiftWriteConfigTest : public TESTBASE {
public:
    void setUp() override;
    void tearDown() override;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(config, SwiftWriteConfigTest);

void SwiftWriteConfigTest::setUp() {}

void SwiftWriteConfigTest::tearDown() {}

TEST_F(SwiftWriteConfigTest, testSimple) {
    string fileName = GET_PRIVATE_TEST_DATA_PATH_WITHIN_TEST() + "/swift_write_config.json";
    string content;
    ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::readFile(fileName, content));
    SwiftWriteConfig walConfig;
    FastFromJsonString(walConfig, content);
    ASSERT_EQ("client1", walConfig._swiftClientConfig);
    ASSERT_EQ(2, walConfig._tableReadWriteConfigMap.size());
    SwiftReaderWriterConfig config;
    ASSERT_TRUE(walConfig.getTableReadWriteConfig("table1", config));
    ASSERT_EQ("topic1", config.swiftTopicName);
    ASSERT_EQ("reader1", config.swiftReaderConfig);
    ASSERT_EQ("writer1", config.swiftWriterConfig);
    ASSERT_TRUE(walConfig.getTableReadWriteConfig("table2", config));
    ASSERT_EQ("topic2", config.swiftTopicName);
    ASSERT_EQ("reader2", config.swiftReaderConfig);
    ASSERT_EQ("writer2", config.swiftWriterConfig);
    ASSERT_FALSE(walConfig.getTableReadWriteConfig("table3", config));
}

} // end namespace sql
