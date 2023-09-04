#include "build_service/common/ConfigDownloader.h"

#include "build_service/config/ConfigDefine.h"
#include "build_service/test/unittest.h"
#include "fslib/fs/FileSystem.h"

using namespace std;
using namespace testing;
using namespace fslib;
using namespace build_service::util;
using namespace build_service::config;

namespace build_service { namespace common {

class ConfigDownloaderTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();
};

void ConfigDownloaderTest::setUp() {}

void ConfigDownloaderTest::tearDown() {}

TEST_F(ConfigDownloaderTest, testDownloadLocalConfig)
{
    string src = GET_TEST_DATA_PATH() + "/config_example";
    string dst = GET_TEMP_DATA_PATH();
    ASSERT_EQ(ConfigDownloader::DEC_NONE, ConfigDownloader::downloadConfig(src, dst));
    EXPECT_EQ(EC_TRUE, fslib::fs::FileSystem::isExist(dst + "/" + HIPPO_FILE_NAME));
    EXPECT_EQ(EC_TRUE, fslib::fs::FileSystem::isExist(dst + "/data_tables/simple_table.json"));

    // delete some file and config will not be downloaded.
    EXPECT_TRUE(fslib::util::FileUtil::remove(dst + "/" + HIPPO_FILE_NAME));
    dst = GET_TEMP_DATA_PATH();
    ASSERT_EQ(ConfigDownloader::DEC_NONE, ConfigDownloader::downloadConfig(src, dst));
    EXPECT_EQ(EC_FALSE, fslib::fs::FileSystem::isExist(dst + "/" + HIPPO_FILE_NAME)) << dst + "/" + HIPPO_FILE_NAME;
    EXPECT_EQ(EC_TRUE, fslib::fs::FileSystem::isExist(dst + "/data_tables/simple_table.json"));
}

}} // namespace build_service::common
