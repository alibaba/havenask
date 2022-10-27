#include "build_service/test/unittest.h"
#include "build_service/task_base/ConfigDownloader.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/util/test/FileUtilForTest.h"

using namespace std;
using namespace testing;
using namespace build_service::util;
using namespace build_service::config;

namespace build_service {
namespace task_base {

class ConfigDownloaderTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
};

void ConfigDownloaderTest::setUp() {
}

void ConfigDownloaderTest::tearDown() {
}

TEST_F(ConfigDownloaderTest, testDownloadLocalConfig) {
    string src = TEST_DATA_PATH"/config_example";
    string dst = GET_TEST_DATA_PATH();
    ASSERT_EQ(ConfigDownloader::DEC_NONE, ConfigDownloader::downloadConfig(src, dst));
    EXPECT_TRUE(FileUtilForTest::checkPathExist(dst + "/" + HIPPO_FILE_NAME));
    EXPECT_TRUE(FileUtilForTest::checkPathExist(dst + "/data_tables/simple_table.json"));

    // delete some file and config will not be downloaded.
    EXPECT_TRUE(FileUtil::remove(dst + "/" + HIPPO_FILE_NAME));
    dst = GET_TEST_DATA_PATH();
    ASSERT_EQ(ConfigDownloader::DEC_NONE, ConfigDownloader::downloadConfig(src, dst));
    EXPECT_FALSE(FileUtilForTest::checkPathExist(dst + "/" + HIPPO_FILE_NAME));
    EXPECT_TRUE(FileUtilForTest::checkPathExist(dst + "/data_tables/simple_table.json"));
}

}
}
