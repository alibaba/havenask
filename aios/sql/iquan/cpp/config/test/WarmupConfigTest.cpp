#include "iquan/config/WarmupConfig.h"

#include <string>

#include "autil/Log.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/test/JsonTestUtil.h"
#include "iquan/common/IquanException.h"
#include "iquan/common/Status.h"
#include "iquan/common/Utils.h"
#include "unittest/unittest.h"

using namespace testing;

namespace iquan {

class WarmupConfigTest : public TESTBASE {
private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(iquan, WarmupConfigTest);

TEST_F(WarmupConfigTest, testWarmupConfig) {
    {
        WarmupConfig config;
        config.threadNum = 0;
        config.warmupSeconds = 0;
        config.warmupQueryNum = 0;

        ASSERT_EQ(std::string(""), config.warmupFilePath);
        ASSERT_FALSE(config.isValid());

        config.threadNum = 10;
        ASSERT_FALSE(config.isValid());

        config.warmupSeconds = 20;
        ASSERT_FALSE(config.isValid());

        config.warmupQueryNum = 30;
        ASSERT_TRUE(config.isValid());

        // copy constructor
        WarmupConfig config2 = config;
        ASSERT_EQ(10, config2.threadNum);
        ASSERT_EQ(20, config2.warmupSeconds);
        ASSERT_EQ(30, config2.warmupQueryNum);
        ASSERT_TRUE(config2.isValid());
    }

    {
        std::string expectStr = R"json({
"thread_number":
  10,
"warmup_file_path":
  "fake_path",
"warmup_log_name":
  "sql_warmup",
"warmup_query_number":
  30,
"warmup_second_interval":
  20
})json";

        WarmupConfig config;
        config.threadNum = 10;
        config.warmupSeconds = 20;
        config.warmupQueryNum = 30;

        config.warmupFilePath = "fake_path";
        std::string actualStr;
        Status status = Utils::toJson(config, actualStr, false);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
        ASSERT_NO_FATAL_FAILURE(
            autil::legacy::JsonTestUtil::checkJsonStringEqual(expectStr, actualStr));
    }
}

} // namespace iquan
