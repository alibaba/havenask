#include "indexlib/util/ErrorLogCollector.h"

#include <cstdio>

#include "autil/EnvUtil.h"
#include "autil/Log.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "unittest/unittest.h"
using namespace std;
using namespace autil;
using namespace fslib;

using namespace fslib::fs;
using namespace indexlib::file_system;

namespace indexlib { namespace util {

class ErrorLogCollectorTest : public TESTBASE
{
public:
    void setUp() override;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.util, ErrorLogCollectorTest);

void ErrorLogCollectorTest::setUp() { setenv("COLLECT_ERROR_LOG", "true", 1); }

TEST_F(ErrorLogCollectorTest, TestSimpleProcess)
{
    DECLARE_ERROR_COLLECTOR_IDENTIFIER("abcde");
    ASSERT_TRUE(ErrorLogCollector::UsingErrorLogCollect());
    for (size_t i = 0; i < 64; i++) {
        ERROR_COLLECTOR_LOG(ERROR, "test [%s]", "error");
    }
    std::string content;
    FslibWrapper::AtomicLoadE("error_log_collector.log", content);
    for (size_t i = 0; i < 6; i++) {
        ASSERT_TRUE(strstr(content.c_str(), StringUtil::toString(1 << i).c_str()));
    }
    FslibWrapper::DeleteFileE("error_log_collector.log", DeleteOption::NoFence(false));
}

}} // namespace indexlib::util
