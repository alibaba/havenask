/**
 * @file   custom_logger_test.cpp
 * @author zongyuan.wuzy <zongyuan.wuzy@alibaba-inc.com>
 * @date   Tue Apr 18 10:01:32 2019
 *
 * @brief
 *
 *
 */
#include "aitheta_indexer/plugins/aitheta/util/custom_logger.h"

#include <string>
#include <map>
#include <unittest/unittest.h>
#include "aitheta_indexer/test/test.h"
#include "aitheta_indexer/plugins/aitheta/common_define.h"

using namespace std;

IE_NAMESPACE_BEGIN(aitheta_plugin);

class CustomLoggerTest : public ::testing::Test {
 public:
    CustomLoggerTest() {}

    virtual ~CustomLoggerTest() {}

    virtual void SetUp() {}

    virtual void TearDown() {}

 protected:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(aitheta_plugin, CustomLoggerTest);

TEST_F(CustomLoggerTest, testLog) {
    aitheta::IndexLogger::Pointer loggerPtr(new CustomLogger());
    aitheta::IndexLoggerBroker::Register(loggerPtr);
    aitheta::IndexLoggerBroker::Log(aitheta::IndexLogger::LEVEL_DEBUG, "test_file.cpp", 1, "%s %d", "hello", 1234);
    aitheta::IndexLoggerBroker::Log(aitheta::IndexLogger::LEVEL_INFO, "test_file.cpp", 1, "%s %d", "hello", 1234);
    aitheta::IndexLoggerBroker::Log(aitheta::IndexLogger::LEVEL_WARN, "test_file.cpp", 1, "%s %d", "hello", 1234);
    aitheta::IndexLoggerBroker::Log(aitheta::IndexLogger::LEVEL_ERROR, "test_file.cpp", 1, "%s %d", "hello", 1234);
    aitheta::IndexLoggerBroker::Log(aitheta::IndexLogger::LEVEL_FATAL, "test_file.cpp", 1, "%s %d", "hello", 1234);
}

IE_NAMESPACE_END(aitheta_plugin);
