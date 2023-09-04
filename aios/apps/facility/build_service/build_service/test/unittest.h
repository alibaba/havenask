#ifndef __BUILD_SERVICE_UNITTEST_H
#define __BUILD_SERVICE_UNITTEST_H

#include <chrono>
#include <dlfcn.h>
#include <execinfo.h>
#include <functional>
#include <string>
#include <thread>
#include <typeinfo>
#include <unistd.h>
#define GTEST_USE_OWN_TR1_TUPLE 0
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "build_service/util/Log.h"
#include "fslib/util/FileUtil.h"
#include "unittest/unittest.h"

#define UNIT_TEST UT

using testing::_;
using testing::DoAll;
using testing::Invoke;
using testing::Return;
using testing::SetArgReferee;
using testing::Throw;

class BUILD_SERVICE_TESTBASE : public TESTBASE
{
protected:
    void testDataPathSetup() override
    {
        // cd to temp directory which can be cleaned in tear down, avoid path conflict
        TESTBASE::testDataPathSetup();
        std::string testClassTempPath = GET_TEMP_DATA_PATH();
        if (testClassTempPath.size() >= 2 && testClassTempPath[0] == '.' && testClassTempPath[1] == '/') {
            // @testClassTempPath begin with "./", meas not runned by bazel, maybe gdb or run binary directly
            return;
        }

        if (0 != chdir(testClassTempPath.c_str())) {
            ASSERT_FALSE(true) << "chdir failed, " << testClassTempPath;
        }
    }

    void SetUp() override
    {
        setenv("INDEXLIB_TEST_MOCK_PANGU_INLINE_FILE", "true", 1);
        TESTBASE::SetUp();
    }

    void TearDown() override
    {
        TESTBASE::TearDown();
        unsetenv("INDEXLIB_TEST_MOCK_PANGU_INLINE_FILE");
    }
};

template <typename Duration>
bool timeWait(std::function<bool()> predicate, Duration checkDuration, size_t checkCount)
{
    size_t count = 0;
    for (;;) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(checkDuration);
        if (++count >= checkCount) {
            return false;
        }
    }
    return false;
}

AUTIL_DECLARE_AND_SETUP_LOGGER(build_service, test);

#endif //__BUILD_SERVICE_UNITTEST_H
