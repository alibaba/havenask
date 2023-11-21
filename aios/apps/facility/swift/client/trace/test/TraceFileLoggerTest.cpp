#include "swift/client/trace/TraceFileLogger.h"

#include <iosfwd>
#include <limits.h>
#include <string>
#include <unistd.h>

#include "autil/TimeUtility.h"
#include "fslib/util/FileUtil.h"
#include "swift/client/trace/TraceLogger.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace swift {
namespace client {

class TraceFileLoggerTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void TraceFileLoggerTest::setUp() {}

void TraceFileLoggerTest::tearDown() {}

TEST_F(TraceFileLoggerTest, testSimple) {
    std::string cwd;
    char buf[PATH_MAX];
    if (getcwd(buf, PATH_MAX) == nullptr) {
        ASSERT_TRUE(false);
    }
    cwd = buf;
    string logFileName = cwd + "/logs/swift/trace.log";
    string writeStr = "aaaaa";
    {
        TraceFileLogger::initLogger();
        TraceFileLogger logger;
        logger.write(writeStr);
    }
    string readContent;
    ASSERT_TRUE(fslib::util::FileUtil::readFile(logFileName, readContent));
    ASSERT_EQ(sizeof(LogHeader) + writeStr.size(), readContent.size());
    const LogHeader *header = (const LogHeader *)readContent.c_str();
    ASSERT_TRUE(header[0].timestamp <= autil::TimeUtility::currentTime());
    ASSERT_EQ(writeStr.size(), header[0].logLen);
    ASSERT_EQ(writeStr, readContent.substr(sizeof(LogHeader)));
}

}; // namespace client
}; // namespace swift
