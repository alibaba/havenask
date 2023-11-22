#include "swift/client/trace/SwiftErrorResponseCollector.h"

#include <iosfwd>
#include <limits.h>
#include <stdint.h>
#include <string>
#include <unistd.h>

#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "fslib/util/FileUtil.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/TraceMessage.pb.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace swift {
namespace client {

AUTIL_DECLARE_AND_SETUP_LOGGER(swift, SwiftErrorResponseCollectorTest);

class SwiftErrorResponseCollectorTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void SwiftErrorResponseCollectorTest::setUp() {}

void SwiftErrorResponseCollectorTest::tearDown() {}

TEST_F(SwiftErrorResponseCollectorTest, testSimple) {
    std::string cwd;
    char buf[PATH_MAX];
    if (getcwd(buf, PATH_MAX) == nullptr) {
        ASSERT_TRUE(false);
    }
    cwd = buf;
    string logFileName = cwd + "/logs/swift/err_response.log";
    string expectStr;
    int32_t expectLen = 0;
    {
        SwiftErrorResponseCollector collector;
        protocol::MessageResponse res;
        res.set_fbmsgs("err_response_aaaaa");
        string content;
        res.SerializeToString(&content);
        protocol::ReaderInfo readerInfo;
        readerInfo.set_topicname("err_topic_aaaa");
        collector.logResponse(readerInfo, MESSAGE_RESPONSE, content);

        protocol::WriteErrorResponse errResponse;
        *errResponse.mutable_readerinfo() = readerInfo;
        errResponse.set_type(MESSAGE_RESPONSE);
        errResponse.set_content(content);
        errResponse.SerializeToString(&expectStr);
        expectLen = sizeof(ErrorResponseLogHeader) + expectStr.size();
    }
    string readContent;
    ASSERT_TRUE(fslib::util::FileUtil::readFile(logFileName, readContent));
    ASSERT_EQ(expectLen, readContent.size());
    const ErrorResponseLogHeader *header = (const ErrorResponseLogHeader *)readContent.c_str();
    ASSERT_TRUE(header[0].timestamp <= autil::TimeUtility::currentTime());
    ASSERT_EQ(expectStr.size(), header[0].logLen);
    ASSERT_EQ(expectStr, readContent.substr(sizeof(ErrorResponseLogHeader)));
}

}; // namespace client
}; // namespace swift
