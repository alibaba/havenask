#include "ha3/common/ErrorResult.h"

#include <iosfwd>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/ErrorDefine.h"
#include "unittest/unittest.h"

using namespace std;
namespace isearch {
namespace common {

class ErrorResultTest : public TESTBASE {
public:
    ErrorResultTest();
    ~ErrorResultTest();

public:
    void setUp();
    void tearDown();

protected:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(ha3, ErrorResultTest);

ErrorResultTest::ErrorResultTest() {}

ErrorResultTest::~ErrorResultTest() {}

void ErrorResultTest::setUp() {}

void ErrorResultTest::tearDown() {}

TEST_F(ErrorResultTest, testSimpleProcess) {
    string msg = string("test getErrorDescription");
    string expectDescription = haErrorCodeToString(ERROR_PARTIAL_SEARCHER) + " " + msg;
    ErrorResult errorResult(ERROR_PARTIAL_SEARCHER, msg);
    errorResult.setHostInfo("partitionid", "hostName");
    ASSERT_EQ(expectDescription, errorResult.getErrorDescription());

    string expectStr = "<Error>\n\
\t<PartitionID>partitionid</PartitionID>\n\
\t<HostName>hostName</HostName>\n\
\t<ErrorCode>20005</ErrorCode>\n\
\t<ErrorDescription><![CDATA[some searchers have error. test getErrorDescription]]></ErrorDescription>\n\
</Error>\n";
    ASSERT_EQ(expectStr, errorResult.toXMLString());
}

TEST_F(ErrorResultTest, testAssignFun) {

    string msg = string("test getErrorDescription");
    string expectDescription = haErrorCodeToString(ERROR_PARTIAL_SEARCHER) + " " + msg;
    ErrorResult errorResult(ERROR_PARTIAL_SEARCHER, msg);
    errorResult.setHostInfo("partitionid", "hostName");
    ASSERT_EQ(expectDescription, errorResult.getErrorDescription());
    ErrorResult errorResult2 = errorResult;
    ASSERT_EQ(errorResult.toXMLString(), errorResult2.toXMLString());
}

TEST_F(ErrorResultTest, testJsonString) {

    {
        string msg = string("test getErrorDescription");
        string expectDescription = haErrorCodeToString(ERROR_PARTIAL_SEARCHER) + " " + msg;
        ErrorResult errorResult(ERROR_PARTIAL_SEARCHER, msg);
        errorResult.setHostInfo("partitionid", "hostName");
        errorResult.setErrorCode(ERROR_UNKNOWN);
        std::string result = R"foo({
"partId": partitionid,
"hostname": hostName,
"errorCode": 1,
"errorMsg": unknow error. test getErrorDescription
}
)foo";
        EXPECT_EQ(result, errorResult.toJsonString());
    }

    {
        string msg = string("test getErrorDescription");
        string expectDescription = haErrorCodeToString(ERROR_PARTIAL_SEARCHER) + " " + msg;
        ErrorResult errorResult(ERROR_PARTIAL_SEARCHER, msg);
        errorResult.setErrorCode(ERROR_UNKNOWN);
        std::string result = R"foo({
"errorCode": 1,
"errorMsg": unknow error. test getErrorDescription
}
)foo";
        EXPECT_EQ(result, errorResult.toJsonString());
    }
}

} // namespace common
} // namespace isearch
