#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/ErrorDefine.h>
#include <ha3/common/ErrorResult.h>

using namespace std;
BEGIN_HA3_NAMESPACE(common);

class ErrorResultTest : public TESTBASE {
public:
    ErrorResultTest();
    ~ErrorResultTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, ErrorResultTest);


ErrorResultTest::ErrorResultTest() { 
}

ErrorResultTest::~ErrorResultTest() { 
}

void ErrorResultTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void ErrorResultTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(ErrorResultTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");
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
    HA3_LOG(DEBUG, "Begin Test!");

    string msg = string("test getErrorDescription");
    string expectDescription = haErrorCodeToString(ERROR_PARTIAL_SEARCHER) + " " + msg;
    ErrorResult errorResult(ERROR_PARTIAL_SEARCHER, msg);
    errorResult.setHostInfo("partitionid", "hostName");
    ASSERT_EQ(expectDescription, errorResult.getErrorDescription());
    ErrorResult errorResult2 = errorResult;
    ASSERT_EQ(errorResult.toXMLString(), errorResult2.toXMLString());
}

END_HA3_NAMESPACE(common);

