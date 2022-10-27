#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/MultiErrorResult.h>

using namespace std;
using namespace autil;
BEGIN_HA3_NAMESPACE(common);

class MultiErrorResultTest : public TESTBASE {
public:
    MultiErrorResultTest();
    ~MultiErrorResultTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, MultiErrorResultTest);


MultiErrorResultTest::MultiErrorResultTest() { 
}

MultiErrorResultTest::~MultiErrorResultTest() { 
}

void MultiErrorResultTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void MultiErrorResultTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(MultiErrorResultTest, testEmptyMultiError){
    ErrorResult result1;
    ErrorResult result2;
    ErrorResult result3;

    MultiErrorResult result;
    result.addErrorResult(result1);
    result.addErrorResult(result2);
    result.addErrorResult(result3);

    ASSERT_EQ(string("<Error>\n\t<ErrorCode>0</ErrorCode>\n\t<ErrorDescription></ErrorDescription>\n</Error>\n"), result.toXMLString());
    
    MultiErrorResult result4;
    result4.addErrorResult(result1);
    result4.addErrorResult(result2);
    result4.addErrorResult(result1);

    ASSERT_EQ(string("<Error>\n\t<ErrorCode>0</ErrorCode>\n\t<ErrorDescription></ErrorDescription>\n</Error>\n"), result4.toXMLString());
}

TEST_F(MultiErrorResultTest, testToXmlString){
    ErrorResult result(ERROR_PARTIAL_SEARCHER, "searcher_1 & searcher_2 failed");
    MultiErrorResult multiErrorResult;
    multiErrorResult.addErrorResult(result);
    string expectedStr = "<Error>\n\
	<ErrorCode>2</ErrorCode>\n\
	<ErrorDescription><![CDATA[general error.]]></ErrorDescription>\n\
<Error>\n\
\t<PartitionID></PartitionID>\n\
\t<HostName></HostName>\n\
\t<ErrorCode>20005</ErrorCode>\n\
\t<ErrorDescription><![CDATA[some searchers have error. searcher_1 & searcher_2 failed]]></ErrorDescription>\n\
</Error>\n\
</Error>\n";
    ASSERT_EQ(expectedStr, multiErrorResult.toXMLString());
}

TEST_F(MultiErrorResultTest, testAddErrorResult){
    ErrorResult result1(ERROR_UNKNOWN);
    ErrorResult result2(ERROR_PARTIAL_SEARCHER);
    ErrorResult result3;
    ErrorResult emptyErrorResult1;
    ErrorResult emptyErrorResult2;

    MultiErrorResult result;
    result.addErrorResult(result1);
    result.addErrorResult(result2);
    result.addErrorResult(result3);
    
    ASSERT_TRUE(result.hasError());
    const ErrorResults &childErrors = result.getErrorResults();
    ASSERT_EQ((size_t)2, childErrors.size());
    ASSERT_EQ(ERROR_UNKNOWN, childErrors[0].getErrorCode());
    ASSERT_EQ(ERROR_PARTIAL_SEARCHER, childErrors[1].getErrorCode());
    string expectedStr = "<Error>\n\
	<ErrorCode>2</ErrorCode>\n\
	<ErrorDescription><![CDATA[general error.]]></ErrorDescription>\n\
<Error>\n\
\t<PartitionID></PartitionID>\n\
\t<HostName></HostName>\n\
\t<ErrorCode>1</ErrorCode>\n\
\t<ErrorDescription><![CDATA[unknow error.]]></ErrorDescription>\n\
</Error>\n\
<Error>\n\
\t<PartitionID></PartitionID>\n\
\t<HostName></HostName>\n\
\t<ErrorCode>20005</ErrorCode>\n\
\t<ErrorDescription><![CDATA[some searchers have error.]]></ErrorDescription>\n\
</Error>\n\
</Error>\n";

    ASSERT_EQ(expectedStr, result.toXMLString());
    
    MultiErrorResult result5;
    result5.addErrorResult(emptyErrorResult1);
    result5.addErrorResult(result2);
    result5.addErrorResult(emptyErrorResult2);

    ASSERT_TRUE(result5.hasError());
    const ErrorResults &childErrors3 = result5.getErrorResults();
    ASSERT_EQ((size_t)1, childErrors3.size());

    expectedStr = "<Error>\n\
	<ErrorCode>2</ErrorCode>\n\
	<ErrorDescription><![CDATA[general error.]]></ErrorDescription>\n\
<Error>\n\
\t<PartitionID></PartitionID>\n\
\t<HostName></HostName>\n\
\t<ErrorCode>20005</ErrorCode>\n\
\t<ErrorDescription><![CDATA[some searchers have error.]]></ErrorDescription>\n\
</Error>\n\
</Error>\n";
    ASSERT_EQ(expectedStr, result5.toXMLString());
}


TEST_F(MultiErrorResultTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");

    MultiErrorResult result;

    ErrorResult result2;
    result2.setHostInfo("partition_id_2", "hostName");
    result2.setErrorCode(ERROR_QRS_NOT_FOUND_CHAIN);

    result.addErrorResult(result2);

    ErrorResult result3;
    result3.setHostInfo("partition_id_3", "");
    result3.setErrorCode(ERROR_QRS_SEARCH_ERROR);

    result.addErrorResult(result3);

    string errorXML = "<Error>\n\
\t<ErrorCode>2</ErrorCode>\n\
\t<ErrorDescription><![CDATA[general error.]]></ErrorDescription>\n\
<Error>\n\
\t<PartitionID>partition_id_2</PartitionID>\n\
\t<HostName>hostName</HostName>\n\
\t<ErrorCode>10005</ErrorCode>\n\
\t<ErrorDescription><![CDATA[Not found chain.]]></ErrorDescription>\n\
</Error>\n\
<Error>\n\
\t<PartitionID>partition_id_3</PartitionID>\n\
\t<HostName></HostName>\n\
\t<ErrorCode>10015</ErrorCode>\n\
\t<ErrorDescription><![CDATA[Qrs search error.]]></ErrorDescription>\n\
</Error>\n\
<Error>\n\
\t<PartitionID>partition_id_5</PartitionID>\n\
\t<HostName></HostName>\n\
\t<ErrorCode>30005</ErrorCode>\n\
\t<ErrorDescription><![CDATA[RankProfile not exist.]]></ErrorDescription>\n\
</Error>\n\
</Error>\n\
";

    ErrorResult result5;
    result5.setHostInfo("partition_id_5", "");
    result5.setErrorCode(ERROR_NO_RANKPROFILE);

    result.addErrorResult(result5);
    ASSERT_EQ(errorXML, result.toXMLString());

    autil::DataBuffer dataBuffer;
    result.serialize(dataBuffer);
    MultiErrorResult result4;
    result4.deserialize(dataBuffer);

    ASSERT_EQ(errorXML, result4.toXMLString());

    result._mErrorResult[0].setHostInfo("", "hostname");
    result.setHostInfo("partition_id_6", "new_hostname");
    EXPECT_EQ("", result._mErrorResult[0].getPartitionID());
    EXPECT_EQ("hostname", result._mErrorResult[0].getHostName());

    result._mErrorResult[0].setHostInfo("", "");
    result.setHostInfo("partition_id_6", "new_hostname");
    EXPECT_EQ("partition_id_6", result._mErrorResult[0].getPartitionID());
    EXPECT_EQ("new_hostname", result._mErrorResult[0].getHostName());
}

TEST_F(MultiErrorResultTest, testHasErrorCode){
    ErrorResult result1(ERROR_UNKNOWN);
    ErrorResult result2(ERROR_PARTIAL_SEARCHER);
    ErrorResult result3(ERROR_RESPONSE_EARLY_TERMINATOR);
    MultiErrorResult result;
    ASSERT_TRUE(!result.hasErrorCode(ERROR_RESPONSE_EARLY_TERMINATOR));
    result.addErrorResult(result1);
    result.addErrorResult(result2);
    ASSERT_TRUE(!result.hasErrorCode(ERROR_RESPONSE_EARLY_TERMINATOR));
    result.addErrorResult(result3);
    ASSERT_TRUE(result.hasErrorCode(ERROR_RESPONSE_EARLY_TERMINATOR));
}


END_HA3_NAMESPACE(common);

