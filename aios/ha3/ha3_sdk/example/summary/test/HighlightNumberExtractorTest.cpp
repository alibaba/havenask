#include <ha3_sdk/testlib/summary/SummaryTestHelper.h>
#include <ha3_sdk/example/summary/HighlightNumberExtractor.h>
#include <unittest/unittest.h>
#include <ha3/util/Log.h>

using namespace std;
BEGIN_HA3_NAMESPACE(summary);

class HighlightNumberExtractorTest : public TESTBASE {
public:
    void setUp() {}
    void tearDown() {}
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(summary, HighlightNumberExtractorTest);

TEST_F(HighlightNumberExtractorTest, testSimpleProcess) {
    HA3_LOG(DEBUG, "Begin Test!");

    SummaryTestParam param;
    param.fakeIndex.attributes = "price:int32_t:1,2,3,4,5;"
                                 "title:string:照相机,mp3,phone,car,pc;"
                                 "nickname:string:aaa,bbb,ccc,1234,4321";
    param.fakeIndex.summarys = "title;nickname";
    param.summarySchemaStr = "title:ft_text:false;nickname:ft_string:false";
    param.docIdStr = "0,2,4";
    param.expectedSummaryStr = "照相机,aaa,<font color=red>1</font>;"
                               "phone,ccc,<font color=red>3</font>;"
                               "pc,<font color=red>4321</font>,<font color=red>5</font>";

    SummaryTestHelper helper("HighlightNumberExtractorTest");
    ASSERT_TRUE(helper.init(&param));

    vector<string> attrNames;
    attrNames.push_back("price");
    HighlightNumberExtractor *extractor = new HighlightNumberExtractor(attrNames);
    ASSERT_TRUE(helper.beginRequest(extractor));

    ASSERT_TRUE(helper.extractSummarys(extractor));

    delete extractor;
}

END_HA3_NAMESPACE(summary);
