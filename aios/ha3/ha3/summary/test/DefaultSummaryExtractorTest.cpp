#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/test/test.h>
#include <ha3/summary/DefaultSummaryExtractor.h>
#include <suez/turing/expression/util/TableInfoConfigurator.h>
#include <ha3/common/SummaryHit.h>
#include <ha3/summary/SummaryProfile.h>
#include <ha3/search/SearchCommonResource.h>

using namespace std;
using namespace suez::turing;
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(search);
BEGIN_HA3_NAMESPACE(summary);

class DefaultSummaryExtractorTest : public TESTBASE {
public:
    DefaultSummaryExtractorTest();
    ~DefaultSummaryExtractorTest();
public:
    void setUp();
    void tearDown();
protected:
    DefaultSummaryExtractor _summaryExtractor;
    config::FieldSummaryConfig _fieldInfo;
    std::map<size_t, ::cava::CavaJitModulePtr> _cavaJitModules;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(summary, DefaultSummaryExtractorTest);


static const string HIGHLIGHT_BEGIN_DELIMITER = "<em>";
static const string HIGHLIGHT_END_DELIMITER = "</em>";

DefaultSummaryExtractorTest::DefaultSummaryExtractorTest() {
}

DefaultSummaryExtractorTest::~DefaultSummaryExtractorTest() { 
}

void DefaultSummaryExtractorTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
    _fieldInfo._highlightPrefix = HIGHLIGHT_BEGIN_DELIMITER;
    _fieldInfo._highlightSuffix = HIGHLIGHT_END_DELIMITER;
    _fieldInfo._snippetDelimiter = "...";
    _fieldInfo._maxSummaryLength = 20;
}

void DefaultSummaryExtractorTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(DefaultSummaryExtractorTest, testDefaultProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");

    string text = "aaa\t \tＢｂb\t \tccc\t \tddd\t \teee\t.\t \t"
                  "fff\t \tiii\t \tfff\t \tＢＢＢ\t \tggg\t.\t \t"
                  "hhh\t \tlll\t \teee\t \twww\t \tddd\t.";
    vector<string> keyWords;
    keyWords.push_back("bbb");
    keyWords.push_back("ddd");
    
    _fieldInfo._maxSummaryLength = 24;
    string summary = _summaryExtractor.getSummary(text, keyWords, &_fieldInfo);
    string checkSummary = string() + "aaa " + HIGHLIGHT_BEGIN_DELIMITER + "Ｂｂb" + HIGHLIGHT_END_DELIMITER
                          + " ccc " + HIGHLIGHT_BEGIN_DELIMITER + "ddd" + HIGHLIGHT_END_DELIMITER + " eee....";
    ASSERT_EQ(checkSummary, summary);
}

TEST_F(DefaultSummaryExtractorTest, testKeywordOverlap) { 
    HA3_LOG(DEBUG, "Begin Test!");

    string text = "aaa\t \tbbb\t \tccc\t \tddd\t \teee\t.\t \t"
                  "fff\t \tiii\t \tfff\t \tbbb\t \tggg\t.\t \t"
                  "hhh\t \tlll\t \teee\t \twww\t \tddd\t.";
    vector<string> keyWords;
    keyWords.push_back("b");
    keyWords.push_back("bb");
    keyWords.push_back("bbb");
    keyWords.push_back("ddd");
    
    string summary = _summaryExtractor.getSummary(text, keyWords, &_fieldInfo);
    string checkSummary = string() + "aaa " + HIGHLIGHT_BEGIN_DELIMITER + "bbb" + HIGHLIGHT_END_DELIMITER
                          + " ccc " + HIGHLIGHT_BEGIN_DELIMITER + "ddd" + HIGHLIGHT_END_DELIMITER + " eee....";
    ASSERT_EQ(checkSummary, summary);
}

TEST_F(DefaultSummaryExtractorTest, testKeywordNoOccurrence) { 
    HA3_LOG(DEBUG, "Begin Test!");

    string text = "aaa\t \tbbb\t \tccc\t \tddd\t \teee\t.\t \t"
                  "fff\t \tiii\t \tfff\t \tbbb\t \tggg\t.\t \t"
                  "hhh\t \tlll\t \teee\t \twww\t \tddd\t.";
    vector<string> keyWords;
    keyWords.push_back("xxx");
    keyWords.push_back("y");
    
    string summary = _summaryExtractor.getSummary(text, keyWords, &_fieldInfo);
    string checkSummary = string() + "aaa bbb ccc ddd eee....";
    ASSERT_EQ(checkSummary, summary);
}

TEST_F(DefaultSummaryExtractorTest, testMaxUniqueKeyword) { 
    HA3_LOG(DEBUG, "Begin Test!");

    string text = "aaa\t \tbbb\t \tccc\t \tddd\t \teee\t.\t \t"
                  "fff\t \tiii\t \tfff\t \tbbb\t \tggg\t.\t \t"
                  "bbb\t \tbbb\t \tbbb\t \tbbb\t \tggg\t.\t \t"
                  "hhh\t \tlll\t \teee\t \twww\t \tddd\t.";
    vector<string> keyWords;
    keyWords.push_back("bbb");
    keyWords.push_back("ddd");
    
    string summary = _summaryExtractor.getSummary(text, keyWords, &_fieldInfo);
    string checkSummary = string() + "aaa " + HIGHLIGHT_BEGIN_DELIMITER + "bbb" + HIGHLIGHT_END_DELIMITER
                          + " ccc " + HIGHLIGHT_BEGIN_DELIMITER + "ddd" + HIGHLIGHT_END_DELIMITER + " eee....";
    ASSERT_EQ(checkSummary, summary);
}

TEST_F(DefaultSummaryExtractorTest, testDefaultChineseKeyword) { 
    HA3_LOG(DEBUG, "Begin Test!");

    string text = "中华\t人民\t共和国\t浙江\t省\t杭州\t市\t.\t"
                  "fff\t \tiii\t \tfff\t \tbbb\t \tggg\t.\t \t"
                  "bbb\t \tbbb\t \tbbb\t \tbbb\t \tggg\t.\t \t"
                  "hhh\t \tlll\t \teee\t \twww\t \tddd\t.";
    vector<string> keyWords;
    keyWords.push_back("人民");
    keyWords.push_back("浙江");

    _fieldInfo._maxSummaryLength = 41;
    string summary = _summaryExtractor.getSummary(text, keyWords, &_fieldInfo);
    string checkSummary = string() + "中华" + HIGHLIGHT_BEGIN_DELIMITER + "人民" + HIGHLIGHT_END_DELIMITER
                          + "共和国" + HIGHLIGHT_BEGIN_DELIMITER + "浙江" + HIGHLIGHT_END_DELIMITER + "省杭州市....";
    ASSERT_EQ(checkSummary, summary);
}

TEST_F(DefaultSummaryExtractorTest, testChineseFixedSummary) { 
    HA3_LOG(DEBUG, "Begin Test!");

    string text = "中华\t人民\t共和国\t浙江\t省\t杭州\t市\t.";
    vector<string> keyWords;
    keyWords.push_back("aaa");
    keyWords.push_back("bbb");
    
    string summary = _summaryExtractor.getSummary(text, keyWords, &_fieldInfo);
    string checkSummary = string() + "中华人民共和...";
    ASSERT_EQ(checkSummary, summary);
}

TEST_F(DefaultSummaryExtractorTest, testExtend2ChineseSeparator) { 
    HA3_LOG(DEBUG, "Begin Test!");

    string text = "中华\t，\t人民\t。\t共和国\t浙江\t省\t杭州\t市\t，\t阿里\t巴巴\t；";
    vector<string> keyWords;
    keyWords.push_back("浙江");

    _fieldInfo._maxSummaryLength = 35;
    string summary = _summaryExtractor.getSummary(text, keyWords, &_fieldInfo);
    string checkSummary = string() + "共和国" + HIGHLIGHT_BEGIN_DELIMITER + "浙江" + HIGHLIGHT_END_DELIMITER + "省杭州市，...";
    ASSERT_EQ(checkSummary, summary);
}

TEST_F(DefaultSummaryExtractorTest, testZeroSummaryLength) { 
    HA3_LOG(DEBUG, "Begin Test!");

    string text = "中华\t，\t人民\t。\t共和国\t浙江\t省\t杭州\t市\t，\t阿里\t巴巴\t；";
    vector<string> keyWords;
    keyWords.push_back("浙江");

    _fieldInfo._maxSummaryLength = 0;
    string summary = _summaryExtractor.getSummary(text, keyWords, &_fieldInfo);
    string checkSummary = string() + "";
    ASSERT_EQ(checkSummary, summary);
}

TEST_F(DefaultSummaryExtractorTest, testKeywordLengthGreaterThanSummaryLength) { 
    HA3_LOG(DEBUG, "Begin Test!");

    string text = "abc\t \tdefg\t \thij\t \t12345678901234567890";
    vector<string> keyWords;
    keyWords.push_back("12345678901234567890");

    _fieldInfo._maxSummaryLength = 10;
    string summary = _summaryExtractor.getSummary(text, keyWords, &_fieldInfo);
    string checkSummary = string() + "abc defg ...";
    ASSERT_EQ(checkSummary, summary);
}

TEST_F(DefaultSummaryExtractorTest, testExtendToHead) { 
    HA3_LOG(DEBUG, "Begin Test!");

    string text = "中华\t，\t共和国\t浙江\t省\t杭州\t市\t，\t阿里\t巴巴\t；\t"
                  "1111111111111111111111111111111111111111";
    vector<string> keyWords;
    keyWords.push_back("浙江");

    _fieldInfo._maxSummaryLength = 40;
    string summary = _summaryExtractor.getSummary(text, keyWords, &_fieldInfo);
    string checkSummary = string() + "中华，共和国" + HIGHLIGHT_BEGIN_DELIMITER + "浙江" + HIGHLIGHT_END_DELIMITER + "省杭州市，...";
    ASSERT_EQ(checkSummary, summary);
}

TEST_F(DefaultSummaryExtractorTest, testExtendToTail) { 
    HA3_LOG(DEBUG, "Begin Test!");

    string text = "中华\t，\t人民\t。\t共和国\t浙江\t省\t杭州\t市\t，\t阿里\t巴巴\t；";
    vector<string> keyWords;
    keyWords.push_back("阿里");

    string summary = _summaryExtractor.getSummary(text, keyWords, &_fieldInfo);
    string checkSummary = string() + HIGHLIGHT_BEGIN_DELIMITER + "阿里" + HIGHLIGHT_END_DELIMITER + "巴巴；";
    ASSERT_EQ(checkSummary, summary);
}

TEST_F(DefaultSummaryExtractorTest, testExtendToAllText) { 
    HA3_LOG(DEBUG, "Begin Test!");

    string text = "aa\t,\t \tbb\t.\t \tcc\t!\t \tdd\t?\t \ta";
    vector<string> keyWords;
    keyWords.push_back("cc");

    string summary = _summaryExtractor.getSummary(text, keyWords, &_fieldInfo);
    string checkSummary = string() + "aa, bb. " + HIGHLIGHT_BEGIN_DELIMITER + "cc" + HIGHLIGHT_END_DELIMITER + "! dd? a";
    ASSERT_EQ(checkSummary, summary);
}

TEST_F(DefaultSummaryExtractorTest, testExtendToAllTextWithDifferentCase) { 
    HA3_LOG(DEBUG, "Begin Test!");

    string text = "\taa\t,\t \tbb\t.\t \tCc\t!\t \tdd\t?\t \ta";
    vector<string> keyWords;
    keyWords.push_back("cc");

    string summary = _summaryExtractor.getSummary(text, keyWords, &_fieldInfo);
    string checkSummary = string() + "aa, bb. " + HIGHLIGHT_BEGIN_DELIMITER + "Cc" + HIGHLIGHT_END_DELIMITER + "! dd? a";
    ASSERT_EQ(checkSummary, summary);
}

TEST_F(DefaultSummaryExtractorTest, testFindNextUtf8Charactor) {
    HA3_LOG(DEBUG, "Begin Test!");

    size_t pos = string::npos;
    pos = _summaryExtractor.findNextUtf8Character("中国", 6);
    ASSERT_EQ((size_t)6, pos);

    pos = string::npos;
    pos = _summaryExtractor.findNextUtf8Character("中国", 18);
    ASSERT_EQ((size_t)6, pos);

    pos = string::npos;
    pos = _summaryExtractor.findNextUtf8Character("中国", 0);
    ASSERT_EQ((size_t)0, pos);

    pos = string::npos;
    pos = _summaryExtractor.findNextUtf8Character("中国", 1);
    ASSERT_EQ((size_t)3, pos);

    pos = string::npos;
    pos = _summaryExtractor.findNextUtf8Character("中国", 2);
    ASSERT_EQ((size_t)3, pos);

    pos = string::npos;
    pos = _summaryExtractor.findNextUtf8Character("中国", 3);
    ASSERT_EQ((size_t)3, pos);

    pos = string::npos;
    pos = _summaryExtractor.findNextUtf8Character("中国", 4);
    ASSERT_EQ((size_t)6, pos);
}


TEST_F(DefaultSummaryExtractorTest, testFindLastUtf8Charactor) {
    HA3_LOG(DEBUG, "Begin Test!");

    size_t pos = string::npos;
    pos = _summaryExtractor.findLastUtf8Character("中国", 6);
    ASSERT_EQ((size_t)6, pos);

    pos = string::npos;
    pos = _summaryExtractor.findLastUtf8Character("中国", 18);
    ASSERT_EQ((size_t)6, pos);

    pos = string::npos;
    pos = _summaryExtractor.findLastUtf8Character("中国", 0);
    ASSERT_EQ((size_t)0, pos);

    pos = string::npos;
    pos = _summaryExtractor.findLastUtf8Character("中国", 1);
    ASSERT_EQ((size_t)0, pos);

    pos = string::npos;
    pos = _summaryExtractor.findLastUtf8Character("中国", 2);
    ASSERT_EQ((size_t)0, pos);

    pos = string::npos;
    pos = _summaryExtractor.findLastUtf8Character("中国", 3);
    ASSERT_EQ((size_t)3, pos);

    pos = string::npos;
    pos = _summaryExtractor.findLastUtf8Character("中国", 4);
    ASSERT_EQ((size_t)3, pos);
}

TEST_F(DefaultSummaryExtractorTest, testOriginalLengthGreaterThanMaxLength) { 
    HA3_LOG(DEBUG, "Begin Test!");

    string text = "中华\t，\t人民\t。\t共和国\t浙江\t省\t杭州\t市\t，\t阿里\t巴巴\t；";
    vector<string> keyWords;
    keyWords.push_back("浙江");

    _fieldInfo._maxSummaryLength = 22;
    string summary = _summaryExtractor.getSummary(text, keyWords, &_fieldInfo);
    string checkSummary = string() + "共和国" + HIGHLIGHT_BEGIN_DELIMITER + "浙江" + HIGHLIGHT_END_DELIMITER + "...";
    ASSERT_EQ(checkSummary, summary);
}

TEST_F(DefaultSummaryExtractorTest, testRequestTrace) {
    HA3_LOG(DEBUG, "Begin Test!");

    TableInfoPtr tableInfoPtr = TableInfoConfigurator::createFromFile(
            TEST_DATA_PATH"/config_test/sample_schema.json");
    HitSummarySchemaPtr hitSummarySchemaPtr(new HitSummarySchema(tableInfoPtr.get()));
    SummaryProfileInfo summaryProfileInfo;
    summaryProfileInfo._fieldSummaryConfigVec.resize(
            hitSummarySchemaPtr->getFieldCount());
    SummaryProfile summaryProfile(summaryProfileInfo, hitSummarySchemaPtr.get());

    autil::mem_pool::Pool *pool = new autil::mem_pool::Pool(1024);

    Request *request = new Request();
    request->setConfigClause(new ConfigClause());
    Tracer *tracer = new Tracer();
    SearchCommonResource* resource =
        new SearchCommonResource(pool, tableInfoPtr, NULL, NULL, tracer, NULL, CavaPluginManagerPtr(), request, NULL, _cavaJitModules);
    tracer->setLevel(ISEARCH_TRACE_TRACE3);
    SummaryQueryInfo *queryInfo = new SummaryQueryInfo();
    SummaryExtractorProvider *provider = new SummaryExtractorProvider(
            queryInfo, &summaryProfile.getFieldSummaryConfig(), request, NULL,
            NULL, *resource);
    _summaryExtractor.beginRequest(provider);
    string traceInfo = tracer->getTraceInfo();
    ASSERT_TRUE(traceInfo.find("DefaultSummaryExtractor begin request\n") 
                != string::npos);

    delete request;
    delete tracer;
    delete queryInfo;
    delete provider;
    delete resource;
    delete pool;
}


END_HA3_NAMESPACE(summary);
