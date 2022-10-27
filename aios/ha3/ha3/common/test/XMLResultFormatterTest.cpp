#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/mem_pool/Pool.h>
#include <ha3/common/XMLResultFormatter.h>
#include <ha3/test/test.h>
#include <ha3/common/test/ResultConstructor.h>
#include <ha3/common/GlobalIdentifier.h>
#include <ha3/common/Result.h>
#include <sstream>
#include <fstream>
#include <libxml/parser.h>
#include <libxml/xpath.h>

using namespace std;
BEGIN_HA3_NAMESPACE(common);

class XMLResultFormatterTest : public TESTBASE {
public:
    XMLResultFormatterTest();
    ~XMLResultFormatterTest();
public:
    void setUp();
    void tearDown();
protected:
    std::string reformatXML(const std::string& str);
    xmlXPathObjectPtr getnodeset(xmlDocPtr doc, xmlChar *xpath);
    void checkKVPairsResult(xmlChar *xpath, const std::vector<std::string> &exprStr);

protected:
    autil::mem_pool::Pool _pool;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, XMLResultFormatterTest);

XMLResultFormatterTest::XMLResultFormatterTest() {
}

XMLResultFormatterTest::~XMLResultFormatterTest() {
}

void XMLResultFormatterTest::setUp() {
}

void XMLResultFormatterTest::tearDown() {
}

#define COMPARE_TO_FILE(expectResultfile, actualResult)   \
    do {                                                        \
        ifstream in(expectResultfile);                          \
        stringstream buffer;                                    \
        buffer << in.rdbuf();                                   \
        string expect = reformatXML(buffer.str());              \
        string actual = reformatXML(actualResult);              \
        ASSERT_EQ(expect, actual);                   \
    } while(0)

TEST_F(XMLResultFormatterTest, testSimpleProcess) {

    ResultPtr resultPtr(new Result(new Hits()));
    ResultConstructor resultConstructor;
    resultConstructor.fillHit(resultPtr->getHits(),
                              1, 3, "title, body, description",
                              "cluster1, 11, title1, body1, des1 # cluster2, 22, title2, body2, des2");

    Tracer tracer;
    tracer.trace("test trace info");
    Hits *hits = resultPtr->getHits();
    ASSERT_EQ((uint32_t)2, hits->size());

    for (size_t i=0; i<hits->size(); i++){
        HitPtr hit = hits->getHit(i);
        ASSERT_TRUE(hit);
        hit->setTracer(&tracer);
        hit->addSortExprValue("1.9", vt_double);
        hit->addSortExprValue("100", vt_double);
    }
    stringstream ss;
    XMLResultFormatter formatter;
    formatter.format(resultPtr, ss);
    COMPARE_TO_FILE(TEST_DATA_PATH"/xml_format/simple_result.xml", ss.str());
}

TEST_F(XMLResultFormatterTest, testFormatHitAndAgg) {
    ResultPtr resultPtr(new Result(new Hits()));
    ResultConstructor resultConstructor;
    resultConstructor.fillHit(resultPtr->getHits(),
                              1, 3, "title, body, description",
                              "cluster1, 11, title1, body1, des1");
    resultConstructor.fillAggregateResult(resultPtr,
            "sum,count,min,max",
            "key1, 4444, 111, 0, 3333 #key2, 1234, 222, 1, 789", &_pool);
    stringstream ss;
    XMLResultFormatter formatter;
    formatter.format(resultPtr, ss);
    COMPARE_TO_FILE(TEST_DATA_PATH"/xml_format/hit_and_agg.xml", ss.str());
}

TEST_F(XMLResultFormatterTest, testFormatNoSummaryHit) {
    GlobalIdentifier gid1(0, 1, 2, 3, 5, 6);
    HitPtr hit1(new Hit);
    hit1->setGlobalIdentifier(gid1);
    hit1->addAttribute("att1", 100);
    hit1->setClusterName("cluster1");
    hit1->setHasPrimaryKeyFlag(true);
    hit1->setIp(123);
    string attVal = "abcd";
    hit1->addAttribute("att2", attVal);

    GlobalIdentifier gid2(7, 8, 9, 10, 11, 12);
    HitPtr hit2(new Hit);
    hit2->setGlobalIdentifier(gid2);
    hit2->addAttribute("att1", 200);
    hit2->setClusterName("cluster2");
    hit2->setHasPrimaryKeyFlag(true);
    hit2->setIp(321);
    attVal = "efgh";
    hit2->addAttribute("att2", attVal);

    Hits* hits = new Hits;
    hits->addHit(hit1);
    hits->addHit(hit2);
    hits->setNoSummary(true);
    hits->setIndependentQuery(true);
    ResultPtr resultPtr(new Result(hits));

    stringstream ss;
    XMLResultFormatter formatter;
    formatter.format(resultPtr, ss);
    COMPARE_TO_FILE(TEST_DATA_PATH"/xml_format/no_summary_hit.xml", ss.str());
}


TEST_F(XMLResultFormatterTest, testFormatEmptyHit) {
    ResultPtr resultPtr(new Result(new Hits()));
    stringstream ss;
    XMLResultFormatter formatter;
    formatter.format(resultPtr, ss);
    COMPARE_TO_FILE(TEST_DATA_PATH"/xml_format/empty_hit.xml", ss.str());
}

TEST_F(XMLResultFormatterTest, testFormatAggregateResults) {
    ResultPtr resultPtr(new Result(new Hits()));
    ResultConstructor resultConstructor;
    resultConstructor.fillAggregateResult(resultPtr,
            "sum", "key1, 4444 #", &_pool, "expr1");
    resultConstructor.fillAggregateResult(resultPtr,
            "sum", "key1&, 4444 #", &_pool, "expr1&expr2");
    resultConstructor.fillAggregateResult(resultPtr,
            "sum", "key1<, 4444 #", &_pool, "expr1<expr2");
    resultConstructor.fillAggregateResult(resultPtr,
            "sum", "key1>, 4444 #", &_pool, "expr1>expr2");
    resultConstructor.fillAggregateResult(resultPtr,
            "sum", "key1', 4444 #", &_pool, "'expr1'");
    resultConstructor.fillAggregateResult(resultPtr,
            "sum", "key1\", 4444 #", &_pool, "\"expr1\"");
    stringstream ss;
    XMLResultFormatter formatter;
    formatter.format(resultPtr, ss);

    COMPARE_TO_FILE(TEST_DATA_PATH"/xml_format/agg_result.xml", ss.str());
}

TEST_F(XMLResultFormatterTest, testFormatVariableValue) {
    HitPtr hit1(new Hit);
    hit1->addVariableValue("var1", 1.1);
    hit1->addVariableValue("var2", string("value12"));
    hit1->setClusterName("cluster1");
    hit1->setHasPrimaryKeyFlag(true);

    HitPtr hit2(new Hit);
    hit2->addVariableValue("var1", 2.1);
    hit2->addVariableValue("var2", string("value22"));
    hit2->setClusterName("cluster2");
    hit2->setHasPrimaryKeyFlag(true);
    Hits* hits = new Hits;
    hits->addHit(hit1);
    hits->addHit(hit2);
    hits->setNoSummary(true);
    ResultPtr resultPtr(new Result(hits));
    stringstream ss;
    XMLResultFormatter formatter;
    formatter.format(resultPtr, ss);

    COMPARE_TO_FILE(TEST_DATA_PATH"/xml_format/variable_value.xml", ss.str());
}

TEST_F(XMLResultFormatterTest, testFormatHitWithRawPk) {
    {
        ResultPtr resultPtr(new Result(new Hits()));
        stringstream ss;
        XMLResultFormatter formatter;
        formatter.format(resultPtr, ss);
        COMPARE_TO_FILE(TEST_DATA_PATH"/xml_format/hit_with_rawpk1.xml", ss.str());
    }
    {
        HitPtr hit1(new Hit);
        hit1->setRawPk("pk1");
        HitPtr hit2(new Hit);
        hit2->setRawPk("pk2");
        Hits* hits = new Hits;
        hits->addHit(hit1);
        hits->addHit(hit2);
        ResultPtr resultPtr(new Result(hits));
        stringstream ss;
        XMLResultFormatter formatter;
        formatter.format(resultPtr, ss);
        COMPARE_TO_FILE(TEST_DATA_PATH"/xml_format/hit_with_rawpk2.xml", ss.str());
    }
}

TEST_F(XMLResultFormatterTest, testFormatMeta) {
    ResultPtr resultPtr(new Result());
    stringstream ss;
    XMLResultFormatter formatter;
    Meta meta1 = {{"seekCount", "100"}, {"matchCount", "1"}};
    Meta meta2 = {{"trace", "ddd"}};
    resultPtr->addMeta("searchInfo", meta1);
    resultPtr->addMeta("qrsInfo", meta2);
    formatter.format(resultPtr, ss);
    COMPARE_TO_FILE(TEST_DATA_PATH"/xml_format/meta.xml", ss.str());
}


std::string XMLResultFormatterTest::reformatXML(const std::string& str) {
    xmlDocPtr doc = xmlReadMemory(str.c_str(), str.size(), "noname.xml", NULL, 0);
    assert(doc);

    xmlChar *xmlbuff = NULL;
    int buffersize = 0;
    xmlDocDumpFormatMemory(doc, &xmlbuff, &buffersize, 1);
    string result((char*)(xmlbuff), buffersize);
    xmlFree(xmlbuff);
    xmlFreeDoc(doc);
    xmlCleanupParser();

    return result;
}

xmlXPathObjectPtr XMLResultFormatterTest::getnodeset(xmlDocPtr doc, xmlChar *xpath) {
    xmlXPathContextPtr context;
    xmlXPathObjectPtr result;

    context = xmlXPathNewContext(doc);
    if (context == NULL) {
        HA3_LOG(WARN, "Error in xmlXPathNewContext\n");
        return NULL;
    }
    result = xmlXPathEvalExpression(xpath, context);
    xmlXPathFreeContext(context);
    if (result == NULL) {
        HA3_LOG(WARN, "Error in xmlXPathEvalExpression\n");
        return NULL;
    }
    if(xmlXPathNodeSetIsEmpty(result->nodesetval)){
        xmlXPathFreeObject(result);
        HA3_LOG(WARN, "No result\n");
        return NULL;
    }
    return result;
}

TEST_F(XMLResultFormatterTest, testWithFormatKVPairs) {
    {
        vector<string> str;
        xmlChar *xpath = (xmlChar*) "//Root/TotalTime";
        str.push_back(string("0.000"));
        checkKVPairsResult(xpath, str);
    }
    {
        vector<string> str;
        xmlChar *xpath = (xmlChar*) "//Root/SortExprMeta";
        str.push_back(string(""));
        checkKVPairsResult(xpath, str);
    }
    {
        vector<string> str;
        xmlChar *xpath = (xmlChar*) "//Root/hits/@numhits";
        str.push_back(string("0"));
        checkKVPairsResult(xpath, str);
    }
    {
        vector<string> str;
        xmlChar *xpath = (xmlChar*) "//Root/hits/@totalhits";
        str.push_back(string("0"));
        checkKVPairsResult(xpath, str);
    }
    {
        vector<string> str;
        xmlChar *xpath = (xmlChar*) "//Root/hits/@coveredPercent";
        str.push_back(string("0.00"));
        checkKVPairsResult(xpath, str);
    }
    {
        vector<string> str;
        xmlChar *xpath = (xmlChar*) "//@key";
        str.push_back(string("expr1"));
        str.push_back(string("expr1&expr2"));
        str.push_back(string("expr1<expr2"));
        str.push_back(string("expr1>expr2"));
        str.push_back(string("\'expr1\'"));
        str.push_back(string("\"expr1\""));
        checkKVPairsResult(xpath, str);
    }
    {
        vector<string> str;
        xmlChar *xpath = (xmlChar*) "//@value";
        str.push_back(string("key1"));
        str.push_back(string("key1&"));
        str.push_back(string("key1<"));
        str.push_back(string("key1>"));
        str.push_back(string("key1\'"));
        str.push_back(string("key1\""));
        checkKVPairsResult(xpath, str);
    }
    {
        vector<string> str;
        xmlChar *xpath = (xmlChar*) "//Root/AggregateResults/AggregateResult/group/sum";
        str.push_back(string("4444"));
        str.push_back(string("4444"));
        str.push_back(string("4444"));
        str.push_back(string("4444"));
        str.push_back(string("4444"));
        str.push_back(string("4444"));
        checkKVPairsResult(xpath, str);
    }
}

void XMLResultFormatterTest::checkKVPairsResult(xmlChar *xpath, const vector<string> &exprStr) {
    ifstream in(TEST_DATA_PATH"/xml_format/agg_result.xml");
    stringstream buffer;
    buffer << in.rdbuf();
    string str = buffer.str();
    xmlDocPtr doc = xmlReadMemory(str.c_str(), str.size(), "noname.xml", NULL, 0);
    xmlNodeSetPtr nodeset;
    xmlXPathObjectPtr result;
    xmlChar *keyword;
    result = getnodeset(doc, xpath);
    ASSERT_TRUE(result);
    if(result) {
        nodeset = result->nodesetval;
        ASSERT_TRUE(nodeset != NULL);
        assert(nodeset != NULL);
        for (int i = 0; i < nodeset->nodeNr; i++) {
            keyword = xmlNodeListGetString(doc, nodeset->nodeTab[i]->xmlChildrenNode, 1);
            ASSERT_EQ(exprStr[i], (string)(char*)keyword);

            xmlFree(keyword);
        }
        xmlXPathFreeObject(result);
    }
    xmlFreeDoc(doc);
    xmlCleanupParser();
}

END_HA3_NAMESPACE(common);
