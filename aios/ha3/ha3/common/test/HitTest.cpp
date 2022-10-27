#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Hit.h>
#include <ha3/test/test.h>
#include <autil/StringUtil.h>

using namespace std;

USE_HA3_NAMESPACE(util);
BEGIN_HA3_NAMESPACE(common);

class HitTest : public TESTBASE {
public:
    HitTest();
    ~HitTest();
public:
    void setUp();
    void tearDown();
public:
    Hit* prepareHit(docid_t docid);
    HitPtr prepareHit(docid_t docid,
                      const std::vector<SortExprValue>& sortExprValues);
protected:
    void checkHit(Hit &resultHit, bool jsonMode = false);
protected:
    config::HitSummarySchema* _hitSummarySchema;
    autil::mem_pool::Pool *_pool;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, HitTest);


HitTest::HitTest() {
    _hitSummarySchema = new config::HitSummarySchema(NULL);
    _hitSummarySchema->declareSummaryField("price1", ft_string, false);
    _hitSummarySchema->declareSummaryField("price2", ft_string, false);
    _hitSummarySchema->calcSignature();
}

HitTest::~HitTest() {
    delete _hitSummarySchema;
}

void HitTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
    _pool = new autil::mem_pool::Pool(10 * 1024 * 1024);
}

void HitTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
    delete _pool;
    _pool = NULL;
}

void HitTest::checkHit(Hit &resultHit, bool jsonMode) {
    ASSERT_EQ(1, resultHit.getDocId());
    if (!jsonMode) {
        ASSERT_EQ((uint32_t)2, resultHit.getSummaryCount());
        ASSERT_EQ(string("100"), resultHit.getSummaryValue("price1"));
        ASSERT_EQ(string("200"), resultHit.getSummaryValue("price2"));
    }
    int32_t int32Value;
    ErrorCode eret;
    bool ret = resultHit.getAttribute("attr_int32", int32Value);
    ASSERT_TRUE(ret);
    ASSERT_EQ(int32_t(1000), int32Value);

    eret = resultHit.getVariableValue("attr_int32", int32Value);
    ASSERT_TRUE(eret == ERROR_NONE);
    ASSERT_EQ(int32_t(1000), int32Value);

    float floatValue;
    ret = resultHit.getAttribute("attr_float", floatValue);
    ASSERT_TRUE(ret);
    ASSERT_EQ(1.005f, floatValue);
    ret = resultHit.getAttribute("attr_int32", floatValue);
    ASSERT_FALSE(ret);
    ret = resultHit.getAttribute("multi_attr_float", floatValue);
    ASSERT_FALSE(ret);

    eret = resultHit.getVariableValue("attr_float", floatValue);
    ASSERT_TRUE(eret == ERROR_NONE);
    ASSERT_EQ(1.005f, floatValue);
    eret = resultHit.getVariableValue("attr_int32", floatValue);
    ASSERT_TRUE(eret == ERROR_VARIABLE_TYPE_NOT_MATCH);
    eret = resultHit.getVariableValue("multi_attr_float", floatValue);
    ASSERT_TRUE(eret == ERROR_VARIABLE_TYPE_NOT_MATCH);

    string stringValue;
    ret = resultHit.getAttribute("attr_string", stringValue);
    ASSERT_TRUE(ret);
    ASSERT_EQ(string("attr_string_value"), stringValue);

    ret = resultHit.getAttribute("attr_string2", stringValue);
    ASSERT_FALSE(ret);

    eret = resultHit.getVariableValue("attr_string", stringValue);
    ASSERT_TRUE(eret == ERROR_NONE);
    ASSERT_EQ(string("attr_string_value"), stringValue);

    eret = resultHit.getVariableValue("attr_string2", stringValue);
    ASSERT_EQ(ERROR_VARIABLE_NAME_NOT_EXIST, eret);

    double doubleValue;
    ret = resultHit.getAttribute("attr_double", doubleValue);
    ASSERT_TRUE(ret);
    ASSERT_EQ(double(10000), doubleValue);

    eret = resultHit.getVariableValue("attr_double", doubleValue);
    ASSERT_TRUE(eret == ERROR_NONE);
    ASSERT_EQ(double(10000), doubleValue);

    //multi value attribute check:
    vector<int32_t> int32MultiValue;
    ret = resultHit.getAttribute("multi_attr_int32", int32MultiValue);
    ASSERT_TRUE(ret);
    ASSERT_EQ((size_t)3, int32MultiValue.size());
    ASSERT_EQ(1000, int32MultiValue[2]);

    eret = resultHit.getVariableValue("multi_attr_int32", int32MultiValue);
    ASSERT_TRUE(eret == ERROR_NONE);
    ASSERT_EQ((size_t)3, int32MultiValue.size());
    ASSERT_EQ(1000, int32MultiValue[2]);

    vector<float> floatMultiValue;
    ret = resultHit.getAttribute("multi_attr_float", floatMultiValue);
    ASSERT_TRUE(ret);
    ASSERT_EQ((size_t)3, floatMultiValue.size());
    ASSERT_EQ(1.005f, floatMultiValue[1]);
    ret = resultHit.getAttribute("multi_attr_int32", floatMultiValue);
    ASSERT_FALSE(ret);
    ret = resultHit.getAttribute("attr_float", floatMultiValue);
    ASSERT_FALSE(ret);

    eret = resultHit.getVariableValue("multi_attr_float", floatMultiValue);
    ASSERT_TRUE(eret == ERROR_NONE);
    ASSERT_EQ((size_t)3, floatMultiValue.size());
    ASSERT_EQ(1.005f, floatMultiValue[1]);
    eret = resultHit.getVariableValue("multi_attr_int32", floatMultiValue);
    ASSERT_EQ(ERROR_VARIABLE_TYPE_NOT_MATCH, eret);
    eret = resultHit.getVariableValue("attr_float", floatMultiValue);
    ASSERT_EQ(ERROR_VARIABLE_TYPE_NOT_MATCH, eret);

    vector<double> doubleMultiValue;
    ret = resultHit.getAttribute("multi_attr_double", doubleMultiValue);
    ASSERT_TRUE(ret);

    eret = resultHit.getVariableValue("multi_attr_double", doubleMultiValue);
    ASSERT_TRUE(eret == ERROR_NONE);

    ASSERT_EQ((hashid_t)12345, resultHit.getHashId());
    const PropertyMap& propertyMap = resultHit.getPropertyMap();
    ASSERT_EQ((size_t)1, propertyMap.size());
    PropertyMap::const_iterator it = propertyMap.find("red");
    ASSERT_TRUE(it != propertyMap.end());
    ASSERT_EQ(string("111"), (*it).second);

    ASSERT_DOUBLE_EQ(double(12.1), resultHit.getSortValue());
    ASSERT_EQ(uint32_t(2), resultHit.getSortExprCount());
    ASSERT_EQ(string("abc"), resultHit.getSortExprValue(0));
    ASSERT_EQ(vt_string, resultHit.getSortExprValueType(0));
    ASSERT_EQ(string("12.1"), resultHit.getSortExprValue(1));
    ASSERT_EQ(vt_double, resultHit.getSortExprValueType(1));

    const SortExprValue&  exprValue1 = resultHit.getSortExprValueStructure(0);
    ASSERT_EQ(string("abc"), exprValue1.valueStr);
    ASSERT_EQ(vt_string, exprValue1.vt);
    const SortExprValue&  exprValue2 = resultHit.getSortExprValueStructure(1);
    ASSERT_EQ(string("12.1"), exprValue2.valueStr);
    ASSERT_EQ(vt_double, exprValue2.vt);
}

TEST_F(HitTest, testSetTracer) {
    HA3_LOG(DEBUG, "Begin Test!");

    Tracer tracer;
    string info = "test trace info";
    string expectInfo = info + "\n";
    tracer.trace(info);
    Hit hit;
    ASSERT_FALSE(hit.getTracer());
    hit.setTracer(&tracer);
    ASSERT_TRUE(hit.getTracer());
    ASSERT_EQ(expectInfo, (hit.getTracer())->getTraceInfo());

    Tracer tracer2;
    string info2 = "another trace info";
    string expectInfo2 = info2 + "\n";
    tracer2.trace(info2);
    ASSERT_TRUE(hit.getTracer());
    hit.setTracer(&tracer2);
    ASSERT_TRUE(hit.getTracer());
    ASSERT_EQ(expectInfo2, (hit.getTracer())->getTraceInfo());
}

TEST_F(HitTest, testStealSummary) {
    vector<SortExprValue> sortExprValues;
    HitPtr hit2 = HitPtr(prepareHit(1));
    HitPtr hit1 = HitPtr(new Hit(hit2->getDocId(), hit2->getHashId()));

    string traceInfo = hit2->getTracer()->getTraceInfo();
    hit1->stealSummary(*hit2);
    ASSERT_TRUE(!hit2->getSummaryHit());
    ASSERT_TRUE(hit1->getSummaryHit());
    ASSERT_TRUE(!hit2->getTracer());
    ASSERT_TRUE(hit1->getTracer());
    ASSERT_EQ(traceInfo, hit1->getTracer()->getTraceInfo());
}

TEST_F(HitTest, testSerializeAndDeserializeWithoutJson) {
    HA3_LOG(DEBUG, "Begin Test!");
    Hit *hit = prepareHit(1);
    shared_ptr<Hit> ptr_hit(hit);
    ASSERT_TRUE(hit);
    SummaryHit *summaryHit = hit->getSummaryHit();
    summaryHit->setHitSummarySchema(_hitSummarySchema);
    autil::DataBuffer dataBuffer;
    hit->serialize(dataBuffer);

    Hit resultHit(0);
    resultHit.deserialize(dataBuffer, _pool);

    summaryHit = resultHit.getSummaryHit();
    ASSERT_EQ(summaryHit->getSignature(), _hitSummarySchema->getSignature());
    summaryHit->setHitSummarySchema(_hitSummarySchema);
    checkHit(resultHit);
    ASSERT_EQ("docid:1\n", hit->getTracer()->getTraceInfo());
}

Hit* HitTest::prepareHit(docid_t docid) {
    Hit *hit = new Hit(docid);
    hit->setHashId(12345);
    Tracer *tracer = new Tracer();
    tracer->setLevel(ISEARCH_TRACE_TRACE3);
    string traceInfo = "docid:" + autil::StringUtil::toString(docid);
    tracer->trace(traceInfo);
    hit->setTracerWithoutCreate(tracer);

    SummaryHit *summaryHit = new SummaryHit(_hitSummarySchema, _pool, tracer);
    summaryHit->setFieldValue(0, "100", 3);
    summaryHit->setFieldValue(1, "200", 3);
    hit->setSummaryHit(summaryHit);
    hit->addAttribute<int32_t>(string("attr_int32"), 1000);
    hit->addAttribute<float>(string("attr_float"), float(1.005));
    hit->addAttribute<string>(string("attr_string"), string("attr_string_value"));
    hit->addAttribute<double>(string("attr_double"), double(10000));

    hit->addAttribute<vector<int32_t> >(string("multi_attr_int32"), vector<int32_t>(3, 1000));
    hit->addAttribute<vector<float> >(string("multi_attr_float"), vector<float>(3, 1.005f));
    // hit->addAttribute<vector<string> >(string("multi_attr_string"), vector<string>(3, "multi_attr_string_value"));
    hit->addAttribute<vector<double> >(string("multi_attr_double"), vector<double>(3, 10000));

    hit->addVariableValue<int32_t>(string("attr_int32"), 1000);
    hit->addVariableValue<float>(string("attr_float"), float(1.005));
    hit->addVariableValue<string>(string("attr_string"), string("attr_string_value"));
    hit->addVariableValue<double>(string("attr_double"), double(10000));

    hit->addVariableValue<vector<int32_t> >(string("multi_attr_int32"), vector<int32_t>(3, 1000));
    hit->addVariableValue<vector<float> >(string("multi_attr_float"), vector<float>(3, 1.005f));
    hit->addVariableValue<vector<double> >(string("multi_attr_double"), vector<double>(3, 10000));

    hit->insertProperty("red", "111");
    hit->setSortValue(12.1);
    hit->addSortExprValue("abc", vt_string);
    hit->addSortExprValue("12.1");
    return hit;
}

HitPtr HitTest::prepareHit(docid_t docid, const vector<SortExprValue>& sortExprValues) {
    HitPtr hit(new Hit(docid));
    for(size_t i = 0; i < sortExprValues.size(); i++) {
        hit->addSortExprValue(sortExprValues[i].valueStr, sortExprValues[i].vt);
    }
    return hit;
}

TEST_F(HitTest, testCompareHitsLessThan) {
    {
        HitPtr hit1(new Hit(1));
        HitPtr hit2(new Hit(1));
        hit1->setSortValue(12.1);
        hit2->setSortValue(13);
        ASSERT_TRUE(hit1->lessThan(hit2));
        ASSERT_TRUE(!hit2->lessThan(hit1));
    }
    {
        HitPtr hit1(new Hit(1));
        HitPtr hit2(new Hit(2));
        hit1->setSortValue(12.1);
        hit2->setSortValue(12.1);
        ASSERT_TRUE(hit1->lessThan(hit2));
        ASSERT_TRUE(!hit2->lessThan(hit1));
    }
}

TEST_F(HitTest, testCompareSortExprValue) {
    {
        SortExprValue v1, v2;
        v1.valueStr = "abc";
        v1.vt = vt_string;
        v2.valueStr = "abd";
        v2.vt = vt_string;
        ASSERT_TRUE(v1 < v2);
        v2.valueStr = "abc";
        ASSERT_TRUE(v1 == v2);
    }
    {
        SortExprValue v1, v2;
        v1.valueStr = "10.1";
        v1.vt = vt_double;
        v2.valueStr = "11";
        v2.vt = vt_double;
        ASSERT_TRUE(v1 < v2);
        v2.valueStr = "10.1";
        ASSERT_TRUE(v1 == v2);
    }
    {
        SortExprValue v1, v2;
        v1.valueStr = "10.1";
        v2.valueStr = "11";
        ASSERT_TRUE(v1 < v2);
        v2.valueStr = "10.1";
        ASSERT_TRUE(v1 == v2);
    }
}

END_HA3_NAMESPACE(common);
