#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/PBResultFormatter.h>
#include <ha3/common/test/ResultConstructor.h>
#include <suez/turing/expression/framework/VariableTypeTraits.h>
#include <ha3/common/Request.h>
#include <ha3/common/test/ResultConstructor.h>
#include <ha3/config/ClusterConfigInfo.h>
#include <ha3/service/QrsSearcherProcessDelegation.h>
#include <autil/StringTokenizer.h>
#include <autil/StringUtil.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

using namespace std;
using namespace autil;
using namespace google::protobuf;
using namespace google::protobuf::io;
using namespace suez::turing;
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(service);
USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(config);
IE_NAMESPACE_USE(index);
BEGIN_HA3_NAMESPACE(common);

class PBResultFormatterTest : public TESTBASE {
public:
    PBResultFormatterTest();
    ~PBResultFormatterTest();
public:
    void setUp();
    void tearDown();
protected:
    template<typename T>
    void fillAttribute(Hits *hits, const std::string &attrName);
    template<typename T>
    void fillAttribute(Hit *hit, const std::string &attrName,
                       const std::string &value);

protected:
    void checkHits(const Hits *hits, const PBHits *pbHits);
    void checkHit(const HitPtr &hit, const PBHit *pbHits, bool isNoSummary);
    void checkHitAttributes(const HitPtr &hitPtr, const PBHit *pbHit);
    void checkHitVariableValues(const HitPtr &hitPtr, const PBHit *pbHit);

    void checkMatchDocs(const ResultPtr &resultPtr, PBMatchDocs pbMatchDocs,
                        const std::vector<clusterid_t> &clusterIds);
    void checkMatchDocsAttributes(const ResultPtr &resultPtr, PBMatchDocs pbMatchDocs);
    void checkMatchDocsUserDatas(const ResultPtr &resultPtr, PBMatchDocs pbMatchDocs);
    void checkMatchDocsSortValues(const ResultPtr &resultPtr, PBMatchDocs pbMatchDocs);
    void checkMatchDocsMultiValue(const ResultPtr &resultPtr, PBMatchDocs pbMatchDocs);

    template<typename T>
    void checkIntAttributeValue(const AttributeItem* attrItem,
                                const PBAttrKVPair *attribute);

    template<typename T>
    void checkDoubleAttributeValue(const AttributeItem* attrItem,
                                   const PBAttrKVPair *attribute);
    void checkStringAttributeValue(const AttributeItem* attrItem,
                                   const PBAttrKVPair *attribute);
    void checkAggregateResult(const ResultPtr &resultPtr,
                              const PBResult* pbResult);
    void checkErrorResults(const ResultPtr &resultPtr,
                           const PBResult* pbResult);

    void checkAttrValues(const AttributeMap & attributeMap,
                         const ::google::protobuf::RepeatedPtrField<PBAttrKVPair> &values);
    ResultPtr createResultWithMatchDocs();

    void checkUserDataForMultiAndVector(const PBMatchDocs &pbMatchDocs);
protected:
    autil::mem_pool::Pool _pool;
    std::map<std::string, std::string> _attrTransMap;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, PBResultFormatterTest);


PBResultFormatterTest::PBResultFormatterTest() {
}

PBResultFormatterTest::~PBResultFormatterTest() {
}

void PBResultFormatterTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
    _attrTransMap.clear();
}

void PBResultFormatterTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(PBResultFormatterTest, testFormatMetaHits) {
    Hits *hits = new Hits();
    unique_ptr<Hits> hitsPtr(hits);

    PBHits pbHits;
    PBResultFormatter formatter;
    formatter.formatMetaHits(hits, &pbHits);
    ASSERT_TRUE(pbHits.metahitmap().size() == 0);

    string metaHitKey1 = "meta_hit_key_1";
    MetaHit &metaHit1 = hits->getMetaHit(metaHitKey1);
    metaHit1["key1"] = "value1";
    metaHit1["key2"] = "value2";

    string metaHitKey2 = "meta_hit_key_2";
    MetaHit &metaHit2 = hits->getMetaHit(metaHitKey2);
    metaHit2["key3"] = "value3";

    pbHits.Clear();
    formatter.formatMetaHits(hits, &pbHits);
    ASSERT_EQ((int)2, pbHits.metahitmap().size());

    const PBMetaHitMap &pbMetaHit1 = pbHits.metahitmap(0);
    ASSERT_EQ(metaHitKey1, pbMetaHit1.metahitkey());
    ASSERT_EQ((int)metaHit1.size(), pbMetaHit1.metahitvalue().size());
    ASSERT_EQ(string("key1"), pbMetaHit1.metahitvalue(0).key());
    ASSERT_EQ(string("value1"), pbMetaHit1.metahitvalue(0).value());
    ASSERT_EQ(string("key2"), pbMetaHit1.metahitvalue(1).key());
    ASSERT_EQ(string("value2"), pbMetaHit1.metahitvalue(1).value());

    const PBMetaHitMap &pbMetaHit2 = pbHits.metahitmap(1);
    ASSERT_EQ(metaHitKey2, pbMetaHit2.metahitkey());
    ASSERT_EQ((int)metaHit2.size(), pbMetaHit2.metahitvalue().size());
    ASSERT_EQ(string("key3"), pbMetaHit2.metahitvalue(0).key());
    ASSERT_EQ(string("value3"), pbMetaHit2.metahitvalue(0).value());
}

TEST_F(PBResultFormatterTest, testFormatMeta) {
    ResultPtr resultPtr(new Result());
    PBResult pbResult;
    PBResultFormatter formatter;
    formatter.formatMeta(resultPtr, pbResult);
    ASSERT_TRUE(pbResult.metamap().size() == 0);

    string metaKey1 = "meta_hit_key_1";
    Meta &meta1 = resultPtr->getMeta(metaKey1);
    meta1["key1"] = "value1";
    meta1["key2"] = "value2";

    string metaKey2 = "meta_hit_key_2";
    Meta &meta2 = resultPtr->getMeta(metaKey2);
    meta2["key3"] = "value3";

    pbResult.Clear();
    formatter.formatMeta(resultPtr, pbResult);
    ASSERT_EQ((int)2, pbResult.metamap().size());

    const PBMetaMap &pbMeta1 = pbResult.metamap(0);
    ASSERT_EQ(metaKey1, pbMeta1.metakey());
    ASSERT_EQ((int)meta1.size(), pbMeta1.metavalue().size());
    ASSERT_EQ(string("key1"), pbMeta1.metavalue(0).key());
    ASSERT_EQ(string("value1"), pbMeta1.metavalue(0).value());
    ASSERT_EQ(string("key2"), pbMeta1.metavalue(1).key());
    ASSERT_EQ(string("value2"), pbMeta1.metavalue(1).value());

    const PBMetaMap &pbMeta2 = pbResult.metamap(1);
    ASSERT_EQ(metaKey2, pbMeta2.metakey());
    ASSERT_EQ((int)meta2.size(), pbMeta2.metavalue().size());
    ASSERT_EQ(string("key3"), pbMeta2.metavalue(0).key());
    ASSERT_EQ(string("value3"), pbMeta2.metavalue(0).value());
}

TEST_F(PBResultFormatterTest, testFormatHitWithSummary) {
    docid_t docId = 123;
    hashid_t hashId = 1;
    string clusterName = "clustername";
    HitPtr hitPtr(new Hit(docId, hashId, clusterName));

    primarykey_t primaryKey;
    primaryKey.value[0] = 10;
    primaryKey.value[1] = 20;
    hitPtr->setPrimaryKey(primaryKey);
    FullIndexVersion fullIndexVersion = 13;
    hitPtr->setFullIndexVersion(fullIndexVersion);
    clusterid_t clusterId = 7;
    hitPtr->setClusterId(clusterId);

    config::HitSummarySchema hitSummarySchema;
    hitSummarySchema.declareSummaryField("field1");
    hitSummarySchema.declareSummaryField("field2");
    hitSummarySchema.declareSummaryField("field3");
    autil::mem_pool::Pool pool;

    SummaryHit *summaryHit = new SummaryHit(&hitSummarySchema, &pool);
    summaryHit->setFieldValue(0, "value0", 6);
    summaryHit->setFieldValue(1, "value1", 6);
    summaryHit->setFieldValue(2, "value2", 6);
    hitPtr->setSummaryHit(summaryHit);

    PBHit pbHit;
    PBResultFormatter formatter;
    formatter.formatHit(hitPtr, &pbHit, false);
    checkHit(hitPtr, &pbHit, false);

    Tracer tracer;
    tracer.trace("test trace info");
    hitPtr->setTracer(&tracer);

    pbHit.Clear();
    formatter.formatHit(hitPtr, &pbHit, false);
    checkHit(hitPtr, &pbHit, false);

    hitPtr->insertProperty("property1", "value1");
    hitPtr->insertProperty("property2", "value2");
    hitPtr->insertProperty("property3", "value3");

    pbHit.Clear();
    formatter.formatHit(hitPtr, &pbHit, false);
    checkHit(hitPtr, &pbHit, false);
}

TEST_F(PBResultFormatterTest, testFormatAttributes) {
    HitPtr hitPtr(new Hit);

    fillAttribute<uint8_t>(hitPtr.get(), "attr_uint8", "10");
    fillAttribute<int8_t>(hitPtr.get(), "attr_int8", "11");
    fillAttribute<uint16_t>(hitPtr.get(), "attr_uint16", "12");
    fillAttribute<int16_t>(hitPtr.get(), "attr_int16", "13");
    fillAttribute<uint32_t>(hitPtr.get(), "attr_uint32", "14");
    fillAttribute<int32_t>(hitPtr.get(), "attr_int32", "15");
    fillAttribute<uint64_t>(hitPtr.get(), "attr_uint64", "1633332232");
    fillAttribute<int64_t>(hitPtr.get(), "attr_int64", "172223232");
    fillAttribute<float>(hitPtr.get(), "attr_float", "22.21");
    fillAttribute<double>(hitPtr.get(), "attr_double", "22.231");
    fillAttribute<string>(hitPtr.get(), "attr_string", "abc");
    fillAttribute<primarykey_t>(hitPtr.get(), "attr_hash128", "1222");

    fillAttribute<uint8_t>(hitPtr.get(), "mult_attr_uint8", "10, 11");
    fillAttribute<int8_t>(hitPtr.get(), "mult_attr_int8", "11, -12");
    fillAttribute<uint16_t>(hitPtr.get(), "mult_attr_uint16", "12, 13");
    fillAttribute<int16_t>(hitPtr.get(), "mult_attr_int16", "13, -14");
    fillAttribute<uint32_t>(hitPtr.get(), "mult_attr_uint32", "14, 15");
    fillAttribute<int32_t>(hitPtr.get(), "mult_attr_int32", "15, -16");
    fillAttribute<uint64_t>(hitPtr.get(), "mult_attr_uint64",
                            "1633332232, 12345534");
    fillAttribute<int64_t>(hitPtr.get(), "mult_attr_int64",
                           "172223232, -242543556");
    fillAttribute<float>(hitPtr.get(), "mult_attr_float",
                         "22.21, 11.0");
    fillAttribute<double>(hitPtr.get(), "mult_attr_double",
                          "22.231, -2.0");
    fillAttribute<string>(hitPtr.get(), "mult_attr_string",
                          "abc, babab");

    hitPtr->addVariableValue("bool", AttributeItemPtr(new AttributeItemTyped<bool>(true)));

    PBHit pbHit;
    PBResultFormatter formatter;
    formatter.formatHit(hitPtr, &pbHit, true);
    checkHitAttributes(hitPtr, &pbHit);
    checkHitVariableValues(hitPtr, &pbHit);
}

TEST_F(PBResultFormatterTest, testFormatAggregateResult) {
    ResultPtr resultPtr(new Result(new Hits()));
    resultPtr->addCoveredRange(string("clustername"), 0, 32767);
    ASSERT_TRUE(resultPtr.get());
    ResultConstructor resultConstructor;

    resultConstructor.fillAggregateResult(resultPtr,
            "sum, count", "key1, 4444, 123#key2, 5555, 345", &_pool, "expr1");
    resultConstructor.fillAggregateResult(resultPtr,
            "sum", "key1, 4444 #", &_pool, "expr1&expr2");
    resultConstructor.fillAggregateResult(resultPtr,
            "sum", "key1, 4444 #", &_pool, "expr1<expr2");
    resultConstructor.fillAggregateResult(resultPtr,
            "sum", "key1, 4444 #", &_pool, "expr1>expr2");
    resultConstructor.fillAggregateResult(resultPtr,
            "sum", "key1, 4444 #", &_pool, "'expr1'");
    resultConstructor.fillAggregateResult(resultPtr,
            "sum", "key1, 4444 #", &_pool, "\"expr1\"");

    PBResult pbResult;
    PBResultFormatter formatter;
    formatter.format(resultPtr, pbResult);
    checkAggregateResult(resultPtr, &pbResult);
    ASSERT_EQ((double)50.0, pbResult.hits().coveredpercent());
}

TEST_F(PBResultFormatterTest, testFormatErrorResults) {
    MultiErrorResultPtr errorResultPtr(new MultiErrorResult);
    ErrorResult result2;
    result2.setHostInfo("partition_id_2", "hostName");
    result2.setErrorCode(ERROR_QRS_NOT_FOUND_CHAIN);
    errorResultPtr->addErrorResult(result2);

    ErrorResult result3;
    result3.setHostInfo("partition_id_3", "");
    result3.setErrorCode(ERROR_QRS_SEARCH_ERROR);
    errorResultPtr->addErrorResult(result3);

    ResultPtr resultPtr(new Result(new Hits()));
    resultPtr->addErrorResult(errorResultPtr);

    PBResult pbResult;
    PBResultFormatter formatter;
    formatter.format(resultPtr, pbResult);
    checkErrorResults(resultPtr, &pbResult);
}

TEST_F(PBResultFormatterTest, testFormatHitNoSummary) {
    docid_t docId = 123;
    hashid_t hashId = 1;
    string clusterName = "clustername";
    HitPtr hitPtr(new Hit(docId, hashId, clusterName));

    primarykey_t primaryKey;
    primaryKey.value[0] = 10;
    primaryKey.value[1] = 20;
    hitPtr->setPrimaryKey(primaryKey);
    FullIndexVersion fullIndexVersion = 13;
    hitPtr->setFullIndexVersion(fullIndexVersion);
    clusterid_t clusterId = 7;
    hitPtr->setClusterId(clusterId);
    uint32_t searcherIp = 12345;
    hitPtr->setIp(searcherIp);

    fillAttribute<uint8_t>(hitPtr.get(), "attr_uint8", "10");
    fillAttribute<double>(hitPtr.get(), "mult_attr_int64",
                          "172223232, -242543556");
    fillAttribute<string>(hitPtr.get(), "mult_attr_string",
                          "abc, babab");
    fillAttribute<string>(hitPtr.get(), "attr_string", "abc");

    PBHit pbHit;
    PBResultFormatter formatter;
    formatter.formatHit(hitPtr, &pbHit, true);
    checkHit(hitPtr, &pbHit, true);

    Tracer tracer;
    tracer.trace("test trace info");
    hitPtr->setTracer(&tracer);

    pbHit.Clear();
    formatter.formatHit(hitPtr, &pbHit, true);
    checkHit(hitPtr, &pbHit, true);

    hitPtr->insertProperty("property1", "value1");
    hitPtr->insertProperty("property2", "value2");
    hitPtr->insertProperty("property3", "value3");

    pbHit.Clear();
    formatter.formatHit(hitPtr, &pbHit, true);
    checkHit(hitPtr, &pbHit, true);
}

TEST_F(PBResultFormatterTest, testFormatResultWithSummary) {
    HA3_LOG(DEBUG, "Begin Test!");

    ResultPtr resultPtr(new Result(new Hits()));
    resultPtr->setTotalTime(1234567);
    ASSERT_TRUE(resultPtr.get());
    ResultConstructor resultConstructor;
    HA3_LOG(DEBUG, "Before fillHit!");
    Hits *hits = resultPtr->getHits();
    resultConstructor.fillHit(hits,
                              1, 3, "title, body, description",
                              "cluster1, 11, title1, body1, des1"
                              " # cluster2, 22, title2, body2, des2");
    hits->setTotalHits(100);
    TracerPtr tracer(new Tracer());
    tracer->trace("test trace info");
    resultPtr->setTracer(tracer);

    PBResult pbResult;
    PBResultFormatter formatter;
    stringstream ss;
    formatter.format(resultPtr, ss);
    pbResult.Clear();
    string pbResultStr = ss.str();
    ArrayInputStream inputStream((void *)pbResultStr.c_str(),
                                 pbResultStr.length());
    pbResult.ParseFromZeroCopyStream(&inputStream);

    ASSERT_EQ((uint64_t)1234567, pbResult.totaltime());
    const PBHits &pbHits = pbResult.hits();
    checkHits(hits, &pbHits);

    ASSERT_TRUE(pbResult.has_tracer());
    ASSERT_EQ(resultPtr->getTracer()->getTraceInfo(),
              pbResult.tracer());

}

TEST_F(PBResultFormatterTest, testFormatResultNoSummary) {
    HA3_LOG(DEBUG, "Begin Test!");

    ResultPtr resultPtr(new Result(new Hits()));
    resultPtr->setTotalTime(1234567);
    ASSERT_TRUE(resultPtr.get());
    vector<SortExprMeta> sortExprMetas;
    SortExprMeta sortExprMeta;
    sortExprMeta.sortExprName = "sortExprName1";
    sortExprMeta.sortFlag = true;
    sortExprMetas.push_back(sortExprMeta);
    sortExprMeta.sortExprName = "sortExprName2";
    sortExprMeta.sortFlag = false;
    sortExprMetas.push_back(sortExprMeta);
    resultPtr->setSortExprMeta(sortExprMetas);
    ResultConstructor resultConstructor;
    HA3_LOG(DEBUG, "Before fillHit!");
    Hits *hits = resultPtr->getHits();
    resultConstructor.fillHit(hits,
                              1, 3, "title, body, description",
                              "cluster1, 11, title1, body1, des1"
                              " # cluster2, 22, title2, body2, des2");
    hits->setTotalHits(100);
    hits->setNoSummary(true);
    hits->setIndependentQuery(true);

    fillAttribute<int32_t>(hits, "attr_int");
    resultConstructor.fillAggregateResult(resultPtr,
            "sum,count,min,max",
            "key1, 4444, 111, 0, 3333 #key2, 1234, 222, 1, 789", &_pool);

    Tracer tracer;
    tracer.trace("test trace info");
    ASSERT_EQ((uint32_t)2, hits->size());
    ASSERT_TRUE(hits);

    for (size_t i = 0; i < hits->size(); i++){
        HitPtr hit = hits->getHit(i);
        ASSERT_TRUE(hit);
        hit->setTracer(&tracer);
        hit->addSortExprValue("1.9", vt_double);
        hit->addSortExprValue("100", vt_double);
    }

    PBResult pbResult;
    PBResultFormatter formatter;
    formatter.format(resultPtr, pbResult);

    ASSERT_EQ((uint64_t)1234567, pbResult.totaltime());
    const PBHits &pbHits = pbResult.hits();
    checkHits(hits, &pbHits);

    ASSERT_EQ(int(2), pbHits.sortexprmetas_size());
    const SortExprssionMeta& sortExprMeta1 = pbHits.sortexprmetas(0);
    ASSERT_TRUE(sortExprMeta1.sortflag());
    ASSERT_EQ(string("sortExprName1"), sortExprMeta1.sortexprname());
    const SortExprssionMeta& sortExprMeta2 = pbHits.sortexprmetas(1);
    ASSERT_TRUE(!sortExprMeta2.sortflag());
    ASSERT_EQ(string("sortExprName2"), sortExprMeta2.sortexprname());

    stringstream ss;
    formatter.format(resultPtr, ss);
}

ResultPtr PBResultFormatterTest::createResultWithMatchDocs() {
    ResultConstructor constructor;
    MatchDocs *matchDocs = new MatchDocs();
    constructor.prepareMatchDocs(matchDocs, &_pool, pob_primarykey64_flag, true);
    autil::DataBuffer dataBuffer(autil::DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, &_pool);
    matchDocs->serialize(dataBuffer);
    MatchDocs *newMatchDocs = new MatchDocs();
    newMatchDocs->deserialize(dataBuffer, &_pool);
    delete matchDocs;
    matchDocs = newMatchDocs;
    ResultPtr resultPtr(new Result(matchDocs));
    auto allocatorPtr = matchDocs->getMatchDocAllocator();
    matchdoc::ReferenceVector sortReferVec;
    constructor.getSortReferences(allocatorPtr.get(), sortReferVec);
    vector<SortExprMeta> sortExprMetaVec;
    for (matchdoc::ReferenceVector::iterator it = sortReferVec.begin();
         it != sortReferVec.end(); ++it)
    {
        SortExprMeta sortExprMeta;
        sortExprMeta.sortExprName = (*it)->getName();
        sortExprMeta.sortRef = *it;
        sortExprMeta.sortFlag = false;
        sortExprMetaVec.push_back(sortExprMeta);
    }
    resultPtr->setSortExprMeta(sortExprMetaVec);
    return resultPtr;
}

TEST_F(PBResultFormatterTest, testFormatMatchDocs) {
    ResultPtr resultPtr = createResultWithMatchDocs();
    resultPtr->addClusterName("cluster1");
    resultPtr->addClusterName("cluster2");
    const vector<SortExprMeta> &expectSortExprMetas =
        resultPtr->getSortExprMeta();

    RequestPtr requestPtr(new Request(&_pool));
    ConfigClause* configClause = new ConfigClause();
    configClause->setStartOffset(0);
    configClause->setHitCount(10);
    requestPtr->setConfigClause(configClause);

    QrsSearcherProcessDelegation::selectMatchDocsToFormat(requestPtr, resultPtr);
    const vector<matchdoc::MatchDoc>& resultMatchDocs = resultPtr->getMatchDocsForFormat();
    ASSERT_EQ((size_t)2, resultMatchDocs.size());
    PBResult pbResult;
    PBResultFormatter formatter;
    _attrTransMap["ref0"] = "attr0";
    formatter.setAttrNameTransMap(_attrTransMap);
    formatter.formatMatchDocs(resultPtr, pbResult);

    // validate
    PBMatchDocs pbMatchDocs = pbResult.matchdocs();
    vector<clusterid_t> clusterIds;
    clusterIds.push_back(0);
    clusterIds.push_back(1);
    checkMatchDocs(resultPtr, pbMatchDocs, clusterIds);
    checkUserDataForMultiAndVector(pbMatchDocs);

    const PBSortValues& sortValues = pbMatchDocs.sortvalues();
    ASSERT_EQ(int(expectSortExprMetas.size()),
              sortValues.sortexprmetas_size());
    for (size_t i = 0; i < expectSortExprMetas.size(); ++i) {
        const SortExprssionMeta& sortExprMeta = sortValues.sortexprmetas(i);
        ASSERT_EQ(expectSortExprMetas[i].sortFlag,
                  sortExprMeta.sortflag());
        ASSERT_EQ(expectSortExprMetas[i].sortExprName,
                  sortExprMeta.sortexprname());
    }
}

void PBResultFormatterTest::checkUserDataForMultiAndVector(
        const PBMatchDocs &pbMatchDocs)
{
#define FIND_INDEX(indexName, pbKey, pbfield)                           \
    int indexName = 0;                                                  \
    for (;indexName < pbMatchDocs.pbfield##_size(); indexName++) {      \
        const auto &tmp = pbMatchDocs.pbfield(indexName);               \
        if (tmp.key() == pbKey) {                                       \
            break;                                                      \
        }                                                               \
    }

    FIND_INDEX(ref8PbIndex, "ref8", doubleattrvalues);
    // check mutli double
    ASSERT_EQ(3, pbMatchDocs.doubleattrvalues_size());
    ASSERT_EQ(string("ref8"),
              pbMatchDocs.doubleattrvalues(ref8PbIndex).key());
    ASSERT_EQ(VARIABLE_VALUE_TYPE,
              pbMatchDocs.doubleattrvalues(ref8PbIndex).type());
    ASSERT_EQ((int32_t)2, pbMatchDocs.doubleattrvalues(ref8PbIndex).offset_size());
    ASSERT_EQ((uint32_t)0, pbMatchDocs.doubleattrvalues(ref8PbIndex).offset(0));
    ASSERT_EQ((uint32_t)3, pbMatchDocs.doubleattrvalues(ref8PbIndex).offset(1));
    ASSERT_EQ(5, pbMatchDocs.doubleattrvalues(ref8PbIndex).doublevalue_size());
    ASSERT_EQ((double)1.1, pbMatchDocs.doubleattrvalues(ref8PbIndex).doublevalue(0));
    ASSERT_EQ((double)2.2, pbMatchDocs.doubleattrvalues(ref8PbIndex).doublevalue(1));
    ASSERT_EQ((double)3.4, pbMatchDocs.doubleattrvalues(ref8PbIndex).doublevalue(2));
    ASSERT_EQ((double)1.1, pbMatchDocs.doubleattrvalues(ref8PbIndex).doublevalue(3));
    ASSERT_EQ((double)2.2, pbMatchDocs.doubleattrvalues(ref8PbIndex).doublevalue(4));

    FIND_INDEX(ref9PbIndex, "ref9", bytesattrvalues);
    // check mutli string
    ASSERT_EQ(2, pbMatchDocs.bytesattrvalues_size());
    ASSERT_EQ(string("ref9"), pbMatchDocs.bytesattrvalues(ref9PbIndex).key());
    ASSERT_EQ(VARIABLE_VALUE_TYPE,
              pbMatchDocs.bytesattrvalues(ref9PbIndex).type());
    ASSERT_EQ(2, pbMatchDocs.bytesattrvalues(ref9PbIndex).offset_size());
    ASSERT_EQ((uint32_t)0, pbMatchDocs.bytesattrvalues(ref9PbIndex).offset(0));
    ASSERT_EQ((uint32_t)2, pbMatchDocs.bytesattrvalues(ref9PbIndex).offset(1));
    ASSERT_EQ(4, pbMatchDocs.bytesattrvalues(ref9PbIndex).bytesvalue_size());
    ASSERT_EQ(string("abc"), pbMatchDocs.bytesattrvalues(ref9PbIndex).bytesvalue(0));
    ASSERT_EQ(string("123456"), pbMatchDocs.bytesattrvalues(ref9PbIndex).bytesvalue(1));
    ASSERT_EQ(string("abc"), pbMatchDocs.bytesattrvalues(ref9PbIndex).bytesvalue(2));
    ASSERT_EQ(string("123456"), pbMatchDocs.bytesattrvalues(ref9PbIndex).bytesvalue(3));

    FIND_INDEX(ref11PbIndex, "ref11", int64attrvalues);
    // check vector
    ASSERT_EQ(4, pbMatchDocs.int64attrvalues_size());
    ASSERT_EQ(string("ref11"),
              pbMatchDocs.int64attrvalues(ref11PbIndex).key());
    ASSERT_EQ(VARIABLE_VALUE_TYPE,
              pbMatchDocs.int64attrvalues(ref11PbIndex).type());
    ASSERT_EQ((int32_t)2, pbMatchDocs.int64attrvalues(ref11PbIndex).offset_size());
    ASSERT_EQ((uint32_t)0, pbMatchDocs.int64attrvalues(ref11PbIndex).offset(0));
    ASSERT_EQ((uint32_t)2, pbMatchDocs.int64attrvalues(ref11PbIndex).offset(1));
    ASSERT_EQ((int64_t)1, pbMatchDocs.int64attrvalues(ref11PbIndex).int64value(0));
    ASSERT_EQ((int64_t)2, pbMatchDocs.int64attrvalues(ref11PbIndex).int64value(1));
    ASSERT_EQ((int64_t)3, pbMatchDocs.int64attrvalues(ref11PbIndex).int64value(2));
    ASSERT_EQ((int64_t)4, pbMatchDocs.int64attrvalues(ref11PbIndex).int64value(3));

#undef FIND_INDEX
}

TEST_F(PBResultFormatterTest, testFormatSillyMatchDocs) {
    ResultPtr resultPtr1(new Result(new MatchDocs()));
    PBResult pbResult;
    PBResultFormatter formatter;
    formatter.formatMatchDocs(resultPtr1, pbResult);

    ResultPtr resultPtr2(new Result());
    formatter.formatMatchDocs(resultPtr2, pbResult);
}

TEST_F(PBResultFormatterTest, testFormatMatchDocsWithPartMatchDocs) {
    {
        ResultPtr resultPtr = createResultWithMatchDocs();
        resultPtr->addClusterName("cluster1");

        RequestPtr requestPtr(new Request);
        ConfigClause* configClause = new ConfigClause();
        configClause->setStartOffset(0);
        configClause->setHitCount(1);
        requestPtr->setConfigClause(configClause);

        QrsSearcherProcessDelegation::selectMatchDocsToFormat(requestPtr, resultPtr);
        const vector<matchdoc::MatchDoc>& resultMatchDocs = resultPtr->getMatchDocsForFormat();
        ASSERT_EQ((size_t)1, resultMatchDocs.size());
        PBResult pbResult;
        PBResultFormatter formatter;
        formatter.formatMatchDocs(resultPtr, pbResult);

        // validate
        PBMatchDocs pbMatchDocs = pbResult.matchdocs();
        vector<clusterid_t> clusterIds;
        clusterIds.push_back(0);
        checkMatchDocs(resultPtr, pbMatchDocs, clusterIds);
    }
    {
        ResultPtr resultPtr = createResultWithMatchDocs();
        resultPtr->addClusterName("cluster2");

        RequestPtr requestPtr(new Request);
        ConfigClause* configClause = new ConfigClause();
        configClause->setStartOffset(1);
        configClause->setHitCount(2);
        requestPtr->setConfigClause(configClause);

        QrsSearcherProcessDelegation::selectMatchDocsToFormat(requestPtr, resultPtr);
        const vector<matchdoc::MatchDoc>& resultMatchDocs = resultPtr->getMatchDocsForFormat();
        ASSERT_EQ((size_t)1, resultMatchDocs.size());
        PBResult pbResult;
        PBResultFormatter formatter;
        formatter.formatMatchDocs(resultPtr, pbResult);

        // validate
        PBMatchDocs pbMatchDocs = pbResult.matchdocs();
        vector<clusterid_t> clusterIds;
        clusterIds.push_back(1);
        checkMatchDocs(resultPtr, pbMatchDocs, clusterIds);
    }
    {
        ResultPtr resultPtr = createResultWithMatchDocs();

        RequestPtr requestPtr(new Request);
        ConfigClause* configClause = new ConfigClause();
        configClause->setStartOffset(2);
        configClause->setHitCount(10);
        requestPtr->setConfigClause(configClause);

        QrsSearcherProcessDelegation::selectMatchDocsToFormat(requestPtr, resultPtr);
        const vector<matchdoc::MatchDoc>& resultMatchDocs = resultPtr->getMatchDocsForFormat();
        ASSERT_EQ((size_t)0, resultMatchDocs.size());
        PBResult pbResult;
        PBResultFormatter formatter;
        formatter.formatMatchDocs(resultPtr, pbResult);

        // validate
        PBMatchDocs pbMatchDocs = pbResult.matchdocs();
        vector<clusterid_t> clusterIds;
        checkMatchDocs(resultPtr, pbMatchDocs, clusterIds);
    }
}

TEST_F(PBResultFormatterTest, testGidofOnlySummary) {
    HA3_LOG(DEBUG, "Begin Test!");

    ResultPtr resultPtr(new Result(new Hits()));
    ASSERT_TRUE(resultPtr.get());

    Hits *hits = resultPtr->getHits();
    HitPtr hit(new Hit);
    hit->setFullIndexVersion(2);
    hit->setIp(123);
    hits->addHit(hit);

    hits->setIndependentQuery(true);

    PBResult pbResult;
    PBResultFormatter formatter;
    formatter.format(resultPtr, pbResult);

    const PBHits &pbHits = pbResult.hits();
    checkHits(hits, &pbHits);
}

TEST_F(PBResultFormatterTest, testFormatHitWithRawPk) {
    HA3_LOG(DEBUG, "Begin Test!");

    HitPtr hit1(new Hit);
    hit1->setRawPk("pk1");
    HitPtr hit2(new Hit);
    hit2->setRawPk("pk2");
    Hits* hits = new Hits;
    hits->addHit(hit1);
    hits->addHit(hit2);
    ResultPtr resultPtr(new Result(hits));
    PBResult pbResult;
    PBResultFormatter formatter;
    formatter.format(resultPtr, pbResult);

    const PBHits &pbHits = pbResult.hits();
    checkHits(hits, &pbHits);
}

TEST_F(PBResultFormatterTest, testFormatHitWithSummaryInBytesOption) {
    HA3_LOG(DEBUG, "Begin Test!");

    ResultPtr resultPtr(new Result(new Hits()));
    resultPtr->setTotalTime(1234567);
    ASSERT_TRUE(resultPtr.get());
    ResultConstructor resultConstructor;
    HA3_LOG(DEBUG, "Before fillHit!");
    Hits *hits = resultPtr->getHits();
    resultConstructor.fillHit(hits, 1, 3, "title, body, description",
                              "cluster1, 11, title1, body1, des1"
                              " # cluster2, 22, t2, b2, d2");
    hits->setTotalHits(100);

    PBResult pbResult;
    PBResultFormatter formatter;
    formatter.setOption(PBResultFormatter::SUMMARY_IN_BYTES);
    formatter.format(resultPtr, pbResult);

    ASSERT_EQ((uint64_t)1234567, pbResult.totaltime());
    const PBHits &pbHits = pbResult.hits();
    uint32_t hitCount = hits->size();
    ASSERT_EQ(hitCount, pbHits.numhits());
    ASSERT_EQ(hits->totalHits(), pbHits.totalhits());

    const PBHit &pbHit1 = pbHits.hit(0);
    ASSERT_EQ(0, pbHit1.summary_size());
    ASSERT_TRUE(pbHit1.has_summarybytes());
    string summaryBytes = pbHit1.summarybytes();
    // fieldName fieldValue with vint
    ASSERT_EQ((size_t)41,  summaryBytes.size());

    const PBHit &pbHit2 = pbHits.hit(1);
    ASSERT_EQ(0, pbHit2.summary_size());
    ASSERT_TRUE(pbHit2.has_summarybytes());
    summaryBytes = pbHit2.summarybytes();
    // fieldName fieldValue with vint
    ASSERT_EQ((size_t)32,  summaryBytes.size());
}

template<typename T>
void PBResultFormatterTest::fillAttribute(Hits *hits, const string &attrName)
{
    for (uint32_t i = 0; i < hits->size(); ++i) {
        HitPtr hitPtr = hits->getHit(i);
        hitPtr->addAttribute(attrName, AttributeItemPtr(
                        new AttributeItemTyped<T>((T)i)));
    }
}

template<typename T>
void PBResultFormatterTest::fillAttribute(
        Hit *hit, const string &attrName, const string &values)
{
    StringTokenizer tokens(values, ",");
    vector<T> vec;
    for (StringTokenizer::Iterator it = tokens.begin();
         it != tokens.end(); it++)
    {
        vec.push_back(StringUtil::fromString<T>(*it));
    }
    if (vec.size() == 1) {
        hit->addAttribute(attrName, AttributeItemPtr(
                        new AttributeItemTyped<T>(vec[0])));
        hit->addVariableValue(attrName, AttributeItemPtr(
                        new AttributeItemTyped<T>(vec[0])));
    } else {
        hit->addAttribute(attrName, AttributeItemPtr(
                        new AttributeItemTyped<vector<T> >(vec)));
        hit->addVariableValue(attrName, AttributeItemPtr(
                        new AttributeItemTyped<vector<T> >(vec)));
    }
}

void PBResultFormatterTest::checkHits(const Hits *hits, const PBHits *pbHits)
{
    uint32_t hitCount = hits->size();
    ASSERT_EQ(hitCount, pbHits->numhits());
    ASSERT_EQ(hits->totalHits(), pbHits->totalhits());

    for (uint32_t i = 0; i < hitCount; i ++) {
        HitPtr hitPtr = hits->getHit(i);
        const PBHit &pbHit = pbHits->hit(i);
        checkHit(hitPtr, &pbHit, hits->isNoSummary());
    }
}

void PBResultFormatterTest::checkHit(const HitPtr &hitPtr, const PBHit *pbHit,
                                     bool isNoSummary)
{
    ASSERT_EQ(hitPtr->getClusterName(), pbHit->clustername());
    ASSERT_EQ(hitPtr->getHashId(), (hashid_t)pbHit->hashid());
    ASSERT_EQ(hitPtr->getDocId(), (docid_t)pbHit->docid());

    if (isNoSummary) {
        GlobalIdentifier gid = hitPtr->getGlobalIdentifier();
        ASSERT_EQ((int32_t)gid.getFullIndexVersion(), (int32_t)pbHit->fullindexversion());
        ASSERT_EQ(gid.getIndexVersion(), pbHit->indexversion());
        if (hitPtr->hasPrimaryKey()) {
            primarykey_t primaryKey = gid.getPrimaryKey();
            ASSERT_EQ(primaryKey.value[0], pbHit->pkhigher());
            ASSERT_EQ(primaryKey.value[1], pbHit->pklower());
        }
        ASSERT_EQ(gid.getIp(), pbHit->searcherip());
    }

    map<string, string> summaryMap;
    SummaryHit *summaryHit = hitPtr->getSummaryHit();
    if (summaryHit) {
        const config::HitSummarySchema *hitSummarySchema = summaryHit->getHitSummarySchema();
        for (size_t i = 0; i < summaryHit->getFieldCount(); ++i)
        {
            const string &fieldName = hitSummarySchema->getSummaryFieldInfo(i)->fieldName;
            const ConstString *str = summaryHit->getFieldValue(i);
            if (!str) {
                continue;
            }
            summaryMap[fieldName] = string(str->data(), str->size());
        }
    }

    ASSERT_EQ((int)summaryMap.size(),
              pbHit->summary().size());
    for (int i = 0; i < pbHit->summary().size(); ++i) {
        const PBKVPair &kvPair = pbHit->summary(i);
        string key = kvPair.key();
        SummaryMap::const_iterator it = summaryMap.find(key);
        ASSERT_TRUE(it != summaryMap.end());
        ASSERT_EQ(it->second, kvPair.value());
    }

    const PropertyMap &propertyMap = hitPtr->getPropertyMap();
    ASSERT_EQ((int)propertyMap.size(),
              pbHit->property().size());
    for (int i = 0; i < pbHit->property().size(); ++i) {
        const PBKVPair &kvPair = pbHit->property(i);
        string key = kvPair.key();
        PropertyMap::const_iterator it = propertyMap.find(key);
        ASSERT_TRUE(it != propertyMap.end());
        ASSERT_EQ(it->second, kvPair.value());
    }

    checkHitAttributes(hitPtr, pbHit);
    checkHitVariableValues(hitPtr, pbHit);

    ASSERT_EQ((int)hitPtr->getSortExprCount(),
              pbHit->sortvalues().size());
    for (int i = 0; i < pbHit->sortvalues().size(); ++i) {
        ASSERT_EQ(hitPtr->getSortExprValue(i), pbHit->sortvalues(i));
    }

    if (hitPtr->getTracer() != NULL) {
        ASSERT_TRUE(pbHit->has_tracer());
        ASSERT_EQ(hitPtr->getTracer()->getTraceInfo(),
                  pbHit->tracer());
    } else {
        ASSERT_TRUE(!pbHit->has_tracer());
    }

    const string &rawPk = hitPtr->getRawPk();
    if (rawPk.empty()) {
        ASSERT_TRUE(!pbHit->has_rawpk());
    } else {
        ASSERT_TRUE(pbHit->has_rawpk());
        ASSERT_EQ(rawPk, pbHit->rawpk());
    }
}

void PBResultFormatterTest::checkHitVariableValues(
        const HitPtr &hit, const PBHit *pbHit)
{
    checkAttrValues(hit->getVariableValueMap(), pbHit->variablevalues());
}

void PBResultFormatterTest::checkHitAttributes(const HitPtr &hit,
        const PBHit *pbHit)
{
    checkAttrValues(hit->getAttributeMap(), pbHit->attributes());
}

void PBResultFormatterTest::checkAttrValues(
        const AttributeMap & attributeMap,
        const RepeatedPtrField<PBAttrKVPair> &values)
{
    int attrSize = values.size();
    ASSERT_EQ((int)attributeMap.size(), attrSize);

    for (int i = 0; i < attrSize; ++i)
    {
        const PBAttrKVPair &attribute = values.Get(i);
        string key = attribute.key();
        AttributeMap::const_iterator it = attributeMap.find(key);
        ASSERT_TRUE(it != attributeMap.end());
        VariableType vt = it->second->getType();
#define CHECK_INT_ATTRIBUTE(vt)                                         \
        case vt:                                                        \
        {                                                               \
            typedef VariableTypeTraits<vt, false>::AttrItemType T;      \
            checkIntAttributeValue<T>(it->second.get(), &attribute);    \
            break;                                                      \
        }

#define CHECK_DOUBLE_ATTRIBUTE(vt)                                      \
        case vt:                                                        \
        {                                                               \
            typedef VariableTypeTraits<vt, false>::AttrItemType T;      \
            checkDoubleAttributeValue<T>(it->second.get(), &attribute); \
            break;                                                      \
        }

        switch (vt) {
            CHECK_INT_ATTRIBUTE(vt_bool);
            CHECK_INT_ATTRIBUTE(vt_int8);
            CHECK_INT_ATTRIBUTE(vt_int16);
            CHECK_INT_ATTRIBUTE(vt_int32);
            CHECK_INT_ATTRIBUTE(vt_int64);
            CHECK_INT_ATTRIBUTE(vt_uint8);
            CHECK_INT_ATTRIBUTE(vt_uint16);
            CHECK_INT_ATTRIBUTE(vt_uint32);
            CHECK_INT_ATTRIBUTE(vt_uint64);

            CHECK_DOUBLE_ATTRIBUTE(vt_float);
            CHECK_DOUBLE_ATTRIBUTE(vt_double);

        case vt_string:
        {
            checkStringAttributeValue(it->second.get(), &attribute);
            break;
        }
        case vt_hash_128:
        {
            const AttributeItemTyped<primarykey_t> *attrItemTyped =
                static_cast<const AttributeItemTyped<primarykey_t>* >(it->second.get());
            ASSERT_TRUE(attrItemTyped);
            primarykey_t value = attrItemTyped->getItem();
            ASSERT_EQ((int)1, attribute.bytesvalue().size());
            ASSERT_EQ(StringUtil::toString(value),
                      attribute.bytesvalue(0));
            break;
        }
        default:
            assert(false);
            break;
        }
    }
}

template <typename T>
void PBResultFormatterTest::checkIntAttributeValue(
        const AttributeItem* attrItem, const PBAttrKVPair *attribute)
{
    if (!attrItem->isMultiValue()) {
        const AttributeItemTyped<T> *attrItemTyped =
            static_cast<const AttributeItemTyped<T>* >(attrItem);
        assert(attrItemTyped);
        T value = attrItemTyped->getItem();
        ASSERT_EQ((int)1, attribute->int64value().size());
        ASSERT_EQ((int64_t)value, attribute->int64value(0));
        return;
    }
    const AttributeItemTyped<vector<T> > *attrItemTyped =
        static_cast<const AttributeItemTyped<vector<T> >* >(attrItem);
    assert(attrItemTyped);

    vector<T> multiValue = attrItemTyped->getItem();
    ASSERT_EQ((int)multiValue.size(),
              attribute->int64value().size());
    for (size_t i = 0; i < multiValue.size(); ++i) {
        ASSERT_EQ((int64_t)multiValue[i], attribute->int64value(i));
    }
}

template <typename T>
void PBResultFormatterTest::checkDoubleAttributeValue(
        const AttributeItem* attrItem, const PBAttrKVPair *attribute)
{
    if (!attrItem->isMultiValue()) {
        const AttributeItemTyped<T> *attrItemTyped =
            static_cast<const AttributeItemTyped<T>* >(attrItem);
        assert(attrItemTyped);

        T value = attrItemTyped->getItem();
        ASSERT_EQ((int)1, attribute->doublevalue().size());
        ASSERT_EQ((double)value, attribute->doublevalue(0));
        return;
    }
    const AttributeItemTyped<vector<T> > *attrItemTyped =
        static_cast<const AttributeItemTyped<vector<T> >* >(attrItem);
    assert(attrItemTyped);

    vector<T> multiValue = attrItemTyped->getItem();
    ASSERT_EQ((int)multiValue.size(),
              attribute->doublevalue().size());
    for (size_t i = 0; i < multiValue.size(); ++i) {
        ASSERT_EQ((double)multiValue[i], attribute->doublevalue(i));
    }
}

void PBResultFormatterTest::checkStringAttributeValue(
        const AttributeItem* attrItem, const PBAttrKVPair *attribute)
{
    if (!attrItem->isMultiValue()) {
        const AttributeItemTyped<string> *attrItemTyped =
            static_cast<const AttributeItemTyped<string>* >(attrItem);
        assert(attrItemTyped);

        string value = attrItemTyped->getItem();
        ASSERT_EQ((int)1, attribute->bytesvalue().size());
        ASSERT_EQ(value, attribute->bytesvalue(0));
        return;
    }
    const AttributeItemTyped<vector<string> > *attrItemTyped =
        static_cast<const AttributeItemTyped<vector<string> >* >(attrItem);
    assert(attrItemTyped);

    vector<string> multiStr = attrItemTyped->getItem();
    ASSERT_EQ((int)multiStr.size(),
              attribute->bytesvalue().size());
    for (size_t i = 0; i < multiStr.size(); ++i) {
        ASSERT_EQ(multiStr[i], attribute->bytesvalue(i));
    }
}


void PBResultFormatterTest::checkAggregateResult(
        const ResultPtr &resultPtr,
        const PBResult* pbResult)
{
    int aggResultCount = resultPtr->getAggregateResultCount();
    ASSERT_EQ(aggResultCount, pbResult->aggresults_size());

    AggregateResults &aggResults = resultPtr->getAggregateResults();
    for (int i = 0; i < aggResultCount; i++)
    {
        AggregateResultPtr aggResultPtr = aggResults[i];
        const PBAggregateResults &pbAggResults = pbResult->aggresults(i);
        string aggKey = aggResultPtr->getGroupExprStr();
        ASSERT_EQ(aggKey, pbAggResults.aggregatekey());

        AggregateResult::AggExprValueMap &aggExprValueMap =
            aggResultPtr->getAggExprValueMap();
        int aggValueSize = pbAggResults.aggregatevalue_size();
        ASSERT_EQ((int)aggExprValueMap.size(), aggValueSize);

        for (int j = 0; j < aggValueSize; j++) {
            const PBAggregateValue &pbAggValue = pbAggResults.aggregatevalue(j);
            string groupValue = pbAggValue.groupvalue();
            AggregateResult::AggExprValueMap::const_iterator groupIt =
                aggExprValueMap.find(groupValue);
            ASSERT_TRUE(groupIt != aggExprValueMap.end());

            int funResultCount = pbAggValue.funnameresultpair_size();
            int aggFunCount = aggResultPtr->getAggFunCount();
            ASSERT_EQ(aggFunCount, funResultCount);

            const auto &allocatorPtr = aggResultPtr->getMatchDocAllocator();
            assert(allocatorPtr);
            auto referenceVec = allocatorPtr->getAllNeedSerializeReferences(
                    SL_NONE + 1);
            ASSERT_EQ((size_t)aggFunCount, referenceVec.size() - 1);

            for (int funIdx = 0; funIdx < aggFunCount; funIdx++) {
                ASSERT_EQ(aggResultPtr->getAggFunName(funIdx),
                        pbAggValue.funnameresultpair(funIdx).key());
                ASSERT_EQ(
                        referenceVec[funIdx + 1]->toString(groupIt->second),
                        pbAggValue.funnameresultpair(funIdx).value());
            }
        }
    }
}

void PBResultFormatterTest::checkErrorResults(
        const ResultPtr &resultPtr,
        const PBResult* pbResult)
{
    MultiErrorResultPtr &multiErrorResult = resultPtr->getMultiErrorResult();
    int errorResultCount = pbResult->errorresults_size();
    if (!multiErrorResult->hasError()) {
        ASSERT_EQ(0, errorResultCount);
    }

    const ErrorResults &errorResults = multiErrorResult->getErrorResults();
    ASSERT_EQ((int)errorResults.size(), errorResultCount);

    for (int i = 0; i < errorResultCount; i++) {
        const ErrorResult &errorResult = errorResults[i];
        const PBErrorResult &pbErrorResult = pbResult->errorresults(i);
        ASSERT_EQ((uint32_t)errorResult.getErrorCode(),
                  pbErrorResult.errorcode());
        ASSERT_EQ(errorResult.getErrorDescription(),
                  pbErrorResult.errordescription());
        ASSERT_EQ(errorResult.getPartitionID(),
                  pbErrorResult.partitionid());
        ASSERT_EQ(errorResult.getHostName(),
                  pbErrorResult.hostname());
    }
}

void PBResultFormatterTest::checkMatchDocsAttributes(const ResultPtr &resultPtr,
        PBMatchDocs pbMatchDocs)

{
    MatchDocs *matchDocs = resultPtr->getMatchDocs();
    const vector<matchdoc::MatchDoc>& resultMatchDocs = resultPtr->getMatchDocsForFormat();
    const auto &allocatorPtr = matchDocs->getMatchDocAllocator();
    ASSERT_TRUE(allocatorPtr != NULL);
    auto attrReferVec = allocatorPtr->getRefBySerializeLevel(SL_ATTRIBUTE);
    ASSERT_EQ((size_t)2, attrReferVec.size());
    for (size_t i = 0; i < resultMatchDocs.size(); ++i) {
        matchdoc::MatchDoc slab = resultMatchDocs[i];
        vector<int32_t> multiInt32Vec =
            VariableTypeTraits<vt_int32, true>::convert(
                    dynamic_cast<matchdoc::Reference<MultiValueType<int32_t> >*>(
                            attrReferVec[0])->getReference(slab));
        uint32_t beginPos = pbMatchDocs.int64attrvalues(0).offset(i);

        ASSERT_EQ(ATTRIBUTE_TYPE,
                  pbMatchDocs.int64attrvalues(0).type());
        string attrName = "ref0";
        if (_attrTransMap.find(attrName) != _attrTransMap.end()) {
            attrName = _attrTransMap[attrName];
        }
        ASSERT_EQ(attrName,
                  pbMatchDocs.int64attrvalues(0).key());
        uint32_t endPos = (i == resultMatchDocs.size() - 1) ?
                          pbMatchDocs.int64attrvalues(0).int64value_size() :
                          pbMatchDocs.int64attrvalues(0).offset(i + 1);
        ASSERT_EQ((uint32)multiInt32Vec.size(), endPos - beginPos);
        for (uint32_t j = beginPos; j < endPos; ++j) {
            ASSERT_EQ((int64_t)multiInt32Vec[j - beginPos],
                      pbMatchDocs.int64attrvalues(0).int64value(j));
        }

        ASSERT_EQ(ATTRIBUTE_TYPE,
                  pbMatchDocs.int64attrvalues(1).type());

        attrName = "ref1";
        if (_attrTransMap.find(attrName) != _attrTransMap.end()) {
            attrName = _attrTransMap[attrName];
        }
        ASSERT_EQ(attrName,
                  pbMatchDocs.int64attrvalues(1).key());
        ASSERT_EQ(dynamic_cast<matchdoc::Reference<int64_t>*>(
                        attrReferVec[1])->getReference(slab),
                  pbMatchDocs.int64attrvalues(1).int64value(i));
    }
}

void PBResultFormatterTest::checkMatchDocsUserDatas(const ResultPtr &resultPtr,
        PBMatchDocs pbMatchDocs)
{
    MatchDocs *matchDocs = resultPtr->getMatchDocs();
    const vector<matchdoc::MatchDoc>& resultMatchDocs = resultPtr->getMatchDocsForFormat();
    const auto &allocatorPtr = matchDocs->getMatchDocAllocator();
    ASSERT_TRUE(allocatorPtr != NULL);
    auto varReferVec = allocatorPtr->getRefBySerializeLevel(SL_VARIABLE);
    ASSERT_EQ((size_t)7, varReferVec.size());
#define FIND_INDEX_AND_REF(allocator, indexName, refName, refField, pbKey, pbfield, type) \
    int indexName = 0;                                                  \
    auto refName = allocatorPtr->findReference<type>(refField);         \
    for (;indexName < pbMatchDocs.pbfield##_size(); indexName++) {      \
        const auto &tmp = pbMatchDocs.pbfield(indexName);               \
        if (tmp.key() == pbKey) {                                       \
            break;                                                      \
        }                                                               \
    }

    FIND_INDEX_AND_REF(allocatorPtr, ref2PbIndex, ref2,
                       PROVIDER_VAR_NAME_PREFIX + "ref2", "ref2",
                       doubleattrvalues, double);
    FIND_INDEX_AND_REF(allocatorPtr, ref3PbIndex, ref3,
                       PROVIDER_VAR_NAME_PREFIX + "ref3", "ref3",
                       int64attrvalues, int32_t);
    FIND_INDEX_AND_REF(allocatorPtr, ref4PbIndex, ref4,
                       PROVIDER_VAR_NAME_PREFIX + "ref4", "ref4",
                       bytesattrvalues, std::string);
#undef FIND_INDEX_AND_REF

    for (size_t i = 0; i < resultMatchDocs.size(); ++i) {
        matchdoc::MatchDoc slab = resultMatchDocs[i];
        ASSERT_EQ(string("ref2"),
                  pbMatchDocs.doubleattrvalues(ref2PbIndex).key());
        ASSERT_EQ(VARIABLE_VALUE_TYPE,
                  pbMatchDocs.doubleattrvalues(ref2PbIndex).type());
        ASSERT_EQ(ref2->getReference(slab),
                  pbMatchDocs.doubleattrvalues(ref2PbIndex).doublevalue(i));

        ASSERT_EQ(string("ref3"),
                  pbMatchDocs.int64attrvalues(ref3PbIndex).key());
        ASSERT_EQ(VARIABLE_VALUE_TYPE,
                  pbMatchDocs.int64attrvalues(ref3PbIndex).type());
        ASSERT_EQ(ref3->getReference(slab),
                  pbMatchDocs.int64attrvalues(ref3PbIndex).int64value(i));

        ASSERT_EQ(string("ref4"),
                  pbMatchDocs.bytesattrvalues(ref4PbIndex).key());
        ASSERT_EQ(VARIABLE_VALUE_TYPE,
                  pbMatchDocs.bytesattrvalues(ref4PbIndex).type());
        auto attrItem = Ha3MatchDocAllocator::createAttributeItem(ref4, slab);
        ASSERT_TRUE(attrItem);
        ASSERT_EQ(attrItem->toString(), pbMatchDocs.
                  bytesattrvalues(ref4PbIndex).bytesvalue(i));
    }
}

void PBResultFormatterTest::checkMatchDocsSortValues(const ResultPtr &resultPtr,
        PBMatchDocs pbMatchDocs)
{
    MatchDocs *matchDocs = resultPtr->getMatchDocs();
    const vector<matchdoc::MatchDoc>& resultMatchDocs = resultPtr->getMatchDocsForFormat();
    const auto &allocatorPtr = matchDocs->getMatchDocAllocator();
    ASSERT_TRUE(allocatorPtr != NULL);

    matchdoc::ReferenceVector sortReferVec;
    const vector<SortExprMeta>& sortExprMetas = resultPtr->getSortExprMeta();
    ASSERT_EQ((size_t)2, sortExprMetas.size());

    for (size_t i = 0; i < resultMatchDocs.size(); ++i) {
        matchdoc::MatchDoc slab = resultMatchDocs[i];
        ASSERT_EQ((uint32_t)2,
                  pbMatchDocs.sortvalues().dimensioncount());
        ASSERT_EQ(dynamic_cast<const matchdoc::Reference<double>*>(
                        sortExprMetas[0].sortRef)->getReference(slab),
                  pbMatchDocs.sortvalues().sortvalues(2*i));
        ASSERT_EQ((double)dynamic_cast<const matchdoc::Reference<int32_t>*>(
                        sortExprMetas[1].sortRef)->getReference(slab),
                  pbMatchDocs.sortvalues().sortvalues(2*i + 1));
    }
}

void PBResultFormatterTest::checkMatchDocsMultiValue(const ResultPtr &resultPtr,
        PBMatchDocs pbMatchDocs)
{
    const vector<matchdoc::MatchDoc>& resultMatchDocs = resultPtr->getMatchDocsForFormat();
    for (size_t i = 0; i < resultMatchDocs.size(); ++i) {
        static double DOUBLE_DATA[] = {1.1, 2.2, 3.4};
        ASSERT_EQ(VARIABLE_VALUE_TYPE,
                  pbMatchDocs.doubleattrvalues(1).type());
        uint32_t beginPos = pbMatchDocs.doubleattrvalues(1).offset(i);
        uint32_t endPos = (i == resultMatchDocs.size() - 1) ?
                          pbMatchDocs.doubleattrvalues(1).doublevalue_size() :
                          pbMatchDocs.doubleattrvalues(1).offset(i + 1);
        for (uint32_t j = beginPos; j < endPos; ++j) {
            ASSERT_EQ(DOUBLE_DATA[j - beginPos],
                      pbMatchDocs.doubleattrvalues(1).doublevalue(j));
        }

        ASSERT_EQ(VARIABLE_VALUE_TYPE,
                  pbMatchDocs.bytesattrvalues(1).type());
        beginPos = pbMatchDocs.bytesattrvalues(1).offset(i);
        endPos = (i == resultMatchDocs.size() - 1) ?
                 pbMatchDocs.bytesattrvalues(1).bytesvalue_size() :
                 pbMatchDocs.bytesattrvalues(1).offset(i + 1);
        string str[2] = {"abc", "123456"};
        for (uint32_t j = beginPos; j < endPos; ++j) {
            ASSERT_EQ(str[j - beginPos],
                      pbMatchDocs.bytesattrvalues(1).bytesvalue(j));
        }
    }
}

void PBResultFormatterTest::checkMatchDocs(const ResultPtr &resultPtr,
        PBMatchDocs pbMatchDocs, const vector<clusterid_t> &clusterIds)
{
    MatchDocs *matchDocs = resultPtr->getMatchDocs();
    vector<matchdoc::MatchDoc>& resultMatchDocs = resultPtr->getMatchDocsForFormat();
    ASSERT_EQ(resultMatchDocs.size(),
              (size_t)pbMatchDocs.nummatchdocs());
    ASSERT_EQ(matchDocs->totalMatchDocs(),
              pbMatchDocs.totalmatchdocs());
    const vector<string>& clusterNames = resultPtr->getClusterNames();
    ASSERT_EQ(clusterNames.size(),
              (size_t)pbMatchDocs.clusternames_size());
    for (int i = 0; i < pbMatchDocs.clusternames_size(); ++i) {
        ASSERT_EQ(clusterNames[i], pbMatchDocs.clusternames(i));
    }
    ASSERT_EQ(resultMatchDocs.size(),
              (size_t)pbMatchDocs.docids_size());
    checkMatchDocsAttributes(resultPtr, pbMatchDocs);
    checkMatchDocsUserDatas(resultPtr, pbMatchDocs);
    checkMatchDocsSortValues(resultPtr, pbMatchDocs);
    checkMatchDocsMultiValue(resultPtr, pbMatchDocs);

    matchdoc::Reference<FullIndexVersion> *fullVersionRef =
        matchDocs->getFullIndexVersionRef();
    if (!fullVersionRef) {
        ASSERT_EQ(int(0), pbMatchDocs.fullindexversions_size());
    }

    matchdoc::Reference<versionid_t> *indexVersionRef =
        matchDocs->getIndexVersionRef();
    if (!indexVersionRef) {
        ASSERT_EQ(int(0), pbMatchDocs.indexversions_size());
    }

    matchdoc::Reference<primarykey_t> *primaryKey128Ref =
        matchDocs->getPrimaryKey128Ref();
    matchdoc::Reference<uint64_t> *primaryKey64Ref =
        matchDocs->getPrimaryKey64Ref();
    if (!primaryKey128Ref) {
        ASSERT_EQ(int(0), pbMatchDocs.pkhighers_size());
        if (!primaryKey64Ref) {
            ASSERT_EQ(int(0), pbMatchDocs.pklowers_size());
        }
    }

    matchdoc::Reference<uint32_t> *ipRef = matchDocs->getIpRef();
    if (!ipRef) {
        ASSERT_EQ(int(0), pbMatchDocs.searcherips_size());
    }

    auto hashIdRef = matchDocs->getHashIdRef();
    assert(hashIdRef);
    for (size_t i = 0; i < resultMatchDocs.size(); ++i) {
        ASSERT_EQ((uint32_t)clusterIds[i], pbMatchDocs.clusterids(i));
        ASSERT_EQ((uint32_t)hashIdRef->get(resultMatchDocs[i]),
                  pbMatchDocs.hashids(i));
        ASSERT_EQ((uint32_t)resultMatchDocs[i].getDocId(),
                  pbMatchDocs.docids(i));
        ASSERT_EQ(string("test tracer\n"),
                  pbMatchDocs.tracers(i));
        matchdoc::MatchDoc slab = resultMatchDocs[i];
        if (fullVersionRef) {
            FullIndexVersion &fullVersion = fullVersionRef->getReference(slab);
            ASSERT_EQ((int32_t)fullVersion,
                      pbMatchDocs.fullindexversions(i));
        }
        if (indexVersionRef) {
            versionid_t &version = indexVersionRef->getReference(slab);
            ASSERT_EQ((int32_t)version,
                      pbMatchDocs.indexversions(i));
        }

        if (primaryKey128Ref) {
            primarykey_t primaryKey128 = primaryKey128Ref->getReference(slab);
            ASSERT_EQ((uint64_t)primaryKey128.value[0],
                      pbMatchDocs.pkhighers(i));
            ASSERT_EQ((uint64_t)primaryKey128.value[1],
                      pbMatchDocs.pklowers(i));
        } else if (primaryKey64Ref) {
            uint64_t primaryKey64 = primaryKey64Ref->getReference(slab);
            ASSERT_EQ(primaryKey64, pbMatchDocs.pklowers(i));
        }
        if (ipRef) {
            uint32_t ip = ipRef->getReference(slab);
            ASSERT_EQ((uint32_t)ip, pbMatchDocs.searcherips(i));
        }
    }
}

END_HA3_NAMESPACE(common);
