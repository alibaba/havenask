#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/MatchDocs2Hits.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <autil/mem_pool/Pool.h>
#include <ha3/common/Request.h>
#include <ha3/common/MatchDocs.h>
#include <ha3/common/ConfigClause.h>
#include <matchdoc/MatchDoc.h>
#include <ha3_sdk/testlib/common/MatchDocCreator.h>
#include <autil/StringTokenizer.h>
#include <ha3/queryparser/RequestParser.h>

using namespace std;
using namespace autil;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(search);
BEGIN_HA3_NAMESPACE(qrs);

class MatchDocs2HitsTest : public TESTBASE {
public:
    MatchDocs2HitsTest();
    ~MatchDocs2HitsTest();
public:
    void setUp();
    void tearDown();
protected:
    std::vector<common::SortExprMeta> createSortExprMeta(
            const std::string &sortExprMetaStr, matchdoc::MatchDocAllocator *allocator);
    common::RequestPtr prepareRequest(const std::string &requestStr);

protected:
    autil::mem_pool::Pool _pool;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(qrs, MatchDocs2HitsTest);


MatchDocs2HitsTest::MatchDocs2HitsTest() { 
}

MatchDocs2HitsTest::~MatchDocs2HitsTest() { 
}

void MatchDocs2HitsTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void MatchDocs2HitsTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(MatchDocs2HitsTest, testExtractNames) { 
    HA3_LOG(DEBUG, "Begin Test!");
    common::Ha3MatchDocAllocatorPtr allocatorPtr(new common::Ha3MatchDocAllocator(&_pool));
    matchdoc::ReferenceVector referVec;
    vector<string> nameVec;

    string name1 = "name1";
    string name2 = "name2";
    matchdoc::ReferenceBase *refer1 = allocatorPtr->declare<int8_t>(name1);
    referVec.push_back(refer1);
    matchdoc::ReferenceBase *refer2 = allocatorPtr->declare<int8_t>(name2);
    referVec.push_back(refer2);

    MatchDocs2Hits convert;
    convert.extractNames(referVec, "", nameVec);
    ASSERT_EQ(size_t(2), nameVec.size());
    ASSERT_EQ(string("name1"), nameVec[0]);
    ASSERT_EQ(string("name2"), nameVec[1]);
}

TEST_F(MatchDocs2HitsTest, testConvertWhenStartOffsetLargerThanMatchDocsSize)
{
    common::Ha3MatchDocAllocator *matchDocAllocator = new common::Ha3MatchDocAllocator(&_pool);
    common::Ha3MatchDocAllocatorPtr matchDocAllocatorPtr(matchDocAllocator);
    MatchDocs matchDocs;
    matchDocs.setMatchDocAllocator(matchDocAllocatorPtr);

    string matchDocStr = "docid : int32 : 1,2;"
                         "hashid       : int16   : 0, 0;"
                         "clusterid    : uint8   : 0, 0;"
                         "v1    : int32 : 1,3;"
                         "v2    : int32 : 2,4;";
    MatchDocCreator::createMatchDocs(matchDocStr,
            matchDocs.getMatchDocsVect(), matchDocAllocator, &_pool);

    RequestPtr requestPtr = prepareRequest("config=start:2");

    vector<SortExprMeta> sortExprMetaVec;
    MatchDocs2Hits convert;
    ErrorResult errResult;
    vector<string> clusterList;
    Hits *hits = convert.convert(&matchDocs, requestPtr, sortExprMetaVec, errResult, clusterList);
    unique_ptr<Hits> hits_ptr(hits);
    ASSERT_EQ((uint32_t)0, hits->size());
}

TEST_F(MatchDocs2HitsTest, testSetSortFlag) {
    common::Ha3MatchDocAllocator *matchDocAllocator = new common::Ha3MatchDocAllocator(&_pool);
    common::Ha3MatchDocAllocatorPtr matchDocAllocatorPtr(matchDocAllocator);
    MatchDocs matchDocs;
    matchDocs.setMatchDocAllocator(matchDocAllocatorPtr);

    string matchDocStr = "docid : int32 : 1,2;"
                         "hashid       : int16   : 0, 0;"
                         "clusterid    : uint8   : 0, 0;"
                         "v1    : int32 : 1,3;"
                         "v2    : int32 : 2,4;";
    MatchDocCreator::createMatchDocs(matchDocStr,
            matchDocs.getMatchDocsVect(), matchDocAllocator, &_pool);

    string sortExprMetaStr = "+v1;+v2";
    vector<SortExprMeta> sortExprMetas = createSortExprMeta(
            sortExprMetaStr, matchDocAllocator);

    RequestPtr requestPtr = prepareRequest("config=hit:2");

    MatchDocs2Hits convert;
    ErrorResult errResult;
    vector<string> clusterList;
    Hits *hits = convert.convert(&matchDocs, requestPtr, sortExprMetas, errResult, clusterList);
    unique_ptr<Hits> hits_ptr(hits);

    ASSERT_EQ((uint32_t)2, hits->size());
    ASSERT_TRUE(hits->getSortAscendFlag());
    
    HitPtr hitPtr = hits->getHit(0);
    ASSERT_EQ(uint32_t(2), hitPtr->getSortExprCount());
    ASSERT_EQ(string("1"), hitPtr->getSortExprValue(0));
    ASSERT_EQ(string("2"), hitPtr->getSortExprValue(1));
    hitPtr = hits->getHit(1);
    ASSERT_EQ(uint32_t(2), hitPtr->getSortExprCount());
    ASSERT_EQ(string("3"), hitPtr->getSortExprValue(0));
    ASSERT_EQ(string("4"), hitPtr->getSortExprValue(1));
}

TEST_F(MatchDocs2HitsTest, testConvertWhenStartOffsetLessThanMatchDocsSize)
{
    int matchDocCount = 5;
    MatchDocs *matchDocs = new MatchDocs();
    unique_ptr<MatchDocs> matchDocs_ptr(matchDocs);
    matchDocs->setTotalMatchDocs(matchDocCount);

    common::Ha3MatchDocAllocator *matchDocAllocator = new common::Ha3MatchDocAllocator(&_pool);
    common::Ha3MatchDocAllocatorPtr matchDocAllocatorPtr(matchDocAllocator);
    matchDocs->setMatchDocAllocator(matchDocAllocatorPtr);
    matchDocAllocator->declare<hashid_t>(HASH_ID_REF, common::HA3_GLOBAL_INFO_GROUP,
            SL_QRS);
    matchDocAllocator->declare<clusterid_t>(CLUSTER_ID_REF,
            CLUSTER_ID_REF_GROUP);
    for (int i = 0; i < matchDocCount; ++i) {
        matchdoc::MatchDoc matchDoc = matchDocAllocator->allocate(i);
        ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != matchDoc);
        matchDocs->addMatchDoc(matchDoc);
    }

    RequestPtr requestPtr(new Request);
    ConfigClause *configClause = new ConfigClause;
    configClause->setStartOffset(2);
    configClause->setHitCount(2);
    requestPtr->setConfigClause(configClause);

    vector<SortExprMeta> sortExprMetaVec;
    MatchDocs2Hits convert;
    ErrorResult errResult;
    vector<string> clusterList;
    Hits *hits = convert.convert(matchDocs, requestPtr, sortExprMetaVec, errResult, clusterList);
    unique_ptr<Hits> hits_ptr(hits);
    ASSERT_EQ((uint32_t)2, hits->size());
}

TEST_F(MatchDocs2HitsTest, testConvertWithClusterName)
{
    int matchDocCount = 3;
    MatchDocs *matchDocs = new MatchDocs();
    unique_ptr<MatchDocs> matchDocs_ptr(matchDocs);
    matchDocs->setTotalMatchDocs(matchDocCount);

    common::Ha3MatchDocAllocator *matchDocAllocator = new common::Ha3MatchDocAllocator(&_pool);
    common::Ha3MatchDocAllocatorPtr matchDocAllocatorPtr(matchDocAllocator);
    matchDocs->setMatchDocAllocator(matchDocAllocatorPtr);

    matchDocAllocator->declare<hashid_t>(HASH_ID_REF, common::HA3_GLOBAL_INFO_GROUP,
            SL_QRS);
    matchDocAllocator->declare<clusterid_t>(CLUSTER_ID_REF,
            CLUSTER_ID_REF_GROUP);
    auto clusterIdRef = matchDocs->getClusterIdRef();
    for (int i = 0; i < matchDocCount; ++i) {
        matchdoc::MatchDoc matchDoc = matchDocAllocator->allocate(i);
        ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != matchDoc);
        clusterIdRef->set(matchDoc, i);
        matchDocs->addMatchDoc(matchDoc);
    }

    RequestPtr requestPtr(new Request);
    ConfigClause *configClause = new ConfigClause;
    requestPtr->setConfigClause(configClause);

    vector<SortExprMeta> sortExprMetaVec;
    MatchDocs2Hits convert;
    ErrorResult errResult;
    vector<string> clusterList;
    clusterList.push_back("mainse_good");
    clusterList.push_back("mainse_bad");
    Hits *hits = convert.convert(matchDocs, requestPtr, sortExprMetaVec, errResult, clusterList);
    unique_ptr<Hits> hits_ptr(hits);
    ASSERT_EQ((uint32_t)3, hits->size());
    HitPtr hit = hits->getHit(0);
    ASSERT_EQ(string("mainse_good"), hit->getClusterName());
    hit = hits->getHit(1);
    ASSERT_EQ(string("mainse_bad"), hit->getClusterName());
    hit = hits->getHit(2);
    ASSERT_EQ(string(""), hit->getClusterName());
}

TEST_F(MatchDocs2HitsTest, testConvertPhaseOneBasicInfos) {
    common::Ha3MatchDocAllocator *allocator = new common::Ha3MatchDocAllocator(&_pool);
    common::Ha3MatchDocAllocatorPtr matchDocAllocatorPtr(allocator);
    MatchDocs matchDocs;
    matchDocs.setMatchDocAllocator(matchDocAllocatorPtr);
    string matchDocStr = "docid        : int32   : 1,2;"
                         "indexversion : int32   : 3,4;"
                         "hashid       : int16   : 11,666;"
                         "fullversion  : uint32  : 33,34;"
                         "clusterid    : uint8   : 1,3;"
                         "ip           : uint32  : 222,333;"
                         "pkattr       : uint64  : 123456789,111122223333;"
                         "rawpk        : string  : 1111,2222;";
    MatchDocCreator::createMatchDocs(matchDocStr, matchDocs.getMatchDocsVect(),
            allocator, &_pool);

    RequestPtr requestPtr = prepareRequest("config=hit:2");

    ErrorResult errResult;
    vector<SortExprMeta> sortExprMetaVec;
    MatchDocs2Hits convert;
    vector<string> clusterList;
    Hits *hits = convert.convert(&matchDocs, requestPtr, sortExprMetaVec, errResult, clusterList);
    unique_ptr<Hits> hitsPtr(hits);

    ASSERT_EQ((uint32_t)2, hits->size());
    {
        HitPtr hit = hits->getHit(0);
        ASSERT_EQ(docid_t(1), hit->getDocId());
        ASSERT_EQ(versionid_t(3), hit->getIndexVersion());
        ASSERT_EQ(hashid_t(11), hit->getHashId());
        ASSERT_EQ(FullIndexVersion(33), hit->getFullIndexVersion());
        ASSERT_EQ(clusterid_t(1), hit->getClusterId());
        ASSERT_EQ(uint32_t(222), hit->getIp());
        ASSERT_EQ(primarykey_t(123456789), hit->getPrimaryKey());
        ASSERT_EQ(std::string("1111"), hit->getRawPk());
    }
    {
        HitPtr hit = hits->getHit(1);
        ASSERT_EQ(docid_t(2), hit->getDocId());
        ASSERT_EQ(versionid_t(4), hit->getIndexVersion());
        ASSERT_EQ(hashid_t(666), hit->getHashId());
        ASSERT_EQ(FullIndexVersion(34), hit->getFullIndexVersion());
        ASSERT_EQ(clusterid_t(3), hit->getClusterId());
        ASSERT_EQ(uint32_t(333), hit->getIp());
        ASSERT_EQ(primarykey_t(111122223333LL), hit->getPrimaryKey());
        ASSERT_EQ(std::string("2222"), hit->getRawPk());
    }
}

vector<SortExprMeta> MatchDocs2HitsTest::createSortExprMeta(
        const string &sortExprMetaStr, matchdoc::MatchDocAllocator *allocator)
{
    vector<SortExprMeta> ret;
    StringTokenizer st(sortExprMetaStr, ";", StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        SortExprMeta meta;
        meta.sortFlag = st[i][0] != '-';
        meta.sortExprName = st[i].substr(1);
        meta.sortRef = allocator->findReferenceWithoutType(meta.sortExprName);
        ret.push_back(meta);
    }
    return ret;
}

RequestPtr MatchDocs2HitsTest::prepareRequest(const string &requestStr) {
    queryparser::RequestParser parser;
    common::RequestPtr request(new Request);
    request->setOriginalString(requestStr);
    bool ret = parser.parseConfigClause(request);
    (void)ret;
    assert(ret);
    if (request->getVirtualAttributeClause()) {
        ret = parser.parseVirtualAttributeClause(request);
        assert(ret);
    }
    return request;
}


END_HA3_NAMESPACE(qrs);

