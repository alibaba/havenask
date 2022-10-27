#include "ha3/common/HashJoinInfo.h"
#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/SingleLayerSearcher.h>
#include <indexlib/index/normal/deletionmap/deletion_map_reader.h>
#include <matchdoc/SubDocAccessor.h>
#include <ha3/search/test/QueryExecutorMock.h>
#include <autil/StringUtil.h>
#include <autil/StringTokenizer.h>
#include <autil/SimpleSegregatedAllocator.h>
#include <ha3/search/QueryExecutorCreator.h>
#include <ha3/search/test/FakeAttributeExpression.h>
#include <ha3/search/test/LayerMetasConstructor.h>
#include <ha3/search/test/SearcherTestHelper.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <ha3/search/test/QueryExecutorTestHelper.h>
#include <ha3/search/test/JoinFilterCreatorForTest.h>
#include <ha3/sql/data/TableUtil.h>

using namespace std;
using namespace autil;
using namespace suez::turing;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(sql);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(partition);

BEGIN_HA3_NAMESPACE(search);

class SingleLayerSearcherTest : public TESTBASE {
public:
    SingleLayerSearcherTest();
    ~SingleLayerSearcherTest();
public:
    void setUp();
    void tearDown();
protected:
    typedef IE_NAMESPACE(index)::JoinDocidAttributeIterator DocMapAttrIterator;
protected:
    void internalTestSeek(const std::string &docsInIndex, const std::string &layerMeta,
                          const std::string &filterStr, const std::string &delDocIds,
                          const std::string &expectResult, int32_t seekedCount = -1,
                          int32_t leftQuota = -1);
    void initDeletionMapReader(const std::string &delDocIds,
                               IE_NAMESPACE(index)::DeletionMapReaderPtr& delReader);
    std::vector<docid_t> seekSubDoc(docid_t docId,QueryExecutor *queryExecutor,
                                    common::Ha3MatchDocAllocator *matchDocAllocator,
                                    SingleLayerSearcher *searcher,
                                    matchdoc::MatchDoc matchDoc);
    std::vector<matchdoc::MatchDoc>
    seekSubMatchDoc(docid_t docId, QueryExecutor *queryExecutor,
                    common::Ha3MatchDocAllocator *matchDocAllocator,
                    SingleLayerSearcher *searcher, matchdoc::MatchDoc matchDoc);

    FilterWrapperPtr
    createFilter(const std::string &filterStr,
                 common::Ha3MatchDocAllocator *allocator = NULL);

    HashJoinInfoPtr buildHashJoinInfo(
            const unordered_map<size_t, vector<int32_t>> &hashJoinMap) {
        HashJoinInfoPtr hashJoinInfo(
                new HashJoinInfo(_auxTableName, _joinFieldName));
	hashJoinInfo->getHashJoinMap() = hashJoinMap;
	return hashJoinInfo;
    }

    void internalTestSeekAndJoin(const std::string &docsInIndex,
            const std::string &layerMeta,
            const std::string &filterStr,
            const std::string &delDocIds,
            const std::string &storeIds,
            const HashJoinInfoPtr &hashJoinInfo,
            const std::string &expectResult,
            const std::string &expectAuxDocIds,
            int32_t seekedCount = -1,
            int32_t leftQuota = -1);

    MultiChar createMultiChar(const char *s) {
	char *buf = MultiValueCreator::createMultiValueBuffer(s, strlen(s), _pool);
	return MultiChar(buf);
    }

protected:
    IE_NAMESPACE(index)::DeletionMapReaderPtr _delReaderPtr;
    IE_NAMESPACE(index)::DeletionMapReaderPtr _subDelReaderPtr;
    autil::mem_pool::Pool *_pool;
    AttributeExpressionTyped<bool> *_attrExpr;
    string _auxTableName{"store"};
    string _joinFieldName{"store_id"};
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, SingleLayerSearcherTest);

SingleLayerSearcherTest::SingleLayerSearcherTest() {
}

SingleLayerSearcherTest::~SingleLayerSearcherTest() {
}

void SingleLayerSearcherTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
    _attrExpr = NULL;
    _pool = new autil::mem_pool::Pool(1024);
}

void SingleLayerSearcherTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
    DELETE_AND_SET_NULL(_attrExpr);
    delete _pool;
}

TEST_F(SingleLayerSearcherTest, testSimpleProcess) {
    HA3_LOG(DEBUG, "Begin Test!");

    string docsInIndex("1,3,4,5,6,7");
    string layerMeta("1,3,4;6,10,10");
    string expectResult("1,3,6,7");
    internalTestSeek(docsInIndex, layerMeta, "", "", expectResult, 8, 10);
}

TEST_F(SingleLayerSearcherTest, testLastDocNotInRange) {
    HA3_LOG(DEBUG, "Begin Test!");

    string docsInIndex("1,3,4,5,6,7,11");
    string layerMeta("1,3,4;6,10,10");
    string expectResult("1,3,6,7");
    internalTestSeek(docsInIndex, layerMeta, "", "", expectResult, 8, 10);
}

TEST_F(SingleLayerSearcherTest, testQuotaNotEnoughInOneRange) {
    HA3_LOG(DEBUG, "Begin Test!");
    {
        string docsInIndex("1,3,4,5,6,7,11");
        string layerMeta("1,3,1;6,10,10");
        string expectResult("1,6,7,3");
        internalTestSeek(docsInIndex, layerMeta, "", "", expectResult, 8, 7);
    }
    {
        string docsInIndex("1,3,4,5,6,7,11");
        string layerMeta("1,3,3;6,10,1");
        string expectResult("1,3,6,7");
        internalTestSeek(docsInIndex, layerMeta, "", "", expectResult, 5, 0);
    }
    {
        string docsInIndex("1,2,4,5,6,7,11");
        string layerMeta("1,3,3;6,10,1");
        string expectResult("1,2,6,7");
        internalTestSeek(docsInIndex, layerMeta, "", "", expectResult, 5, 0);
    }
}

TEST_F(SingleLayerSearcherTest, testQuotaNotEnoughInAllRange) {
    HA3_LOG(DEBUG, "Begin Test!");

    string docsInIndex("1,3,4,5,6,7,11");
    string layerMeta("1,3,0;6,10,1");
    string expectResult("6");
    internalTestSeek(docsInIndex, layerMeta, "", "", expectResult, 1, 0);
}

TEST_F(SingleLayerSearcherTest, testOutofRange) {
    HA3_LOG(DEBUG, "Begin Test!");
    {
        string docsInIndex("11,12,13");
        string layerMeta("1,3,3;6,10,6");
        string expectResult("");
        internalTestSeek(docsInIndex, layerMeta, "", "", expectResult, 8, 9);
    }
    {
        string docsInIndex("11,12,13");
        string layerMeta("1,3,3;6,7,6;9,10,2;11,13,2");
        string expectResult("11,12,13");
        internalTestSeek(docsInIndex, layerMeta, "", "", expectResult, 10, 10);
    }
    {
        string docsInIndex("1,2,3");
        string layerMeta("6,10,6");
        string expectResult("");
        internalTestSeek(docsInIndex, layerMeta, "", "", expectResult, 5, 6);
    }
}

TEST_F(SingleLayerSearcherTest, testContinueRange) {
    HA3_LOG(DEBUG, "Begin Test!");

    string docsInIndex("1,2,3,4,7");
    string layerMeta("1,2,2;3,4,3");
    string expectResult("1,2,3,4");
    internalTestSeek(docsInIndex, layerMeta, "", "", expectResult, 4, 1);
}

TEST_F(SingleLayerSearcherTest, testSeekWithFilter) {
    HA3_LOG(DEBUG, "Begin Test!");

    string docsInIndex("1,2,3,4,7");
    string layerMeta("1,2,2;3,4,3");
    string filterStr("1,2,3,6");
    string expectResult("1,2,3");
    internalTestSeek(docsInIndex, layerMeta, filterStr, "", expectResult, 4, 2);
}

TEST_F(SingleLayerSearcherTest, testSeekWithFilterNoUseQuota) {
    HA3_LOG(DEBUG, "Begin Test!");

    string docsInIndex("1,2,3,4,7");
    string layerMeta("1,3,1;6,8,3");
    string filterStr("2,3");
    string expectResult("2,3");
    internalTestSeek(docsInIndex, layerMeta, filterStr, "", expectResult, 6, 2);
}

TEST_F(SingleLayerSearcherTest, testGetFactor) {
    HA3_LOG(DEBUG, "Begin Test!");

    string docsInIndex("1,2,3,4,7,16,19,20,22,25");
    string layerMetaStr("1,3,1;6,8,3;16,25,1");

    common::Ha3MatchDocAllocator matchDocAllocator(_pool);

    QueryExecutorMock queryExecutor(docsInIndex);
    initDeletionMapReader("", _delReaderPtr);

    LayerMeta layerMeta = LayerMetasConstructor::createLayerMeta(
            _pool, layerMetaStr);
    SingleLayerSearcher searcher(&queryExecutor, &layerMeta, NULL, _delReaderPtr.get(), &matchDocAllocator, NULL, NULL, _subDelReaderPtr.get());
    vector<uint32_t> result;
    result.push_back(1);
    result.push_back(3);
    result.push_back(5);
    result.push_back(8);
    result.push_back(9);

    for (size_t i = 0; i < result.size(); ++i) {
        matchdoc::MatchDoc matchDoc;
        ASSERT_EQ(IE_NAMESPACE(common)::ErrorCode::OK, searcher.seek(false, matchDoc));
        HA3_LOG(INFO, "expect docid: [%d]", result[i]);
        ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != matchDoc);
        ASSERT_EQ(result[i], searcher.getSeekedCount());
        HA3_LOG(INFO, "expect docid: [%d]", matchDoc.getDocId());
    }
    matchdoc::MatchDoc matchDoc;
    ASSERT_EQ(IE_NAMESPACE(common)::ErrorCode::OK, searcher.seek(false, matchDoc));
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC == matchDoc);
}

TEST_F(SingleLayerSearcherTest, testQuotaModePerLayer) {
    {
        string docsInIndex("1,2,3,4,7");
        string layerMeta("param,10,QM_PER_LAYER;1,3,0;6,8,0");
        string expectResult("1,2,3,7");
        internalTestSeek(docsInIndex, layerMeta, "", "", expectResult, 6, 6);
    }
    {
        string docsInIndex("1,2,3,4,7");
        string layerMeta("param,2,QM_PER_LAYER;1,3,0;6,8,0");
        string expectResult("1,2,3,7");
        internalTestSeek(docsInIndex, layerMeta, "", "", expectResult, 6, 0);
    }
    {
        string docsInIndex("1,2,3,4,7");
        string layerMeta("param,2,QM_PER_LAYER,3;1,3,0;6,8,0")
;
        string expectResult("1,2,3");
        internalTestSeek(docsInIndex, layerMeta, "", "", expectResult, 3, 0);
    }
}

TEST_F(SingleLayerSearcherTest, testLayerMetaSearchReset) {
    {
        string docsInIndex("1,2,3,4,5,6,7,8,9,10,11,12,13,14,15");
        string layerMeta("1,3,1;4,5,2;6,7,3");
        string expectResult("1,4,5,6,7,2");
        internalTestSeek(docsInIndex, layerMeta, "", "", expectResult, 6, 0);
    }
    {
        string docsInIndex("1,2,3,4,5,6,7,8,9,10,11,12,13,14,15");
        string layerMeta("1,3,3;5,8,2;10,14,8");
        string expectResult("1,2,3,5,6,10,11,12,13,14,7,8");
        internalTestSeek(docsInIndex, layerMeta, "", "", expectResult, 12, 1);
    }
    {
        string docsInIndex("1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20");
        string layerMeta("1,3,3;5,8,2;10,14,2;16,17,1;19,20,8");
        string expectResult("1,2,3,5,6,10,11,16,19,20,7,8,12,13,14,17");
        internalTestSeek(docsInIndex, layerMeta, "", "", expectResult, 16, 0);
    }
    {
        string docsInIndex("1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20");
        string layerMeta("1,3,3;5,8,2;10,14,2;16,17,1;19,20,7");
        string expectResult("1,2,3,5,6,10,11,16,19,20,7,8,12,13,14");
        internalTestSeek(docsInIndex, layerMeta, "", "", expectResult, 15, 0);
    }
    {
        string docsInIndex("1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20");
        string layerMeta("1,3,3;5,8,2;10,14,5;16,17,1;19,20,5");
        string expectResult("1,2,3,5,6,10,11,12,13,14,16,19,20,7,8,17");
        internalTestSeek(docsInIndex, layerMeta, "", "", expectResult, 16, 0);
    }
}

TEST_F(SingleLayerSearcherTest, testDeletionMap) {
    // Reset + some element in deletionMap
    {
        string docsInIndex("1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20");
        string layerMeta("1,3,3;5,8,2;10,14,5;16,17,1;19,20,5");
        string expectResult("2,5,6,7,8,10,11,12,13,14,16,20,17");
        string deletionMap("1,3,19");
        internalTestSeek(docsInIndex, layerMeta, "", deletionMap, expectResult, 16, 3);
    }
    // Reset + deletionMap with all element
    {
        string docsInIndex("1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20");
        string layerMeta("1,3,3;5,8,2;10,14,5;16,17,1;19,20,5");
        string expectResult("");
        string deletionMap("1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20");
        internalTestSeek(docsInIndex, layerMeta, "", deletionMap, expectResult, 16, 16);
    }
    //no Reset
    {
        string docsInIndex("1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,22,25,28,31");
        string layerMeta("1,3,3;5,8,2;10,14,5;16,17,1;19,20,1;23,33,2");
        string expectResult("2,5,6,7,8,10,11,12,13,14,16,19,25,28");
        string deletionMap("1,3");
        internalTestSeek(docsInIndex, layerMeta, "", deletionMap, expectResult, 20, 0);
    }
    //boundary no reset
    {
        string docsInIndex("1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,22,25,28,31");
        string layerMeta("1,3,3;5,8,2;10,14,5;16,17,1;19,20,1;23,33,2");
        string expectResult("2,5,6,7,8,10,11,12,13,14,16,19,25,31");
        string deletionMap("1,3,28");
        internalTestSeek(docsInIndex, layerMeta, "", deletionMap, expectResult, 23, 0);
    }
    //boundary dirve reset
    {
        string docsInIndex("1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,22,25,28,31");
        string layerMeta("1,3,3;5,8,2;10,14,5;16,17,1;19,20,1;23,33,2");
        string expectResult("2,5,6,7,8,10,11,12,13,14,16,19,25,17");
        string deletionMap("1,3,28,31");
        internalTestSeek(docsInIndex, layerMeta, "", deletionMap, expectResult, 26, 0);
    }
    //deletionMap cover one range
    {
        string docsInIndex("1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,22,25,28,31");
        string layerMeta("1,3,3;5,8,2;10,14,5;16,17,1;19,20,1;23,33,2");
        string expectResult("1,2,3,5,6,10,11,12,13,14,19,20,25,28");
        string deletionMap("16,17");
        internalTestSeek(docsInIndex, layerMeta, "", deletionMap, expectResult, 20, 0);
    }
}

vector<docid_t> SingleLayerSearcherTest::seekSubDoc(
        docid_t docId, QueryExecutor *queryExecutor,
        common::Ha3MatchDocAllocator *matchDocAllocator,
        SingleLayerSearcher *searcher,
        matchdoc::MatchDoc matchDoc)
{
    docid_t id = queryExecutor->legacySeek(docId);
    (void)id;
    assert(docId == id);
    matchDoc = matchDocAllocator->allocate(docId);
    searcher->constructSubMatchDocs(matchDoc);
    matchdoc::SubDocAccessor* accessor = matchDocAllocator->getSubDocAccessor();
    vector<docid_t> subId;
    accessor->getSubDocIds(matchDoc, subId);
    return subId;
}

vector<matchdoc::MatchDoc> SingleLayerSearcherTest::seekSubMatchDoc(
        docid_t docId, QueryExecutor *queryExecutor,
        common::Ha3MatchDocAllocator *matchDocAllocator,
        SingleLayerSearcher *searcher,
        matchdoc::MatchDoc matchDoc)
{
    docid_t id = queryExecutor->legacySeek(docId);
    (void)id;
    assert(docId == id);
    matchDoc = matchDocAllocator->allocate(docId);
    searcher->constructSubMatchDocs(matchDoc);
    matchdoc::SubDocAccessor* accessor = matchDocAllocator->getSubDocAccessor();
    std::vector<matchdoc::MatchDoc> subDocs;
    accessor->getSubDocs(matchDoc, subDocs);
    return subDocs;
}

TEST_F(SingleLayerSearcherTest, testSimpleConstruct) {
    initDeletionMapReader("", _subDelReaderPtr);
    initDeletionMapReader("", _delReaderPtr);
    common::Ha3MatchDocAllocator matchDocAllocator(_pool, true);
    string layerMetaStr("0,1000,1000");
    LayerMeta layerMeta = LayerMetasConstructor::createLayerMeta(
            _pool, layerMetaStr);
    FakeIndex fakeIndex;
    QueryExecutorTestHelper::addSubDocAttrMap(fakeIndex, "2,4,8");
    IndexPartitionReaderWrapperPtr indexPartReader =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    QueryExecutorMock queryExecutor("0,1,2");
    const IndexPartitionReaderPtr &readerPtr = indexPartReader->getReader();
    AttributeReaderPtr attrReader = readerPtr->GetAttributeReader(MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME);
    AttributeIteratorBase* mainDocToSubDocIt = attrReader->CreateIterator(_pool);
    DocMapAttrIterator *typedMainDocToSubDocIt =
        dynamic_cast<DocMapAttrIterator*>(mainDocToSubDocIt);
    SingleLayerSearcher searcher(&queryExecutor, &layerMeta, NULL,
                                 _delReaderPtr.get(), &matchDocAllocator, NULL,
                                 typedMainDocToSubDocIt, _subDelReaderPtr.get());
    queryExecutor.legacySeek(0);
    auto matchDoc1 = matchDocAllocator.allocate(0);
    searcher.constructSubMatchDocs(matchDoc1);
    vector<docid_t> subId1 = seekSubDoc(docid_t(0), &queryExecutor,
            &matchDocAllocator, &searcher, matchDoc1);
    ASSERT_EQ(docid_t(0), matchDoc1.getDocId());
    ASSERT_EQ(docid_t(0), subId1[0]);
    ASSERT_EQ(docid_t(1), subId1[1]);

    queryExecutor.legacySeek(1);
    auto matchDoc2 = matchDocAllocator.allocate(1);
    searcher.constructSubMatchDocs(matchDoc2);
    vector<docid_t> subId2 = seekSubDoc(docid_t(1), &queryExecutor,
            &matchDocAllocator, &searcher, matchDoc2);
    ASSERT_EQ(docid_t(1), matchDoc2.getDocId());
    ASSERT_EQ(docid_t(2), subId2[0]);
    ASSERT_EQ(docid_t(3), subId2[1]);

    queryExecutor.legacySeek(2);
    auto matchDoc3 = matchDocAllocator.allocate(2);
    searcher.constructSubMatchDocs(matchDoc3);
    vector<docid_t> subId3 = seekSubDoc(docid_t(2), &queryExecutor,
            &matchDocAllocator, &searcher, matchDoc3);
    ASSERT_EQ(docid_t(2), matchDoc3.getDocId());
    ASSERT_EQ(docid_t(4), subId3[0]);
    ASSERT_EQ(docid_t(5), subId3[1]);
    ASSERT_EQ(docid_t(6), subId3[2]);
    ASSERT_EQ(docid_t(7), subId3[3]);
    POOL_DELETE_CLASS(mainDocToSubDocIt);
}

TEST_F(SingleLayerSearcherTest, testGetAllSubDocConstruct) {
    initDeletionMapReader("", _subDelReaderPtr);
    initDeletionMapReader("", _delReaderPtr);
    common::Ha3MatchDocAllocator matchDocAllocator(_pool, true);
    string layerMetaStr("0,1000,1000");
    LayerMeta layerMeta = LayerMetasConstructor::createLayerMeta(
            _pool, layerMetaStr);
    FakeIndex fakeIndex;
    QueryExecutorTestHelper::addSubDocAttrMap(fakeIndex, "2,4,8");
    IndexPartitionReaderWrapperPtr indexPartReader =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    QueryExecutorMock queryExecutor("0,1,2");
    const IndexPartitionReaderPtr &readerPtr = indexPartReader->getReader();
    AttributeReaderPtr attrReader = readerPtr->GetAttributeReader(MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME);
    AttributeIteratorBase* mainDocToSubDocIt = attrReader->CreateIterator(_pool);
    DocMapAttrIterator *typedMainDocToSubDocIt =
        dynamic_cast<DocMapAttrIterator*>(mainDocToSubDocIt);
    SingleLayerSearcher searcher(&queryExecutor, &layerMeta, NULL,
                                 _delReaderPtr.get(), &matchDocAllocator, NULL,
                                 typedMainDocToSubDocIt, _subDelReaderPtr.get(), NULL, true);
    queryExecutor.legacySeek(0);
    auto matchDoc1 = matchDocAllocator.allocate(0);
    searcher.constructSubMatchDocs(matchDoc1);
    vector<matchdoc::MatchDoc> subMatchDoc1 = seekSubMatchDoc(docid_t(0), &queryExecutor,
            &matchDocAllocator, &searcher, matchDoc1);
    ASSERT_EQ(docid_t(0), matchDoc1.getDocId());
    ASSERT_EQ(docid_t(0), subMatchDoc1[0].getDocId());
    ASSERT_EQ(docid_t(1), subMatchDoc1[1].getDocId());
    ASSERT_EQ(false, subMatchDoc1[0].isDeleted());
    ASSERT_EQ(false, subMatchDoc1[1].isDeleted());

    queryExecutor.legacySeek(1);
    auto matchDoc2 = matchDocAllocator.allocate(1);
    searcher.constructSubMatchDocs(matchDoc2);
    vector<matchdoc::MatchDoc> subMatchDoc2 = seekSubMatchDoc(docid_t(1), &queryExecutor,
            &matchDocAllocator, &searcher, matchDoc2);
    ASSERT_EQ(docid_t(1), matchDoc2.getDocId());
    ASSERT_EQ(docid_t(2), subMatchDoc2[0].getDocId());
    ASSERT_EQ(docid_t(3), subMatchDoc2[1].getDocId());
    ASSERT_EQ(false, subMatchDoc2[0].isDeleted());
    ASSERT_EQ(false, subMatchDoc2[1].isDeleted());


    queryExecutor.legacySeek(2);
    auto matchDoc3 = matchDocAllocator.allocate(2);
    searcher.constructSubMatchDocs(matchDoc3);
    vector<matchdoc::MatchDoc> subMatchDoc3 = seekSubMatchDoc(docid_t(2), &queryExecutor,
            &matchDocAllocator, &searcher, matchDoc3);
    ASSERT_EQ(docid_t(2), matchDoc3.getDocId());
    ASSERT_EQ(docid_t(4), subMatchDoc3[0].getDocId());
    ASSERT_EQ(docid_t(5), subMatchDoc3[1].getDocId());
    ASSERT_EQ(docid_t(6), subMatchDoc3[2].getDocId());
    ASSERT_EQ(docid_t(7), subMatchDoc3[3].getDocId());
    ASSERT_EQ(false, subMatchDoc3[0].isDeleted());
    ASSERT_EQ(false, subMatchDoc3[1].isDeleted());
    ASSERT_EQ(false, subMatchDoc3[2].isDeleted());
    ASSERT_EQ(false, subMatchDoc3[3].isDeleted());

    POOL_DELETE_CLASS(mainDocToSubDocIt);
}

FilterWrapperPtr SingleLayerSearcherTest::createFilter(
        const std::string &filterStr, common::Ha3MatchDocAllocator *allocator)
{
    if (filterStr.empty()) {
        return FilterWrapperPtr();
    }
    vector<bool> values(50, false);
    StringTokenizer filterVec(filterStr, ",", StringTokenizer::TOKEN_IGNORE_EMPTY
                              | StringTokenizer::TOKEN_TRIM);
    for (size_t i = 0; i < filterVec.getNumTokens(); ++i) {
        values[StringUtil::fromString<docid_t>(filterVec[i].c_str())] = true;
    }
    FakeAttributeExpression<bool> *fakeExpr =
        new FakeAttributeExpression<bool>("filter", values);
    _attrExpr = fakeExpr;
    if (allocator) {
        fakeExpr->setIsSubExpression(true);
        fakeExpr->setSubCurrentRef(allocator->getSubDocRef());
    }
    Filter *filter = POOL_NEW_CLASS(_pool, Filter, _attrExpr);
    FilterWrapperPtr filterWrapper(new FilterWrapper);
    filterWrapper->setFilter(filter);
    return filterWrapper;
}

TEST_F(SingleLayerSearcherTest, testSimpleConstructWithFilter) {
    initDeletionMapReader("", _subDelReaderPtr);
    initDeletionMapReader("", _delReaderPtr);
    common::Ha3MatchDocAllocator matchDocAllocator(_pool, true);
    string layerMetaStr("0,1000,1000");
    LayerMeta layerMeta = LayerMetasConstructor::createLayerMeta(
            _pool, layerMetaStr);
    FakeIndex fakeIndex;
    QueryExecutorTestHelper::addSubDocAttrMap(fakeIndex, "2,4,8");
    IndexPartitionReaderWrapperPtr indexPartReader =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    string filterStr("1,3,5");
    FilterWrapperPtr filterWrapper = createFilter(filterStr, &matchDocAllocator);
    QueryExecutorMock queryExecutor("0,1,2");
    const IndexPartitionReaderPtr &readerPtr = indexPartReader->getReader();
    AttributeReaderPtr attrReader =
        readerPtr->GetAttributeReader(MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME);
    AttributeIteratorBase* mainDocToSubDocIt = attrReader->CreateIterator(_pool);
    DocMapAttrIterator *typedMainDocToSubDocIt =
        dynamic_cast<DocMapAttrIterator*>(mainDocToSubDocIt);
    SingleLayerSearcher searcher(&queryExecutor, &layerMeta, filterWrapper.get(),
                                 _delReaderPtr.get(), &matchDocAllocator, NULL,
                                 typedMainDocToSubDocIt, _subDelReaderPtr.get());
    queryExecutor.legacySeek(0);
    auto matchDoc1 = matchDocAllocator.allocate(0);
    searcher.constructSubMatchDocs(matchDoc1);
    vector<docid_t> subId1 = seekSubDoc(docid_t(0), &queryExecutor,
            &matchDocAllocator, &searcher, matchDoc1);
    ASSERT_EQ(size_t(1), subId1.size());
    ASSERT_EQ(docid_t(0), matchDoc1.getDocId());
    ASSERT_EQ(docid_t(1), subId1[0]);


    queryExecutor.legacySeek(1);
    auto matchDoc2 = matchDocAllocator.allocate(1);
    searcher.constructSubMatchDocs(matchDoc2);
    vector<docid_t> subId2 = seekSubDoc(docid_t(1), &queryExecutor,
            &matchDocAllocator, &searcher, matchDoc2);
    ASSERT_EQ(size_t(1), subId2.size());
    ASSERT_EQ(docid_t(1), matchDoc2.getDocId());
    ASSERT_EQ(docid_t(3), subId2[0]);

    queryExecutor.legacySeek(2);
    auto matchDoc3 = matchDocAllocator.allocate(2);
    searcher.constructSubMatchDocs(matchDoc3);
    vector<docid_t> subId3 = seekSubDoc(docid_t(2), &queryExecutor,
            &matchDocAllocator, &searcher, matchDoc3);
    ASSERT_EQ(size_t(1), subId3.size());
    ASSERT_EQ(docid_t(2), matchDoc3.getDocId());
    ASSERT_EQ(docid_t(5), subId3[0]);

    POOL_DELETE_CLASS(mainDocToSubDocIt);
}


TEST_F(SingleLayerSearcherTest, testGetAllSubDocConstructWithFilter) {
    initDeletionMapReader("0", _subDelReaderPtr);
    initDeletionMapReader("", _delReaderPtr);
    common::Ha3MatchDocAllocator matchDocAllocator(_pool, true);
    string layerMetaStr("0,1000,1000");
    LayerMeta layerMeta = LayerMetasConstructor::createLayerMeta(
            _pool, layerMetaStr);
    FakeIndex fakeIndex;
    QueryExecutorTestHelper::addSubDocAttrMap(fakeIndex, "2,4,8");
    IndexPartitionReaderWrapperPtr indexPartReader =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    string filterStr("1,2,3,5");
    FilterWrapperPtr filterWrapper = createFilter(filterStr, &matchDocAllocator);
    QueryExecutorMock queryExecutor("0,1,2");
    const IndexPartitionReaderPtr &readerPtr = indexPartReader->getReader();
    AttributeReaderPtr attrReader =
        readerPtr->GetAttributeReader(MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME);
    AttributeIteratorBase* mainDocToSubDocIt = attrReader->CreateIterator(_pool);
    DocMapAttrIterator *typedMainDocToSubDocIt =
        dynamic_cast<DocMapAttrIterator*>(mainDocToSubDocIt);
    SingleLayerSearcher searcher(&queryExecutor, &layerMeta, filterWrapper.get(),
                                 _delReaderPtr.get(), &matchDocAllocator, NULL,
                                 typedMainDocToSubDocIt, _subDelReaderPtr.get(), NULL, true);
    queryExecutor.legacySeek(0);
    auto matchDoc1 = matchDocAllocator.allocate(0);
    searcher.constructSubMatchDocs(matchDoc1);
    vector<matchdoc::MatchDoc> subMatchDoc1 = seekSubMatchDoc(docid_t(0), &queryExecutor,
            &matchDocAllocator, &searcher, matchDoc1);
    ASSERT_EQ(size_t(1), subMatchDoc1.size());
    ASSERT_EQ(docid_t(0), matchDoc1.getDocId());
    ASSERT_EQ(docid_t(1), subMatchDoc1[0].getDocId());
    ASSERT_EQ(false, subMatchDoc1[0].isDeleted());

    queryExecutor.legacySeek(1);
    auto matchDoc2 = matchDocAllocator.allocate(1);
    searcher.constructSubMatchDocs(matchDoc2);
    vector<matchdoc::MatchDoc> subMatchDoc2 = seekSubMatchDoc(docid_t(1), &queryExecutor,
            &matchDocAllocator, &searcher, matchDoc2);
    ASSERT_EQ(size_t(2), subMatchDoc2.size());
    ASSERT_EQ(docid_t(1), matchDoc2.getDocId());
    ASSERT_EQ(docid_t(2), subMatchDoc2[0].getDocId());
    ASSERT_EQ(docid_t(3), subMatchDoc2[1].getDocId());
    ASSERT_EQ(false, subMatchDoc2[0].isDeleted());
    ASSERT_EQ(false, subMatchDoc2[1].isDeleted());


    queryExecutor.legacySeek(2);
    auto matchDoc3 = matchDocAllocator.allocate(2);
    searcher.constructSubMatchDocs(matchDoc3);
    vector<matchdoc::MatchDoc> subMatchDoc3 = seekSubMatchDoc(docid_t(2), &queryExecutor,
            &matchDocAllocator, &searcher, matchDoc3);
    ASSERT_EQ(size_t(4), subMatchDoc3.size());
    ASSERT_EQ(docid_t(2), matchDoc3.getDocId());
    ASSERT_EQ(docid_t(4), subMatchDoc3[0].getDocId());
    ASSERT_EQ(docid_t(5), subMatchDoc3[1].getDocId());
    ASSERT_EQ(docid_t(6), subMatchDoc3[2].getDocId());
    ASSERT_EQ(docid_t(7), subMatchDoc3[3].getDocId());
    ASSERT_EQ(true, subMatchDoc3[0].isDeleted());
    ASSERT_EQ(false, subMatchDoc3[1].isDeleted());
    ASSERT_EQ(true, subMatchDoc3[2].isDeleted());
    ASSERT_EQ(true, subMatchDoc3[3].isDeleted());

    POOL_DELETE_CLASS(mainDocToSubDocIt);
}

TEST_F(SingleLayerSearcherTest, testConstructWithFirstSubDocFilter) {
    common::Ha3MatchDocAllocator matchDocAllocator(_pool, true);
    initDeletionMapReader("", _subDelReaderPtr);
    initDeletionMapReader("", _delReaderPtr);
    string layerMetaStr("0,1000,1000");
    LayerMeta layerMeta = LayerMetasConstructor::createLayerMeta(
            _pool, layerMetaStr);
    FakeIndex fakeIndex;
    QueryExecutorTestHelper::addSubDocAttrMap(fakeIndex, "2,4,8");
    IndexPartitionReaderWrapperPtr indexPartReader =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    string filterStr("2");
    FilterWrapperPtr filterWrapper = createFilter(filterStr, &matchDocAllocator);
    QueryExecutorMock queryExecutor("0,1,2");
    const IndexPartitionReaderPtr &readerPtr = indexPartReader->getReader();
    AttributeReaderPtr attrReader = readerPtr->GetAttributeReader(MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME);
    AttributeIteratorBase* mainDocToSubDocIt = attrReader->CreateIterator(_pool);
    DocMapAttrIterator *typedMainDocToSubDocIt =
        dynamic_cast<DocMapAttrIterator*>(mainDocToSubDocIt);
    SingleLayerSearcher searcher(&queryExecutor, &layerMeta, filterWrapper.get(),
                                 _delReaderPtr.get(), &matchDocAllocator, NULL,
                                 typedMainDocToSubDocIt, _subDelReaderPtr.get());
    queryExecutor.legacySeek(0);
    auto matchDoc1 = matchDocAllocator.allocate(0);
    searcher.constructSubMatchDocs(matchDoc1);
    vector<docid_t> subId1 = seekSubDoc(docid_t(0), &queryExecutor,
            &matchDocAllocator, &searcher, matchDoc1);
    ASSERT_EQ(docid_t(0), matchDoc1.getDocId());
    ASSERT_EQ(size_t(0), subId1.size());

    queryExecutor.legacySeek(1);
    auto matchDoc2 = matchDocAllocator.allocate(1);
    searcher.constructSubMatchDocs(matchDoc2);
    vector<docid_t> subId2 = seekSubDoc(docid_t(1), &queryExecutor,
            &matchDocAllocator, &searcher, matchDoc2);
    ASSERT_EQ(docid_t(1), matchDoc2.getDocId());
    ASSERT_EQ(size_t(1), subId2.size());
    ASSERT_EQ(docid_t(2), subId2[0]);

    queryExecutor.legacySeek(2);
    auto matchDoc3 = matchDocAllocator.allocate(2);
    searcher.constructSubMatchDocs(matchDoc3);
    vector<docid_t> subId3 = seekSubDoc(docid_t(2), &queryExecutor,
            &matchDocAllocator, &searcher, matchDoc3);
    ASSERT_EQ(docid_t(2), matchDoc3.getDocId());
    ASSERT_EQ(size_t(0), subId3.size());

    POOL_DELETE_CLASS(mainDocToSubDocIt);
}

TEST_F(SingleLayerSearcherTest, testConstructWithFilterAndJoinFilter) {
    initDeletionMapReader("", _subDelReaderPtr);
    initDeletionMapReader("", _delReaderPtr);
    common::Ha3MatchDocAllocator matchDocAllocator(_pool, true);
    string layerMetaStr("0,1000,1000");
    LayerMeta layerMeta = LayerMetasConstructor::createLayerMeta(
            _pool, layerMetaStr);
    FakeIndex fakeIndex;
    QueryExecutorTestHelper::addSubDocAttrMap(fakeIndex, "2,4,8");
    IndexPartitionReaderWrapperPtr indexPartReader =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    string filterStr("1,3,5,6");
    FilterWrapperPtr filterWrapper = createFilter(filterStr, &matchDocAllocator);
    JoinFilterCreatorForTest joinFilterCreator(_pool, &matchDocAllocator);
    vector<vector<int> > converterDocIdMaps;
    vector<int> docIds(8,-1);
    vector<bool> isSubs(1,true);
    vector<bool> isStrongJoins(1,true);
    docIds[3] = 1;
    converterDocIdMaps.push_back(docIds);
    JoinFilterPtr joinFilter = joinFilterCreator.createJoinFilter(converterDocIdMaps, isSubs, isStrongJoins, true);
    filterWrapper->setJoinFilter(joinFilter.get());
    QueryExecutorMock queryExecutor("0,1,2");
    const IndexPartitionReaderPtr &readerPtr = indexPartReader->getReader();
    AttributeReaderPtr attrReader =
        readerPtr->GetAttributeReader(MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME);
    AttributeIteratorBase* mainDocToSubDocIt = attrReader->CreateIterator(_pool);
    DocMapAttrIterator *typedMainDocToSubDocIt =
        dynamic_cast<DocMapAttrIterator*>(mainDocToSubDocIt);
    SingleLayerSearcher searcher(&queryExecutor, &layerMeta, filterWrapper.get(),
                                 _delReaderPtr.get(), &matchDocAllocator, NULL,
                                 typedMainDocToSubDocIt, _subDelReaderPtr.get());
    queryExecutor.legacySeek(0);
    auto matchDoc1 = matchDocAllocator.allocate(0);
    searcher.constructSubMatchDocs(matchDoc1);
    vector<docid_t> subId1 = seekSubDoc(docid_t(0), &queryExecutor,
            &matchDocAllocator, &searcher, matchDoc1);
    ASSERT_EQ(size_t(0), subId1.size());
    ASSERT_EQ(docid_t(0), matchDoc1.getDocId());

    queryExecutor.legacySeek(1);
    auto matchDoc2 = matchDocAllocator.allocate(1);
    searcher.constructSubMatchDocs(matchDoc2);
    vector<docid_t> subId2 = seekSubDoc(docid_t(1), &queryExecutor,
            &matchDocAllocator, &searcher, matchDoc2);
    ASSERT_EQ(size_t(1), subId2.size());
    ASSERT_EQ(docid_t(1), matchDoc2.getDocId());
    ASSERT_EQ(docid_t(3), subId2[0]);

    queryExecutor.legacySeek(2);
    auto matchDoc3 = matchDocAllocator.allocate(2);
    searcher.constructSubMatchDocs(matchDoc3);
    vector<docid_t> subId3 = seekSubDoc(docid_t(2), &queryExecutor,
            &matchDocAllocator, &searcher, matchDoc3);
    ASSERT_EQ(size_t(0), subId3.size());
    ASSERT_EQ(docid_t(2), matchDoc3.getDocId());

    POOL_DELETE_CLASS(mainDocToSubDocIt);
}

TEST_F(SingleLayerSearcherTest, testGetAllSubDocConstructWithFilterAndJoinFilter) {
    initDeletionMapReader("", _subDelReaderPtr);
    initDeletionMapReader("", _delReaderPtr);
    common::Ha3MatchDocAllocator matchDocAllocator(_pool, true);
    string layerMetaStr("0,1000,1000");
    LayerMeta layerMeta = LayerMetasConstructor::createLayerMeta(
            _pool, layerMetaStr);
    FakeIndex fakeIndex;
    QueryExecutorTestHelper::addSubDocAttrMap(fakeIndex, "2,4,8");
    IndexPartitionReaderWrapperPtr indexPartReader =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    string filterStr("1,3,5,6");
    FilterWrapperPtr filterWrapper = createFilter(filterStr, &matchDocAllocator);
    JoinFilterCreatorForTest joinFilterCreator(_pool, &matchDocAllocator);
    vector<vector<int> > converterDocIdMaps;
    vector<int> docIds(8,-1);
    vector<bool> isSubs(1,true);
    vector<bool> isStrongJoins(1,true);
    docIds[3] = 1;
    converterDocIdMaps.push_back(docIds);
    JoinFilterPtr joinFilter = joinFilterCreator.createJoinFilter(converterDocIdMaps, isSubs, isStrongJoins, true);
    filterWrapper->setJoinFilter(joinFilter.get());
    QueryExecutorMock queryExecutor("0,1,2");
    const IndexPartitionReaderPtr &readerPtr = indexPartReader->getReader();
    AttributeReaderPtr attrReader =
        readerPtr->GetAttributeReader(MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME);
    AttributeIteratorBase* mainDocToSubDocIt = attrReader->CreateIterator(_pool);
    DocMapAttrIterator *typedMainDocToSubDocIt =
        dynamic_cast<DocMapAttrIterator*>(mainDocToSubDocIt);
    SingleLayerSearcher searcher(&queryExecutor, &layerMeta, filterWrapper.get(),
                                 _delReaderPtr.get(), &matchDocAllocator, NULL,
                                 typedMainDocToSubDocIt, _subDelReaderPtr.get(), NULL, true);
    queryExecutor.legacySeek(0);
    auto matchDoc1 = matchDocAllocator.allocate(0);
    searcher.constructSubMatchDocs(matchDoc1);
    vector<matchdoc::MatchDoc> subMatchDoc1 = seekSubMatchDoc(docid_t(0), &queryExecutor,
            &matchDocAllocator, &searcher, matchDoc1);
    ASSERT_EQ(size_t(2), subMatchDoc1.size());
    ASSERT_EQ(docid_t(0), matchDoc1.getDocId());
    ASSERT_EQ(docid_t(0), subMatchDoc1[0].getDocId());
    ASSERT_EQ(docid_t(1), subMatchDoc1[1].getDocId());
    ASSERT_EQ(true, subMatchDoc1[0].isDeleted());
    ASSERT_EQ(true, subMatchDoc1[1].isDeleted());

    queryExecutor.legacySeek(1);
    auto matchDoc2 = matchDocAllocator.allocate(1);
    searcher.constructSubMatchDocs(matchDoc2);
    vector<matchdoc::MatchDoc> subMatchDoc2 = seekSubMatchDoc(docid_t(1), &queryExecutor,
            &matchDocAllocator, &searcher, matchDoc2);
    ASSERT_EQ(size_t(2), subMatchDoc2.size());
    ASSERT_EQ(docid_t(1), matchDoc2.getDocId());
    ASSERT_EQ(docid_t(2), subMatchDoc2[0].getDocId());
    ASSERT_EQ(docid_t(3), subMatchDoc2[1].getDocId());
    ASSERT_EQ(true, subMatchDoc2[0].isDeleted());
    ASSERT_EQ(false, subMatchDoc2[1].isDeleted());


    queryExecutor.legacySeek(2);
    auto matchDoc3 = matchDocAllocator.allocate(2);
    searcher.constructSubMatchDocs(matchDoc3);
    vector<matchdoc::MatchDoc> subMatchDoc3 = seekSubMatchDoc(docid_t(2), &queryExecutor,
            &matchDocAllocator, &searcher, matchDoc3);
    ASSERT_EQ(size_t(4), subMatchDoc3.size());
    ASSERT_EQ(docid_t(2), matchDoc3.getDocId());
    ASSERT_EQ(docid_t(4), subMatchDoc3[0].getDocId());
    ASSERT_EQ(docid_t(5), subMatchDoc3[1].getDocId());
    ASSERT_EQ(docid_t(6), subMatchDoc3[2].getDocId());
    ASSERT_EQ(docid_t(7), subMatchDoc3[3].getDocId());
    ASSERT_EQ(true, subMatchDoc3[0].isDeleted());
    ASSERT_EQ(true, subMatchDoc3[1].isDeleted());
    ASSERT_EQ(true, subMatchDoc3[2].isDeleted());
    ASSERT_EQ(true, subMatchDoc3[3].isDeleted());

    POOL_DELETE_CLASS(mainDocToSubDocIt);
}

TEST_F(SingleLayerSearcherTest, testConstructWithFirstSubDocDeleted) {
    common::Ha3MatchDocAllocator matchDocAllocator(_pool, true);

    initDeletionMapReader("0,1", _subDelReaderPtr);
    initDeletionMapReader("", _delReaderPtr);

    string layerMetaStr("0,1000,1000");
    LayerMeta layerMeta = LayerMetasConstructor::createLayerMeta(
            _pool, layerMetaStr);
    FakeIndex fakeIndex;
    QueryExecutorTestHelper::addSubDocAttrMap(fakeIndex, "2,4,8");
    IndexPartitionReaderWrapperPtr indexPartReader =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    QueryExecutorMock queryExecutor("0,1,2");
    const IndexPartitionReaderPtr &readerPtr = indexPartReader->getReader();
    AttributeReaderPtr attrReader = readerPtr->GetAttributeReader(MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME);
    AttributeIteratorBase* mainDocToSubDocIt = attrReader->CreateIterator(_pool);
    DocMapAttrIterator *typedMainDocToSubDocIt =
        dynamic_cast<DocMapAttrIterator*>(mainDocToSubDocIt);
    SingleLayerSearcher searcher(&queryExecutor, &layerMeta, NULL,
                                 _delReaderPtr.get(), &matchDocAllocator, NULL,
                                 typedMainDocToSubDocIt, _subDelReaderPtr.get());
    queryExecutor.legacySeek(0);
    auto matchDoc1 = matchDocAllocator.allocate(0);
    searcher.constructSubMatchDocs(matchDoc1);
    vector<docid_t> subId1 = seekSubDoc(docid_t(0), &queryExecutor,
            &matchDocAllocator, &searcher, matchDoc1);
    ASSERT_EQ(docid_t(0), matchDoc1.getDocId());
    ASSERT_EQ(size_t(0), subId1.size());

    POOL_DELETE_CLASS(mainDocToSubDocIt);
}

TEST_F(SingleLayerSearcherTest, testIgnoreDelete) {
    common::Ha3MatchDocAllocator matchDocAllocator(_pool, true);

    string layerMetaStr("0,1000,1000");
    LayerMeta layerMeta = LayerMetasConstructor::createLayerMeta(
            _pool, layerMetaStr);
    FakeIndex fakeIndex;
    QueryExecutorTestHelper::addSubDocAttrMap(fakeIndex, "2,4,8");
    IndexPartitionReaderWrapperPtr indexPartReader =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    QueryExecutorMock queryExecutor("0,1,2");
    const IndexPartitionReaderPtr &readerPtr = indexPartReader->getReader();
    AttributeReaderPtr attrReader = readerPtr->GetAttributeReader(MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME);
    AttributeIteratorBase* mainDocToSubDocIt = attrReader->CreateIterator(_pool);
    DocMapAttrIterator *typedMainDocToSubDocIt =
        dynamic_cast<DocMapAttrIterator*>(mainDocToSubDocIt);
    SingleLayerSearcher searcher(&queryExecutor, &layerMeta, NULL,
                                 nullptr, &matchDocAllocator, NULL,
                                 typedMainDocToSubDocIt, nullptr);

    matchdoc::MatchDoc matchDoc1;
    ASSERT_EQ(IE_NAMESPACE(common)::ErrorCode::OK, searcher.seek(false, matchDoc1));
    vector<docid_t> subId1 = seekSubDoc(docid_t(0), &queryExecutor,
            &matchDocAllocator, &searcher, matchDoc1);
    ASSERT_EQ(docid_t(0), matchDoc1.getDocId());
    ASSERT_EQ(size_t(2), subId1.size());

    POOL_DELETE_CLASS(mainDocToSubDocIt);
}

TEST_F(SingleLayerSearcherTest, testConstructWithDeletionMap) {
    common::Ha3MatchDocAllocator matchDocAllocator(_pool, true);

    initDeletionMapReader("1,2,5", _subDelReaderPtr);
    initDeletionMapReader("", _delReaderPtr);

    string layerMetaStr("0,1000,1000");
    LayerMeta layerMeta = LayerMetasConstructor::createLayerMeta(
            _pool, layerMetaStr);
    FakeIndex fakeIndex;
    QueryExecutorTestHelper::addSubDocAttrMap(fakeIndex, "2,4,8");
    IndexPartitionReaderWrapperPtr indexPartReader =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);

    QueryExecutorMock queryExecutor("0,1,2");
    const IndexPartitionReaderPtr &readerPtr = indexPartReader->getReader();
    AttributeReaderPtr attrReader = readerPtr->GetAttributeReader(MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME);
    AttributeIteratorBase* mainDocToSubDocIt = attrReader->CreateIterator(_pool);
    DocMapAttrIterator *typedMainDocToSubDocIt =
        dynamic_cast<DocMapAttrIterator*>(mainDocToSubDocIt);

    SingleLayerSearcher searcher(&queryExecutor, &layerMeta, NULL,
                                 _delReaderPtr.get(), &matchDocAllocator, NULL,
                                 typedMainDocToSubDocIt, _subDelReaderPtr.get());
    queryExecutor.legacySeek(0);
    auto matchDoc1 = matchDocAllocator.allocate(0);
    searcher.constructSubMatchDocs(matchDoc1);
    vector<docid_t> subId1 = seekSubDoc(docid_t(0), &queryExecutor,
            &matchDocAllocator, &searcher, matchDoc1);
    ASSERT_EQ(docid_t(0), matchDoc1.getDocId());
    ASSERT_EQ(docid_t(0), subId1[0]);

    queryExecutor.legacySeek(1);
    auto matchDoc2 = matchDocAllocator.allocate(1);
    searcher.constructSubMatchDocs(matchDoc2);
    vector<docid_t> subId2 = seekSubDoc(docid_t(1), &queryExecutor,
            &matchDocAllocator, &searcher, matchDoc2);
    ASSERT_EQ(docid_t(1), matchDoc2.getDocId());
    ASSERT_EQ(docid_t(3), subId2[0]);

    queryExecutor.legacySeek(2);
    auto matchDoc3 = matchDocAllocator.allocate(2);
    searcher.constructSubMatchDocs(matchDoc3);
    vector<docid_t> subId3 = seekSubDoc(docid_t(2), &queryExecutor,
            &matchDocAllocator, &searcher, matchDoc3);
    ASSERT_EQ(docid_t(2), matchDoc3.getDocId());
    ASSERT_EQ(docid_t(4), subId3[0]);
    ASSERT_EQ(docid_t(6), subId3[1]);
    ASSERT_EQ(docid_t(7), subId3[2]);
    POOL_DELETE_CLASS(mainDocToSubDocIt);
}

void SingleLayerSearcherTest::internalTestSeek(const string &docsInIndex,
        const string &layerMetaStr, const string &filterStr, const string &delDocIds,
        const string &expectResult, int32_t seekedCount, int32_t leftQuota)
{
    HA3_LOG(DEBUG, "Begin Test!");

    LayerMeta layerMeta = LayerMetasConstructor::createLayerMeta(
            _pool, layerMetaStr);
    common::Ha3MatchDocAllocator matchDocAllocator(_pool);

    QueryExecutorMock queryExecutor(docsInIndex);

    initDeletionMapReader(delDocIds, _delReaderPtr);
    unique_ptr<Filter> filter;
    unique_ptr<FakeAttributeExpression<bool> > attrExpr;
    vector<bool> values(50, false);

    if (!filterStr.empty()) {
        StringTokenizer st(filterStr, ",", StringTokenizer::TOKEN_IGNORE_EMPTY
                           | StringTokenizer::TOKEN_TRIM);
        for (size_t i = 0; i < st.getNumTokens(); ++i) {
            values[StringUtil::fromString<docid_t>(st[i].c_str())] = true;
        }
        attrExpr.reset(new FakeAttributeExpression<bool>("filter", &values));
        filter.reset(new Filter(attrExpr.get()));
    }

    FilterWrapper filterWrapper;
    filterWrapper.setFilter(filter.get());
    SingleLayerSearcher searcher(&queryExecutor, &layerMeta, &filterWrapper,
                                 _delReaderPtr.get(), &matchDocAllocator,
                                 NULL, NULL, _subDelReaderPtr.get());
    vector<docid_t> result = SearcherTestHelper::covertToResultDocIds(expectResult);

    for (size_t i = 0; i < result.size(); ++i) {
        matchdoc::MatchDoc matchDoc;
        ASSERT_EQ(IE_NAMESPACE(common)::ErrorCode::OK, searcher.seek(false, matchDoc));
        (void)matchDoc;
        HA3_LOG(INFO, "expect docid: [%d]", result[i]);
        assert(matchdoc::INVALID_MATCHDOC != matchDoc);
        HA3_LOG(INFO, "i : [%ld]", i);
        assert( result[i] == matchDoc.getDocId());
    }
    matchdoc::MatchDoc matchDoc;
    ASSERT_EQ(IE_NAMESPACE(common)::ErrorCode::OK, searcher.seek(false, matchDoc));
    assert(matchdoc::INVALID_MATCHDOC == matchDoc);
    HA3_LOG(INFO, "layerMetaStr : [%s]", layerMetaStr.c_str());
    if (seekedCount >= 0) {
        assert((uint32_t)seekedCount == searcher.getSeekedCount());
    }
    if (leftQuota >= 0) {
        assert((uint32_t)leftQuota == searcher.getLeftQuotaInTheEnd());
    }
}

void SingleLayerSearcherTest::initDeletionMapReader(
        const std::string &delDocIds,
        DeletionMapReaderPtr& delReaderPtr) {
    delReaderPtr.reset(new DeletionMapReader(1000));
    StringTokenizer st(delDocIds, ",", StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    for (size_t i = 0;i < st.getNumTokens(); ++i) {
        delReaderPtr->Delete(StringUtil::fromString<docid_t>(st[i].c_str()));
    }

}

void SingleLayerSearcherTest::internalTestSeekAndJoin(const string &docsInIndex,
        const string &layerMetaStr,
        const string &filterStr,
        const string &delDocIds,
        const string &storeIdsStr,
        const HashJoinInfoPtr &hashJoinInfo,
        const string &expectResult,
        const string &expectAuxDocIds,
        int32_t seekedCount,
        int32_t leftQuota) {
    HA3_LOG(DEBUG, "Begin Test!");

    LayerMeta layerMeta = LayerMetasConstructor::createLayerMeta(
            _pool, layerMetaStr);
    common::Ha3MatchDocAllocator matchDocAllocator(_pool);
    auto *auxDocidRef = matchDocAllocator.declare<DocIdWrapper>(
            BUILD_IN_JOIN_DOCID_REF_PREIX + hashJoinInfo->getJoinFieldName());
    matchDocAllocator.extend();
    hashJoinInfo->setAuxDocidRef(auxDocidRef);

    QueryExecutorMock queryExecutor(docsInIndex);

    initDeletionMapReader(delDocIds, _delReaderPtr);
    unique_ptr<Filter> filter;
    unique_ptr<FakeAttributeExpression<bool> > attrExpr;
    vector<bool> values(50, false);

    if (!filterStr.empty()) {
        StringTokenizer st(filterStr, ",", StringTokenizer::TOKEN_IGNORE_EMPTY
                           | StringTokenizer::TOKEN_TRIM);
        for (size_t i = 0; i < st.getNumTokens(); ++i) {
            values[StringUtil::fromString<docid_t>(st[i].c_str())] = true;
        }
        attrExpr.reset(new FakeAttributeExpression<bool>("filter", &values));
        filter.reset(new Filter(attrExpr.get()));
    }

    FilterWrapper filterWrapper;
    filterWrapper.setFilter(filter.get());

    vector<string> storeIdsStrVec;
    autil::StringUtil::split(storeIdsStrVec, storeIdsStr, ",");
    vector<int32_t> storeIds;
    for (auto &s : storeIdsStrVec) {
	storeIds.push_back(autil::StringUtil::fromString<int32_t>(s));
    }
    unique_ptr<FakeAttributeExpression<int32_t>> joinAttrExpr{new FakeAttributeExpression<int32_t>(_joinFieldName, storeIds)};

    SingleLayerSearcher searcher(&queryExecutor, &layerMeta, &filterWrapper,
            _delReaderPtr.get(), &matchDocAllocator, NULL, NULL, _subDelReaderPtr.get(),
            nullptr, false, hashJoinInfo.get(), joinAttrExpr.get());
    vector<docid_t> result = SearcherTestHelper::covertToResultDocIds(expectResult);

    vector<matchdoc::MatchDoc> matchDocs;
    while (true) {
        matchdoc::MatchDoc matchDoc;
        ASSERT_EQ(IE_NAMESPACE(common)::ErrorCode::OK,
	    searcher.seekAndJoin<int32_t>(false, matchDoc));
	if (matchDoc == matchdoc::INVALID_MATCHDOC) {
	    break;
	}
	matchDocs.push_back(matchDoc);
    }
    vector<int32_t> gotDocIdVec;
    vector<int32_t> gotAuxDocIdVec;
    for (auto &doc : matchDocs) {
	gotDocIdVec.push_back(doc.getDocId());
	gotAuxDocIdVec.push_back(auxDocidRef->get(doc));
    }
    string gotDocIds = autil::StringUtil::toString(gotDocIdVec, ",");
    string gotAuxDocIds = autil::StringUtil::toString(gotAuxDocIdVec, ",");
    ASSERT_EQ(expectResult, gotDocIds);
    ASSERT_EQ(expectAuxDocIds, gotAuxDocIds);

    if (seekedCount >= 0) {
        ASSERT_EQ((uint32_t)seekedCount, searcher.getSeekedCount());
    }
    if (leftQuota >= 0) {
        ASSERT_EQ((uint32_t)leftQuota, searcher.getLeftQuotaInTheEnd());
    }
}

TEST_F(SingleLayerSearcherTest, testSeekWithMatchValues) {
    HA3_LOG(DEBUG, "Begin Test!");
    FakeIndex fakeIndex;
    fakeIndex.indexes["default"] = "a:1^101;2^102;3^103;4^104\n"
                                   "b:1^201;3^203;4^204\n"
                                   "c:2^302;3^303;5^305\n";
    string queryStr = "a OR b OR c";
    string metaInfoStr = "default:a;default:b;default:c";
    string matchValueStr = "101,201,0;102,0,302;103,203,303;104,204,0;0,0,305";

    auto indexPartReaderPtr = FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    indexPartReaderPtr->setTopK(1);
    QueryPtr query;
    query.reset(SearcherTestHelper::createQuery(queryStr));
    MatchDataManager manager;
    QueryExecutor * queryExecutor = nullptr;
    QueryExecutorCreator creator(&manager, indexPartReaderPtr.get(), _pool);
    query->accept(&creator);
    queryExecutor = creator.stealQuery();

    manager.setQueryCount(1);

    common::Ha3MatchDocAllocator matchDocAllocator(_pool);
    auto matchValueRef = manager.requireMatchValues(&matchDocAllocator,
            MATCH_VALUE_REF, SUB_DOC_DISPLAY_NO, _pool);
    ASSERT_TRUE(matchValueRef != NULL);
    initDeletionMapReader("", _delReaderPtr);
    string layerMetaStr("0,1000,1000");
    LayerMeta layerMeta = LayerMetasConstructor::createLayerMeta(
            _pool, layerMetaStr);
    SingleLayerSearcher searcher(queryExecutor, &layerMeta, NULL,
                                 _delReaderPtr.get(), &matchDocAllocator,
                                 NULL, NULL, _subDelReaderPtr.get(), &manager);
    string expectResult("1,2,3,4,5");
    vector<docid_t> result = SearcherTestHelper::covertToResultDocIds(expectResult);

    vector<vector<matchvalue_t> > matchValuesResult =
        SearcherTestHelper::convertToMatchValues(matchValueStr);
    for (size_t i = 0; i < result.size(); ++i) {
        matchdoc::MatchDoc matchDoc;
        ASSERT_EQ(IE_NAMESPACE(common)::ErrorCode::OK, searcher.seek(false, matchDoc));
        ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != matchDoc);
        ASSERT_EQ(result[i],  matchDoc.getDocId());
        auto &matchValue = matchValueRef->getReference(matchDoc);
        ASSERT_EQ((uint32_t)3u, matchValue.getNumTerms());
        for (size_t j = 0; j < matchValue.getNumTerms(); j++) {
            ASSERT_EQ(matchValuesResult[i][j].GetUint16(),
                      matchValue.getTermMatchValue(j).GetUint16());
        }
    }

    POOL_DELETE_CLASS(queryExecutor);
}

TEST_F(SingleLayerSearcherTest, testSeekAndJoinWithMatchValues) {
    HA3_LOG(DEBUG, "Begin Test!");
    FakeIndex fakeIndex;
    fakeIndex.indexes["default"] = "a:1^101;2^102;3^103;4^104\n"
                                   "b:1^201;3^203;4^204\n"
                                   "c:2^302;3^303;5^305\n";
    string queryStr = "a OR b OR c";
    string matchValueStr = "101,201,0;101,201,0;102,0,302;103,203,303;103,203,303;104,204,0;0,0,305";

    string layerMetaStr("1,7,UNLIMITED");
    LayerMeta layerMeta = LayerMetasConstructor::createLayerMeta(
            _pool, layerMetaStr);
    string storeIds("-1,1,2,3");
    string expectAuxDocIds("0,1,2,3,4,-1,-1");
    std::unordered_map<size_t, std::vector<int32_t>> hashJoinMap{
        { 1, { 0, 1 } },
	{ 2, { 2 } },
	{ 3, { 3, 4 } },
	{ 4, { 5 } }
    };
    HashJoinInfoPtr hashJoinInfo = buildHashJoinInfo(hashJoinMap);
    common::Ha3MatchDocAllocator matchDocAllocator(_pool);
    auto *auxDocidRef = matchDocAllocator.declare<DocIdWrapper>(
            BUILD_IN_JOIN_DOCID_REF_PREIX + hashJoinInfo->getJoinFieldName());
    matchDocAllocator.extend();
    hashJoinInfo->setAuxDocidRef(auxDocidRef);
    vector<string> storeIdsStrVec;
    autil::StringUtil::split(storeIdsStrVec, storeIds, ",");
    vector<int32_t> storeIdVec;
    for (auto &s : storeIdsStrVec) {
	storeIdVec.push_back(autil::StringUtil::fromString<int32_t>(s));
    }
    unique_ptr<FakeAttributeExpression<int32_t>> joinAttrExpr{new FakeAttributeExpression<int32_t>(_joinFieldName, storeIdVec)};


    auto indexPartReaderPtr = FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    indexPartReaderPtr->setTopK(1);
    QueryPtr query;
    query.reset(SearcherTestHelper::createQuery(queryStr));
    MatchDataManager manager;
    QueryExecutor * queryExecutor = nullptr;
    QueryExecutorCreator creator(&manager, indexPartReaderPtr.get(), _pool);
    query->accept(&creator);
    queryExecutor = creator.stealQuery();

    manager.setQueryCount(1);

    auto matchValueRef = manager.requireMatchValues(&matchDocAllocator,
            MATCH_VALUE_REF, SUB_DOC_DISPLAY_NO, _pool);
    ASSERT_TRUE(matchValueRef != NULL);
    initDeletionMapReader("", _delReaderPtr);
    SingleLayerSearcher searcher(queryExecutor, &layerMeta, NULL,
                                 _delReaderPtr.get(), &matchDocAllocator,
                                 NULL, NULL, _subDelReaderPtr.get(), &manager,
                                 false, hashJoinInfo.get(), joinAttrExpr.get());
    string expectResult("1,1,2,3,3,4,5");
    vector<docid_t> result = SearcherTestHelper::covertToResultDocIds(expectResult);

    vector<vector<matchvalue_t> > matchValuesResult =
        SearcherTestHelper::convertToMatchValues(matchValueStr);
    for (size_t i = 0; i < result.size(); ++i) {
        matchdoc::MatchDoc matchDoc;
        ASSERT_EQ(IE_NAMESPACE(common)::ErrorCode::OK, searcher.seekAndJoin<int32_t>(false, matchDoc));
        ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != matchDoc);
        ASSERT_EQ(result[i],  matchDoc.getDocId());
        auto &matchValue = matchValueRef->getReference(matchDoc);
        ASSERT_EQ((uint32_t)3u, matchValue.getNumTerms());
        for (size_t j = 0; j < matchValue.getNumTerms(); j++) {
            ASSERT_EQ(matchValuesResult[i][j].GetUint16(),
                      matchValue.getTermMatchValue(j).GetUint16());
        }
    }

    POOL_DELETE_CLASS(queryExecutor);
}

TEST_F(SingleLayerSearcherTest, testSeekAndJoin) {
    HA3_LOG(DEBUG, "Begin Test!");

    string docsInIndex("1,2,3,4,5,7");
    string layerMeta("1,2,UNLIMITED;3,7,UNLIMITED");
    string filterStr("1,2,3,5");
    string storeIds("-1,1,2,3");
    string expectResult("1,1,2,3,3,5");
    string expectAuxDocIds("0,1,2,3,4,-1");
    std::unordered_map<size_t, std::vector<int32_t>> hashJoinMap{
        { 1, { 0, 1 } },
	{ 2, { 2 } },
	{ 3, { 3, 4 } },
	{ 4, { 5 } }
    };
    HashJoinInfoPtr hashJoinInfo = buildHashJoinInfo(hashJoinMap);
    internalTestSeekAndJoin(docsInIndex, layerMeta, filterStr, "", storeIds,
            hashJoinInfo, expectResult, expectAuxDocIds, 7, -1);
}

TEST_F(SingleLayerSearcherTest, testDoJoin) {
    HA3_LOG(DEBUG, "Begin Test!");

    matchdoc::MatchDocAllocator matchDocAllocator(_pool);
    auto *testFieldRef = matchDocAllocator.declare<string>("test_field");
    auto *auxDocidRef = matchDocAllocator.declare<DocIdWrapper>(
        BUILD_IN_JOIN_DOCID_REF_PREIX + _joinFieldName);
    vector<int32_t> storeIds{-1, 0};
    unique_ptr<FakeAttributeExpression<int32_t>> joinAttrExpr{
        new FakeAttributeExpression<int32_t>(_joinFieldName, &storeIds)
    };

    HashJoinInfoPtr hashJoinInfo =
        buildHashJoinInfo(HashJoinInfo::HashJoinMap{});
    hashJoinInfo->setAuxDocidRef(auxDocidRef);

    matchdoc::MatchDoc matchDoc = matchDocAllocator.allocate(1);
    testFieldRef->set(matchDoc, "hello");
    ASSERT_EQ(UNINITIALIZED_DOCID, auxDocidRef->get(matchDoc));

    {
        std::list<matchdoc::MatchDoc> matchDocs;
        SingleLayerSearcher::doJoin<int32_t>(&matchDocAllocator,
            matchDoc,
            matchDocs,
            hashJoinInfo.get(),
            joinAttrExpr.get());
        ASSERT_TRUE(matchDocs.empty());
        ASSERT_EQ(INVALID_DOCID, auxDocidRef->get(matchDoc));
    }
    {
	hashJoinInfo->getHashJoinMap() = {
	    {1, {1,2}}
	};
        std::list<matchdoc::MatchDoc> matchDocs;
        SingleLayerSearcher::doJoin<int32_t>(&matchDocAllocator,
            matchDoc,
            matchDocs,
            hashJoinInfo.get(),
            joinAttrExpr.get());
        ASSERT_TRUE(matchDocs.empty());
	ASSERT_EQ(INVALID_DOCID, auxDocidRef->get(matchDoc));
    }
    {
	hashJoinInfo->getHashJoinMap() = {
	    {0, {1}},
	    {1, {1, 2}}
	};
        std::list<matchdoc::MatchDoc> matchDocs;
        SingleLayerSearcher::doJoin<int32_t>(&matchDocAllocator,
            matchDoc,
            matchDocs,
            hashJoinInfo.get(),
            joinAttrExpr.get());
        ASSERT_TRUE(matchDocs.empty());
        ASSERT_EQ((docid_t)1, auxDocidRef->get(matchDoc));
    }
    {
	hashJoinInfo->getHashJoinMap() = {
	    {0, {1, 2}},
	    {1, {3, 4}}
	};
        std::list<matchdoc::MatchDoc> matchDocs;
        SingleLayerSearcher::doJoin<int32_t>(&matchDocAllocator,
            matchDoc,
            matchDocs,
            hashJoinInfo.get(),
            joinAttrExpr.get());
        ASSERT_EQ(1u, matchDocs.size());
	ASSERT_EQ((docid_t)1, auxDocidRef->get(matchDoc));
	ASSERT_EQ((docid_t)2, auxDocidRef->get(matchDocs.front()));
        ASSERT_EQ(string("hello"), testFieldRef->get(matchDocs.front()));
    }
    {
        hashJoinInfo->getHashJoinMap() = {
	    { 0, { 1, 2, 3 } },
	    { 1, { 3, 4 } }
	};
        std::list<matchdoc::MatchDoc> matchDocs;
        SingleLayerSearcher::doJoin<int32_t>(&matchDocAllocator,
            matchDoc,
            matchDocs,
            hashJoinInfo.get(),
            joinAttrExpr.get());
        ASSERT_EQ(2u, matchDocs.size());
	ASSERT_EQ((docid_t)1, auxDocidRef->get(matchDoc));
	auto it = matchDocs.begin();
	ASSERT_EQ((docid_t)2, auxDocidRef->get(*it));
	ASSERT_EQ(string("hello"), testFieldRef->get(*it));
	it++;
	ASSERT_EQ((docid_t)3, auxDocidRef->get(*it));
	ASSERT_EQ(string("hello"), testFieldRef->get(*it));
    }
}

TEST_F(SingleLayerSearcherTest, testDoJoinMultiChar) {
    HA3_LOG(DEBUG, "Begin Test!");

    matchdoc::MatchDocAllocator matchDocAllocator(_pool);
    auto *testFieldRef = matchDocAllocator.declare<string>("test_field");
    auto *auxDocidRef = matchDocAllocator.declare<DocIdWrapper>(
        BUILD_IN_JOIN_DOCID_REF_PREIX + _joinFieldName);
    vector<MultiChar> storeIds{createMultiChar(""), createMultiChar("0")};
    unique_ptr<FakeAttributeExpression<MultiChar>> joinAttrExpr{
        new FakeAttributeExpression<MultiChar>(_joinFieldName, &storeIds)
    };

    HashJoinInfoPtr hashJoinInfo{ new HashJoinInfo(
        _auxTableName, _joinFieldName) };
    hashJoinInfo->setAuxDocidRef(auxDocidRef);

    matchdoc::MatchDoc matchDoc = matchDocAllocator.allocate(1);
    testFieldRef->set(matchDoc, "hello");
    ASSERT_EQ(UNINITIALIZED_DOCID, auxDocidRef->get(matchDoc));

    {
        std::list<matchdoc::MatchDoc> matchDocs;
        SingleLayerSearcher::doJoin<MultiChar>(&matchDocAllocator,
            matchDoc,
            matchDocs,
            hashJoinInfo.get(),
            joinAttrExpr.get());
        ASSERT_TRUE(matchDocs.empty());
        ASSERT_EQ(INVALID_DOCID, auxDocidRef->get(matchDoc));
    }
    {
	hashJoinInfo->getHashJoinMap() = {
	    {TableUtil::calculateHashValue<MultiChar>(createMultiChar("1")), {1,2}}
	};
        std::list<matchdoc::MatchDoc> matchDocs;
        SingleLayerSearcher::doJoin<MultiChar>(&matchDocAllocator,
            matchDoc,
            matchDocs,
            hashJoinInfo.get(),
            joinAttrExpr.get());
        ASSERT_TRUE(matchDocs.empty());
	ASSERT_EQ(INVALID_DOCID, auxDocidRef->get(matchDoc));
    }
    {
	hashJoinInfo->getHashJoinMap() = {
	    {TableUtil::calculateHashValue<MultiChar>(createMultiChar("0")), {1}},
	    {TableUtil::calculateHashValue<MultiChar>(createMultiChar("1")), {1, 2}}
	};
        std::list<matchdoc::MatchDoc> matchDocs;
        SingleLayerSearcher::doJoin<MultiChar>(&matchDocAllocator,
            matchDoc,
            matchDocs,
            hashJoinInfo.get(),
            joinAttrExpr.get());
        ASSERT_TRUE(matchDocs.empty());
        ASSERT_EQ((docid_t)1, auxDocidRef->get(matchDoc));
    }
    {
	hashJoinInfo->getHashJoinMap() = {
	    {TableUtil::calculateHashValue<MultiChar>(createMultiChar("0")), {1, 2}},
	    {TableUtil::calculateHashValue<MultiChar>(createMultiChar("1")), {3, 4}}
	};
        std::list<matchdoc::MatchDoc> matchDocs;
        SingleLayerSearcher::doJoin<MultiChar>(&matchDocAllocator,
            matchDoc,
            matchDocs,
            hashJoinInfo.get(),
            joinAttrExpr.get());
        ASSERT_EQ(1u, matchDocs.size());
	ASSERT_EQ((docid_t)1, auxDocidRef->get(matchDoc));
	ASSERT_EQ((docid_t)2, auxDocidRef->get(matchDocs.front()));
        ASSERT_EQ(string("hello"), testFieldRef->get(matchDocs.front()));
    }
    {
        hashJoinInfo->getHashJoinMap() = {
	    { TableUtil::calculateHashValue<MultiChar>(createMultiChar("0")), { 1, 2, 3 } },
	    { TableUtil::calculateHashValue<MultiChar>(createMultiChar("1")), { 3, 4 } }
	};
        std::list<matchdoc::MatchDoc> matchDocs;
        SingleLayerSearcher::doJoin<MultiChar>(&matchDocAllocator,
            matchDoc,
            matchDocs,
            hashJoinInfo.get(),
            joinAttrExpr.get());
        ASSERT_EQ(2u, matchDocs.size());
	ASSERT_EQ((docid_t)1, auxDocidRef->get(matchDoc));
	auto it = matchDocs.begin();
	ASSERT_EQ((docid_t)2, auxDocidRef->get(*it));
	ASSERT_EQ(string("hello"), testFieldRef->get(*it));
	it++;
	ASSERT_EQ((docid_t)3, auxDocidRef->get(*it));
	ASSERT_EQ(string("hello"), testFieldRef->get(*it));
    }
}

TEST_F(SingleLayerSearcherTest, testDoJoinString) {
    HA3_LOG(DEBUG, "Begin Test!");

    matchdoc::MatchDocAllocator matchDocAllocator(_pool);
    auto *testFieldRef = matchDocAllocator.declare<string>("test_field");
    auto *auxDocidRef = matchDocAllocator.declare<DocIdWrapper>(
        BUILD_IN_JOIN_DOCID_REF_PREIX + _joinFieldName);
    vector<string> storeIds{"", "0"};
    unique_ptr<FakeAttributeExpression<string>> joinAttrExpr{
        new FakeAttributeExpression<string>(_joinFieldName, &storeIds)
    };

    HashJoinInfoPtr hashJoinInfo{ new HashJoinInfo(
        _auxTableName, _joinFieldName) };
    hashJoinInfo->setAuxDocidRef(auxDocidRef);

    matchdoc::MatchDoc matchDoc = matchDocAllocator.allocate(1);
    testFieldRef->set(matchDoc, "hello");
    ASSERT_EQ(UNINITIALIZED_DOCID, auxDocidRef->get(matchDoc));

    {
        std::list<matchdoc::MatchDoc> matchDocs;
        SingleLayerSearcher::doJoin<string>(&matchDocAllocator,
            matchDoc,
            matchDocs,
            hashJoinInfo.get(),
            joinAttrExpr.get());
        ASSERT_TRUE(matchDocs.empty());
        ASSERT_EQ(INVALID_DOCID, auxDocidRef->get(matchDoc));
    }
    {
	hashJoinInfo->getHashJoinMap() = {
	    {TableUtil::calculateHashValue<string>("1"), {1,2}}
	};
        std::list<matchdoc::MatchDoc> matchDocs;
        SingleLayerSearcher::doJoin<string>(&matchDocAllocator,
            matchDoc,
            matchDocs,
            hashJoinInfo.get(),
            joinAttrExpr.get());
        ASSERT_TRUE(matchDocs.empty());
	ASSERT_EQ(INVALID_DOCID, auxDocidRef->get(matchDoc));
    }
    {
	hashJoinInfo->getHashJoinMap() = {
	    {TableUtil::calculateHashValue<string>("0"), {1}},
	    {TableUtil::calculateHashValue<string>("1"), {1, 2}}
	};
        std::list<matchdoc::MatchDoc> matchDocs;
        SingleLayerSearcher::doJoin<string>(&matchDocAllocator,
            matchDoc,
            matchDocs,
            hashJoinInfo.get(),
            joinAttrExpr.get());
        ASSERT_TRUE(matchDocs.empty());
        ASSERT_EQ((docid_t)1, auxDocidRef->get(matchDoc));
    }
    {
	hashJoinInfo->getHashJoinMap() = {
	    {TableUtil::calculateHashValue<string>("0"), {1, 2}},
	    {TableUtil::calculateHashValue<string>("1"), {3, 4}}
	};
        std::list<matchdoc::MatchDoc> matchDocs;
        SingleLayerSearcher::doJoin<string>(&matchDocAllocator,
            matchDoc,
            matchDocs,
            hashJoinInfo.get(),
            joinAttrExpr.get());
        ASSERT_EQ(1u, matchDocs.size());
	ASSERT_EQ((docid_t)1, auxDocidRef->get(matchDoc));
	ASSERT_EQ((docid_t)2, auxDocidRef->get(matchDocs.front()));
        ASSERT_EQ(string("hello"), testFieldRef->get(matchDocs.front()));
    }
    {
        hashJoinInfo->getHashJoinMap() = {
	    { TableUtil::calculateHashValue<string>("0"), { 1, 2, 3 } },
	    { TableUtil::calculateHashValue<string>("1"), { 3, 4 } }
	};
        std::list<matchdoc::MatchDoc> matchDocs;
        SingleLayerSearcher::doJoin<string>(&matchDocAllocator,
            matchDoc,
            matchDocs,
            hashJoinInfo.get(),
            joinAttrExpr.get());
        ASSERT_EQ(2u, matchDocs.size());
	ASSERT_EQ((docid_t)1, auxDocidRef->get(matchDoc));
	auto it = matchDocs.begin();
	ASSERT_EQ((docid_t)2, auxDocidRef->get(*it));
	ASSERT_EQ(string("hello"), testFieldRef->get(*it));
	it++;
	ASSERT_EQ((docid_t)3, auxDocidRef->get(*it));
	ASSERT_EQ(string("hello"), testFieldRef->get(*it));
    }
}

END_HA3_NAMESPACE(search);
