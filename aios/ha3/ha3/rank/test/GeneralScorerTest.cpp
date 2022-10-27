#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/rank/GeneralScorer.h>
#include <ha3/rank/ScoringProvider.h>
#include <ha3_sdk/testlib/index/FakePostingMaker.h>
#include <ha3_sdk/testlib/index/FakeTextIndexReader.h>
#include <suez/turing/expression/util/IndexInfos.h>
#include <ha3/config/IndexInfoHelper.h>
#include <ha3/config/LegacyIndexInfoHelper.h>
#include <suez/turing/expression/util/FieldBoostTable.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReader.h>
#include <ha3/config/ConfigAdapter.h>
#include <ha3/search/QueryExecutor.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/search/MatchDataManager.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>
#include <ha3/search/test/SearcherTestHelper.h>
#include <ha3/search/QueryExecutorCreator.h>
#include <ha3/search/test/FakeAttributeExpressionCreator.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <ha3/test/test.h>
#include <math.h>

using namespace suez::turing;

namespace
{

constexpr IE_NAMESPACE(common)::ErrorCode IE_OK = IE_NAMESPACE(common)::ErrorCode::OK;

}

BEGIN_HA3_NAMESPACE(rank);


class GeneralScorerTest : public TESTBASE {

public:
    GeneralScorerTest();
    ~GeneralScorerTest();
public:
    void setUp();
    void tearDown();
protected:
    ScoringProvider *_scoringProvider;
    index::IndexReaderPtr _readerPtr;
    common::Ha3MatchDocAllocatorPtr _allocator;
    search::QueryExecutor *_queryExe;
    config::LegacyIndexInfoHelper *_indexInfoHelper;
    IndexInfos *_indexInfos;
    AttributeExpressionCreator *_attrExprCreator;
    config::ResourceReaderPtr _resourceReaderPtr;
    common::DataProvider *_dataProvider;
protected:
    void prepareMatchData(matchdoc::MatchDoc matchDoc);
    void prepareQueryExecutor();
    IndexInfos* createIndexInfos();
    void prepareScoringProvider();
    void prepareQueryExecutorWithField();
    void prepareQueryExecutorWithMultiWords();
    void prepareQueryExecutorWithMultiTermsProximity1();
    void prepareQueryExecutorWithMultiTermsProximity2();
    void prepareQueryExecutorForDFOne();
    void prepareOrQueryExecutor();
    void prepareAttributeExpression();
    Scorer* getCloneScorer(KeyValueMap &scorerParameters);
    void checkStaticScore(KeyValueMap &scorerParameters, score_t expectScore);
    void checkScoreResult(Scorer *scorer, float expectScore , int32_t phaseNum = 0);
    void checkDefaultValue(float *array, int *pos, int len, GeneralScorer::FloatVecPtr tablePtr);
    void checkScoreResult(docid_t seekDocId, float expectScore, docid_t seekId = 0);
    void checkScoreWithParameters(docid_t seekId, docid_t docId, float expectScore,
                                  KeyValueMap &scorerParameters,
                                  float totalFreq = 0, int32_t phase = 0,
                                  score_t expectScoreOne = 0);
    config::ResourceReaderPtr getResourceReader();
    void createIndexPartitionReaderWrapper(const std::string &indexStr);
protected:
    search::MatchDataManager *_manager;
    search::IndexPartitionReaderWrapperPtr _indexPartitionReaderWrapper;
    autil::mem_pool::Pool *_pool;
    IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr _snapshotPtr;
    IE_NAMESPACE(partition)::TableMem2IdMap _emptyTableMem2IdMap;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(rank, GeneralScorerTest);

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(partition);

USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);

using namespace std;
using namespace autil;

GeneralScorerTest::GeneralScorerTest() {
    _scoringProvider = NULL;
    _queryExe = NULL;
    _indexInfoHelper = NULL;
    _indexInfos = NULL;
    _attrExprCreator = NULL;
    _dataProvider = NULL;
    _manager = NULL;
    getResourceReader();
    _pool = new mem_pool::Pool(1024);
}

GeneralScorerTest::~GeneralScorerTest() {
    delete _pool;
}

void GeneralScorerTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");

}
void GeneralScorerTest::prepareScoringProvider()
{
    if (!_allocator) {
        _allocator.reset(new common::Ha3MatchDocAllocator(_pool));
    }
    _indexInfos = createIndexInfos();
    _indexInfoHelper = new LegacyIndexInfoHelper();
    _indexInfoHelper->setIndexInfos(_indexInfos);

    const FieldBoostTable& fieldBoostTable = _indexInfos->getFieldBoostTable();
    const FieldBoostTable *fieldBoostTablePtr = &fieldBoostTable;
    _dataProvider = new DataProvider();

    _snapshotPtr.reset(new PartitionReaderSnapshot(&_emptyTableMem2IdMap,
                    &_emptyTableMem2IdMap, &_emptyTableMem2IdMap,
                    std::vector<IndexPartitionReaderInfo>()));

    RankResource rankResource;
    rankResource.pool = _pool;
    rankResource.attrExprCreator = _attrExprCreator;
    rankResource.indexInfoHelper = _indexInfoHelper;
    rankResource.boostTable = fieldBoostTablePtr;
    rankResource.dataProvider = _dataProvider;
    rankResource.matchDocAllocator = _allocator;
    rankResource.matchDataManager = _manager;
    rankResource.partitionReaderSnapshot = _snapshotPtr.get();

    _scoringProvider = new ScoringProvider(rankResource);
}

void GeneralScorerTest::prepareAttributeExpression()
{
    FakeIndex fakeIndex;
    fakeIndex.attributes = "static_score:int32_t:0,6000,537";
    IndexPartitionReaderWrapperPtr wrapperPtr =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    if (NULL == _allocator) {
        _allocator.reset(new common::Ha3MatchDocAllocator(_pool));
    }
    _attrExprCreator = new FakeAttributeExpressionCreator(_pool, wrapperPtr,
            NULL, NULL, NULL, NULL, NULL);
}

void GeneralScorerTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");

    DELETE_AND_SET_NULL(_scoringProvider);
    _allocator.reset();
    DELETE_AND_SET_NULL(_dataProvider);
    DELETE_AND_SET_NULL(_indexInfoHelper);
    DELETE_AND_SET_NULL(_indexInfos);
    DELETE_AND_SET_NULL(_attrExprCreator);
    DELETE_AND_SET_NULL(_manager);
    POOL_DELETE_CLASS(_queryExe);

}

TEST_F(GeneralScorerTest, testSimpleProcess) {
    HA3_LOG(DEBUG, "Begin Test!");

    prepareQueryExecutor();
    checkScoreResult(0, 1360.283);
    //ASSERT_DOUBLE_EQ(1,log(2.0));
}

TEST_F(GeneralScorerTest, testScoreWithBoostTable)
{
    HA3_LOG(DEBUG, "Begin Test!");

    prepareQueryExecutorWithField();
    checkScoreResult(1, 2167.406);
}

TEST_F(GeneralScorerTest, testScoreWithMultiWords)
{
    HA3_LOG(DEBUG, "Begin Test!");

    prepareQueryExecutorWithMultiWords();
    checkScoreResult(1, 7146.284);
}

TEST_F(GeneralScorerTest, testScoreWithMultiTermsProximity)
{
    HA3_LOG(DEBUG, "Begin Test!");

    prepareQueryExecutorWithMultiTermsProximity1();
    checkScoreResult(1, 6785.912);
}

TEST_F(GeneralScorerTest, testPhaseOne)
{
    HA3_LOG(DEBUG, "Begin Test!");

    KeyValueMap scorerParameters;
    scorerParameters.insert(make_pair(SCORE_PHASE_NUM, "1"));
    prepareQueryExecutorWithMultiTermsProximity1();
    checkScoreWithParameters(0, 1, 6785.913, scorerParameters, 2.9047487, 1);
}

TEST_F(GeneralScorerTest, testPhaseTwo)
{
    HA3_LOG(DEBUG, "Begin Test!");

    KeyValueMap scorerParameters;
    scorerParameters.insert(make_pair(SCORE_PHASE_NUM, "2"));
    prepareQueryExecutorWithMultiTermsProximity1();
    checkScoreWithParameters(0, 1, 6905.2866, scorerParameters, 2.9047487, 2, 6785.913);
}

TEST_F(GeneralScorerTest, testOrQueryScore)
{
    HA3_LOG(DEBUG, "Begin Test!");

    KeyValueMap scorerParameters;
    scorerParameters.insert(make_pair(SCORE_PHASE_NUM, "0"));
    prepareOrQueryExecutor();
    checkScoreWithParameters(0, 1, 2399.990, scorerParameters);

    checkScoreWithParameters(2, 19, 2080.234, scorerParameters);
    checkScoreWithParameters(20, 28, 3058.987, scorerParameters);
    checkScoreWithParameters(29, 30, 4573.538, scorerParameters);

}
void GeneralScorerTest::checkScoreResult(
        docid_t docId, float expectScore, docid_t seekDocId)
{
    KeyValueMap scorerParameters;
    checkScoreWithParameters(seekDocId,docId,expectScore,scorerParameters);
}
void GeneralScorerTest::checkScoreWithParameters(
        docid_t seekId, docid_t docId, float expectScore, KeyValueMap &scorerParameters,
        float totalFreq, int32_t phase, score_t expectScoreOne)
{
    prepareScoringProvider();
    GeneralScorer generalScorer(scorerParameters, NULL);

    ASSERT_TRUE(generalScorer.beginRequest(_scoringProvider));
    matchdoc::MatchDoc matchDoc1 = _allocator->allocate(docId);

    docid_t tmpid = INVALID_DOCID;
    ASSERT_EQ(IE_OK, _queryExe->seek(seekId, tmpid));
    ASSERT_EQ(docId, tmpid);

    _scoringProvider->prepareMatchDoc(matchDoc1);
    prepareMatchData(matchDoc1);

    if (2 == phase) {
        generalScorer._totalFreqRef->set(matchDoc1,totalFreq);
        generalScorer._phaseOneScoreRef->set(matchDoc1,expectScoreOne);
    }

    float score = generalScorer.score(matchDoc1);
    ASSERT_FLOAT_EQ(expectScore, score);

    if (1 == phase) {
        ASSERT_EQ(1, generalScorer._phase );
        ASSERT_TRUE(generalScorer._totalFreqRef);
        ASSERT_TRUE(generalScorer._phaseOneScoreRef);
        float freq = 0;
        score_t scoreOne = 0.0;
        freq = generalScorer._totalFreqRef->get(matchDoc1);
        scoreOne = generalScorer._phaseOneScoreRef->get(matchDoc1);
        ASSERT_FLOAT_EQ(totalFreq, freq);
        ASSERT_FLOAT_EQ(expectScore, scoreOne);
    }
    if (2 == phase) {
        ASSERT_EQ(2, generalScorer._phase );
        ASSERT_TRUE(generalScorer._totalFreqRef);
        ASSERT_TRUE(generalScorer._phaseOneScoreRef);
        float freq = 0;
        score_t scoreOne = 0.0;
        freq = generalScorer._totalFreqRef->get(matchDoc1);
        scoreOne = generalScorer._phaseOneScoreRef->get(matchDoc1);
        ASSERT_DOUBLE_EQ(totalFreq, freq);
        ASSERT_DOUBLE_EQ(expectScoreOne, scoreOne);
    }
    _allocator->deallocate(matchDoc1);
    generalScorer.endRequest();
    DELETE_AND_SET_NULL(_scoringProvider);
    _allocator.reset();
    _manager->resetMatchData();
    DELETE_AND_SET_NULL(_dataProvider);
    DELETE_AND_SET_NULL(_indexInfoHelper);
    DELETE_AND_SET_NULL(_indexInfos);
    DELETE_AND_SET_NULL(_attrExprCreator);
}

void GeneralScorerTest::prepareQueryExecutor()
{
    createIndexPartitionReaderWrapper("ALIBABA:0[20];1[10,20,30];\n");
    unique_ptr<Query> query(SearcherTestHelper::createQuery("phrase:ALIBABA^111"));
    DELETE_AND_SET_NULL(_manager);
    _manager = new MatchDataManager;
    _manager->setQueryCount(1);
    QueryExecutorCreator creator(_manager, _indexPartitionReaderWrapper.get(), _pool);
    query->accept(&creator);
    _queryExe = creator.stealQuery();
}

void GeneralScorerTest::prepareQueryExecutorWithField()
{
    string str1 = "abc^1:1^5[1_0,3_0,5_1,23_0];19^4[4_1,23_3,55_0];28^1[8_1,23_2,59_0];\n";
    createIndexPartitionReaderWrapper(str1);
    unique_ptr<Query> query(SearcherTestHelper::createQuery("phrase:abc^111"));
    DELETE_AND_SET_NULL(_manager);
    _manager = new MatchDataManager;
    _manager->setQueryCount(1);
    QueryExecutorCreator creator(_manager, _indexPartitionReaderWrapper.get(), _pool);
    query->accept(&creator);
    _queryExe = creator.stealQuery();
}

void GeneralScorerTest::prepareOrQueryExecutor()
{
    string str1 = "中国^1:1^5[1_0,4_0,10_1,23_0];30^5[1_0,4_0,10_1,23_0];\n";
    string str2 = "浙江^1:28^1[9_0,24_2,60_0];\n";
    string str3 = "杭州^1:19^4[7_1,32_3,59_0];30^4[7_1,32_3,59_0];\n";

    createIndexPartitionReaderWrapper(str1 + str2 + str3);
    unique_ptr<Query> query(SearcherTestHelper::createQuery("phrase:中国^111 OR phrase:浙江^111 OR phrase:杭州^111"));
    DELETE_AND_SET_NULL(_manager);
    _manager = new MatchDataManager;
    _manager->setQueryCount(1);
    QueryExecutorCreator creator(_manager, _indexPartitionReaderWrapper.get(), _pool);

    query->accept(&creator);
    _queryExe = creator.stealQuery();
}

IndexInfos* GeneralScorerTest::createIndexInfos() {

    IndexInfos *indexInfos = new IndexInfos();
    IndexInfo *indexInfo1 = new IndexInfo();
    indexInfo1->indexName = "default";

    IndexInfo *indexInfo = new IndexInfo();
    indexInfo->indexId = 1;
    indexInfo->indexName = "phrase";
    indexInfo->addField("title", 100);
    indexInfo->addField("body", 50);
    indexInfo->addField("description", 20);

    indexInfos->addIndexInfo(indexInfo1);
    indexInfos->addIndexInfo(indexInfo);

    return indexInfos;
}



void GeneralScorerTest::prepareQueryExecutorWithMultiWords()
{
    string str1 = "中国^1:1^5[1_0,4_0,10_1,23_0];19^4[4_1,23_3,55_0];28^1[8_1,23_2,59_0];\n";
    string str2 = "浙江^1:1^5[2_0,7_0,11_1];28^1[9_0,24_2,60_0];\n";
    string str3 = "杭州^1:1^5[3_0,9_0,15_1,24_0];19^4[7_1,32_3,59_0];\n";
    createIndexPartitionReaderWrapper(str1 + str2 + str3);
    unique_ptr<Query> query(SearcherTestHelper::createQuery("phrase:中国^111 AND phrase:浙江^111 AND phrase:杭州^111"));
    DELETE_AND_SET_NULL(_manager);
    _manager = new MatchDataManager;
    _manager->setQueryCount(1);
    QueryExecutorCreator creator(_manager, _indexPartitionReaderWrapper.get(), _pool);
    query->accept(&creator);
    _queryExe = creator.stealQuery();
}

void GeneralScorerTest::prepareQueryExecutorWithMultiTermsProximity1()
{
    string str1 = "中国^1:1^5[1_0,12_0,25_1,30_0];19^4[4_1,23_3,55_0];28^1[8_1,23_2,59_0];\n";
    string str2 = "浙江^1:1^5[5_0,7_0,18_1];28^1[9_0,24_2,60_0];\n";
    string str3 = "杭州^1:1^5[3_0,9_0,17_1,35_0];19^4[7_1,32_3,59_0];\n";
    createIndexPartitionReaderWrapper(str1 + str2 + str3);
    unique_ptr<Query> query(SearcherTestHelper::createQuery("phrase:中国^111 AND phrase:浙江^111 AND phrase:杭州^111"));
    DELETE_AND_SET_NULL(_manager);
    _manager = new MatchDataManager;
    _manager->setQueryCount(1);
    QueryExecutorCreator creator(_manager, _indexPartitionReaderWrapper.get(), _pool);
    query->accept(&creator);
    _queryExe = creator.stealQuery();
}

void GeneralScorerTest::prepareQueryExecutorWithMultiTermsProximity2()
{
    string str1 = "中国^1:1^5[1_0,12_0,25_1,30_0];19^4[4_1,23_3,55_0];28^1[8_1,23_2,59_0];\n";
    string str2 = "浙江^1:1^5[5_0,7_0,18_1];28^1[9_0,24_2,60_0];\n";
    string str3 = "杭州^1:1^5[3_0,9_0,15_1,35_0];19^4[7_1,32_3,59_0];\n";
    createIndexPartitionReaderWrapper(str1 + str2 + str3);
    unique_ptr<Query> query(SearcherTestHelper::createQuery("phrase:中国^111 AND phrase:浙江^111 AND phrase:杭州^111"));
    DELETE_AND_SET_NULL(_manager);
    _manager = new MatchDataManager;
    _manager->setQueryCount(1);
    QueryExecutorCreator creator(_manager, _indexPartitionReaderWrapper.get(), _pool);

    query->accept(&creator);
    _queryExe = creator.stealQuery();
}
TEST_F( GeneralScorerTest, testgetMinDist)
{
    HA3_LOG(DEBUG, "Begin Test!");

    int32_t arryPos[] = {2,4,1};
    std::vector<pos_t> vctPos(arryPos,arryPos+3);
    pos_t testPos = 9;
    KeyValueMap scorerParameters;
    GeneralScorer generalScorer(scorerParameters, NULL);
    int32_t value = generalScorer.getMinDist(vctPos,testPos);
    ASSERT_EQ(5,value);
}

TEST_F(GeneralScorerTest, testMatchedFieldProximityExtractorWithOneDistance){
    HA3_LOG(DEBUG, "Begin Test!");

    prepareQueryExecutorWithMultiTermsProximity1();
    prepareScoringProvider();
    KeyValueMap scorerParameters;
    GeneralScorer generalScorer(scorerParameters, NULL);
    ASSERT_TRUE(generalScorer.beginRequest(_scoringProvider));
    matchdoc::MatchDoc matchDoc1 = _allocator->allocate((docid_t)1);

    docid_t tmpid = INVALID_DOCID;
    ASSERT_EQ(IE_OK, _queryExe->seek(0, tmpid));
    ASSERT_EQ((docid_t)1, tmpid);
    _scoringProvider->prepareMatchDoc(matchDoc1);
    prepareMatchData(matchDoc1);

    uint32_t value = generalScorer.MatchedFieldProximityExtractor(1,matchDoc1);
    ASSERT_EQ((uint32_t)1,value);
    _allocator->deallocate(matchDoc1);
    generalScorer.endRequest();
}
TEST_F(GeneralScorerTest, testMatchedFieldProximityExtractorWithThreeDistance){
    HA3_LOG(DEBUG, "Begin Test!");

    prepareQueryExecutorWithMultiTermsProximity2();
    prepareScoringProvider();
    KeyValueMap scorerParameters;
    GeneralScorer generalScorer(scorerParameters, NULL);
    ASSERT_TRUE(generalScorer.beginRequest(_scoringProvider));
    matchdoc::MatchDoc matchDoc1 = _allocator->allocate((docid_t)1);

    docid_t tmpid = INVALID_DOCID;
    _queryExe->seek(0, tmpid);
    ASSERT_EQ((docid_t)1, tmpid);
    _scoringProvider->prepareMatchDoc(matchDoc1);
    prepareMatchData(matchDoc1);

    uint32_t value = generalScorer.MatchedFieldProximityExtractor(1,matchDoc1);
    ASSERT_EQ((uint32_t)3,value);
    _allocator->deallocate(matchDoc1);
    generalScorer.endRequest();
}

TEST_F(GeneralScorerTest, testBeginRequestFail)
{
    HA3_LOG(DEBUG, "Begin Test!");

    KeyValueMap scorerParameters;
    GeneralScorer generalScorer(scorerParameters, NULL);
    bool ret = generalScorer.beginRequest(_scoringProvider);
    ASSERT_EQ(false, ret);
    generalScorer.endRequest();
}

TEST_F(GeneralScorerTest, testExtractIndexId)
{
    HA3_LOG(DEBUG, "Begin Test!");

    prepareQueryExecutorWithMultiTermsProximity2();
    prepareScoringProvider();
    KeyValueMap scorerParameters;
    GeneralScorer generalScorer(scorerParameters, NULL);
    ASSERT_TRUE(generalScorer.beginRequest(_scoringProvider));
    ASSERT_EQ((indexid_t)1,generalScorer._indexId[0]);
    ASSERT_EQ((indexid_t)1,generalScorer._indexId[1]);
    ASSERT_EQ((indexid_t)1,generalScorer._indexId[2]);
    generalScorer.endRequest();
}
TEST_F(GeneralScorerTest, testInitTableNumOccWithDefaultValue)
{
    HA3_LOG(DEBUG, "Begin Test!");

    float array[4] = {1.39996, 2.19964, 100.484, 100.865};
    int pos[4] = {0,2,254,255};
    KeyValueMap scorerParameters;
    GeneralScorer generalScorer(scorerParameters, NULL);

    checkDefaultValue(array, pos, 4, generalScorer._numOccTablePtr);
}
TEST_F(GeneralScorerTest, testInitTableFirstOccWithDefaultValue)
{
    HA3_LOG(DEBUG, "Begin Test!");

    float array[4] = { 6658.61, 6024.96, 0.0203162, 0.0193254};
    int pos[4] = {0,2,254,255};
    KeyValueMap scorerParameters;
    GeneralScorer generalScorer(scorerParameters, NULL);
    checkDefaultValue(array, pos, 4, generalScorer._firstOccTablePtr);
}

TEST_F(GeneralScorerTest, testInitTableProximityWithDefaultValue)
{
    HA3_LOG(DEBUG, "Begin Test!");

    float array[4] = {346.751, 260.576, 6.04411e-014, 5.23951e-014};
    int pos[4] = {0,2,254,255};
    KeyValueMap scorerParameters;
    GeneralScorer generalScorer(scorerParameters, NULL);
    checkDefaultValue(array, pos, 4,  generalScorer._proximityTablePtr);
}
void GeneralScorerTest::checkDefaultValue(float *array,
        int *pos,
        int len,
        GeneralScorer::FloatVecPtr tablePtr)
{
    for (int i=0; i<len; ++i) {
        ASSERT_DOUBLE_EQ(array[i],tablePtr->at(pos[i]));
    }
}


TEST_F(GeneralScorerTest, testInitTableWithFile)
{
    HA3_LOG(DEBUG, "Begin Test!");

    KeyValueMap scorerParameters;
    scorerParameters.insert(make_pair(RESOURCE_FILE_NAME, "correctValue.tbl"));

    ASSERT_TRUE(_resourceReaderPtr.get());

    GeneralScorer generalScorer(scorerParameters, _resourceReaderPtr.get());

    float array1[3] = {2.39996, 111.865, 3.19964};
    int pos[3] = {0, 255, 2};
    checkDefaultValue(array1, pos, 3,  generalScorer._firstOccTablePtr);

    float array2[3] = {10.39996,101.865, 5.19964};
    checkDefaultValue(array2, pos, 3,  generalScorer._numOccTablePtr);

    float array3[3] = {1.39996, 121.865, 7.19964};
    checkDefaultValue(array3, pos, 3,  generalScorer._proximityTablePtr);
}

TEST_F(GeneralScorerTest, testInitWithWrongFile)
{

    KeyValueMap scorerParameters;
    scorerParameters.insert(make_pair(RESOURCE_FILE_NAME, "wrongValue.tbl"));

    ASSERT_TRUE(_resourceReaderPtr.get());

    GeneralScorer generalScorer(scorerParameters, _resourceReaderPtr.get());

    float array1[4] = { 6658.61, 6024.96, 0.0203162, 0.0193254};
    int pos[4] = {0,2,254,255};
    checkDefaultValue(array1, pos, 4,  generalScorer._firstOccTablePtr);

    float array2[4] = {1.39996, 2.19964, 100.484, 100.865};
    checkDefaultValue(array2, pos, 4,  generalScorer._numOccTablePtr);

    float array3[4] = {346.751, 260.576, 6.04411e-014, 5.23951e-014};
    checkDefaultValue(array3, pos, 4,  generalScorer._proximityTablePtr);
}


TEST_F(GeneralScorerTest, testInitWithIncompleteFile)
{

    KeyValueMap scorerParameters;
    scorerParameters.insert(make_pair(RESOURCE_FILE_NAME, "incompleteValue.tbl"));

    ASSERT_TRUE(_resourceReaderPtr.get());

    GeneralScorer generalScorer(scorerParameters, _resourceReaderPtr.get());


    float array1[3] = {2.39996, 111.865, 3.19964};
    int pos[3] = {0, 255, 2};
    checkDefaultValue(array1, pos, 3,  generalScorer._firstOccTablePtr);

    float array3[3] = {1.39996, 121.865, 7.19964};
    checkDefaultValue(array3, pos, 3,  generalScorer._proximityTablePtr);

    int pos1[4] = {0,2,254,255};
    float array2[4] = {1.39996, 2.19964, 100.484, 100.865};
    checkDefaultValue(array2, pos1, 4,  generalScorer._numOccTablePtr);

}

TEST_F(GeneralScorerTest, testGetContentFromResourceFileWithNullPointer)
{
    KeyValueMap scorerParameters;
    scorerParameters.insert(make_pair(RESOURCE_FILE_NAME, "none_exist_file"));
    GeneralScorer generalScorer(scorerParameters, NULL);
    std::string content;
    bool ret = generalScorer.getContentFromResourceFile(RESOURCE_FILE_NAME,
            scorerParameters, content, NULL);
    ASSERT_TRUE(!ret);

    ASSERT_TRUE(_resourceReaderPtr.get());
    generalScorer.getContentFromResourceFile(RESOURCE_FILE_NAME,
            scorerParameters, content, _resourceReaderPtr.get());

    ASSERT_TRUE(content.empty());


}
TEST_F(GeneralScorerTest, testStaticScore)
{
    HA3_LOG(DEBUG, "Begin Test!");

    KeyValueMap scorerParameters;
    scorerParameters.insert(make_pair(STATIC_SCORE_FIELD,"static_score"));
    scorerParameters.insert(make_pair(STATIC_SCORE_WEIGHT,"0.5"));
    checkStaticScore(scorerParameters, 15785.912);
}

TEST_F(GeneralScorerTest, testStaticScoreWithoutWeight)
{
    HA3_LOG(DEBUG, "Begin Test!");

    KeyValueMap scorerParameters;
    scorerParameters.insert(make_pair(STATIC_SCORE_FIELD,"static_score"));
    checkStaticScore(scorerParameters, 24785.912);
}

TEST_F(GeneralScorerTest, testStaticScoreWithoutStaticScoreField)
{
    HA3_LOG(DEBUG, "Begin Test!");

    KeyValueMap scorerParameters;
    scorerParameters.insert(make_pair(STATIC_SCORE_WEIGHT,"0.5"));
    checkStaticScore(scorerParameters, 6785.913);
}

TEST_F(GeneralScorerTest, testClone)
{
    HA3_LOG(DEBUG, "Begin Test!");

    KeyValueMap scorerParameters;
    scorerParameters.insert(make_pair(STATIC_SCORE_FIELD,"static_score"));
    scorerParameters.insert(make_pair(STATIC_SCORE_WEIGHT,"0.5"));
    Scorer* cloneScorer= getCloneScorer(scorerParameters);
    checkScoreResult(cloneScorer, 15785.912);
    cloneScorer->destroy();
}
TEST_F(GeneralScorerTest, testPhaseOneClone)
{
    HA3_LOG(DEBUG, "Begin Test!");

    KeyValueMap scorerParameters;
    scorerParameters.insert(make_pair(STATIC_SCORE_FIELD,"static_score"));
    scorerParameters.insert(make_pair(STATIC_SCORE_WEIGHT,"0.5"));
    scorerParameters.insert(make_pair(SCORE_PHASE_NUM,"1"));

    Scorer* cloneScorer= getCloneScorer(scorerParameters);
    checkScoreResult(cloneScorer, 15785.912);
    cloneScorer->destroy();
}
TEST_F(GeneralScorerTest, testPhaseTwoClone)
{
    HA3_LOG(DEBUG, "Begin Test!");

    KeyValueMap scorerParameters;
    scorerParameters.insert(make_pair(STATIC_SCORE_FIELD,"static_score"));
    scorerParameters.insert(make_pair(STATIC_SCORE_WEIGHT,"0.5"));
    scorerParameters.insert(make_pair(SCORE_PHASE_NUM,"2"));
    Scorer* cloneScorer= getCloneScorer(scorerParameters);
    checkScoreResult(cloneScorer, 1150.2955, 2);
    cloneScorer->destroy();
}

Scorer* GeneralScorerTest::getCloneScorer(KeyValueMap &scorerParameters)
{
    prepareAttributeExpression();
    prepareQueryExecutorWithMultiTermsProximity2();
    prepareScoringProvider();
    GeneralScorer generalScorer(scorerParameters, NULL);
    return generalScorer.clone();
}

void GeneralScorerTest::checkStaticScore(KeyValueMap &scorerParameters, score_t expectScore)
{
    prepareAttributeExpression();
    prepareQueryExecutorWithMultiTermsProximity2();

    prepareScoringProvider();
    GeneralScorer generalScorer(scorerParameters, NULL);
    checkScoreResult(&generalScorer,expectScore);
}

void GeneralScorerTest::checkScoreResult(Scorer *scorer, float expectScore, int32_t phaseNum)
{
    ASSERT_TRUE(scorer->beginRequest(_scoringProvider));
    matchdoc::MatchDoc matchDoc1 = _allocator->allocate((docid_t)1);

    docid_t tmpid = INVALID_DOCID;
    ASSERT_EQ(IE_OK, _queryExe->seek(0, tmpid));
    ASSERT_EQ((docid_t)1, tmpid);
    _scoringProvider->prepareMatchDoc(matchDoc1);
    prepareMatchData(matchDoc1);
    if (phaseNum == 2) {
        float freq = 2.0;
        score_t score = 1000.0;
        dynamic_cast<GeneralScorer*> (scorer)->_totalFreqRef->set(matchDoc1,freq);
        dynamic_cast<GeneralScorer*> (scorer)->_phaseOneScoreRef->set(matchDoc1,score);
    }

    float score = scorer->score(matchDoc1);

    ASSERT_FLOAT_EQ(expectScore, score);
    _allocator->deallocate(matchDoc1);

    scorer->endRequest();

}

TEST_F(GeneralScorerTest, testDFWithOneCount)
{
    HA3_LOG(DEBUG, "Begin Test!");

    prepareQueryExecutorForDFOne();
    prepareScoringProvider();
    KeyValueMap scorerParameters;
    GeneralScorer generalScorer(scorerParameters, NULL);

    ASSERT_TRUE(generalScorer.beginRequest(_scoringProvider));
    matchdoc::MatchDoc matchDoc1 = _allocator->allocate((docid_t)1);

    docid_t tmpid = INVALID_DOCID;
    ASSERT_EQ(IE_OK, _queryExe->seek(0, tmpid));
    ASSERT_EQ((docid_t)1, tmpid);

    _scoringProvider->prepareMatchDoc(matchDoc1);
    prepareMatchData(matchDoc1);

    ASSERT_EQ(1,generalScorer._metaInfo[0].getTotalTermFreq());

    float score = generalScorer.score(matchDoc1);
    ASSERT_FLOAT_EQ(7143.138, score);

    _allocator->deallocate(matchDoc1);
    generalScorer.endRequest();
}

void GeneralScorerTest::prepareQueryExecutorForDFOne()
{
    string str1 = "abc:1_1-5[1_0];";
    createIndexPartitionReaderWrapper(str1);
    unique_ptr<Query> query(SearcherTestHelper::createQuery("phrase:abc^111"));
    DELETE_AND_SET_NULL(_manager);
    _manager = new MatchDataManager;
    _manager->setQueryCount(1);
    QueryExecutorCreator creator(_manager, _indexPartitionReaderWrapper.get(), _pool);

    query->accept(&creator);
    _queryExe = creator.stealQuery();
}

config::ResourceReaderPtr GeneralScorerTest::getResourceReader()
{
    string configRoot = TEST_DATA_CONF_PATH"/configurations/configuration_1/";
    ConfigAdapterPtr configAdapterPtr(new ConfigAdapter(configRoot));
    _resourceReaderPtr.reset(new ResourceReader(configAdapterPtr->getConfigPath()));
    return _resourceReaderPtr;

}

void GeneralScorerTest::prepareMatchData(matchdoc::MatchDoc matchDoc) {
    _manager->fillMatchData(matchDoc);
}

void GeneralScorerTest::createIndexPartitionReaderWrapper(
        const string &indexStr)
{
    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] = indexStr;
    _indexPartitionReaderWrapper =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    _indexPartitionReaderWrapper->setTopK(1000);
}

END_HA3_NAMESPACE(rank);
