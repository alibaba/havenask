#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Request.h>
#include <ha3/search/SortExpressionCreator.h>
#include <ha3/rank/RankProfileManager.h>
#include <ha3/rank/RankProfile.h>
#include <suez/turing/expression/syntax/SyntaxExpr.h>
#include <ha3/search/RankResource.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/rank/RankProfile.h>
#include <ha3/config/ResourceReader.h>
#include <matchdoc/MatchDocAllocator.h>
#include <ha3_sdk/testlib/qrs/RequestCreator.h>
#include <build_service/plugin/PlugInManager.h>
#include <ha3/search/test/FakeAttributeExpressionFactory.h>
#include <ha3/search/test/FakeAttributeExpressionCreator.h>

using namespace std;
USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(qrs);
using namespace build_service::plugin;
using namespace suez::turing;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);
BEGIN_HA3_NAMESPACE(search);

class SortExpressionCreatorTest : public TESTBASE {
public:
    SortExpressionCreatorTest();
    ~SortExpressionCreatorTest();
public:
    void setUp();
    void tearDown();
protected:
    common::RequestPtr prepareRequest(std::string &query, 
            const std::map<std::string, ExprResultType> &typeInfo);
    
protected:
    rank::RankProfile *_rankProfile;
    rank::RankProfileManagerPtr _rankProfileManagerPtr; 
    AttributeExpressionCreator *_attrExprCreator;
    common::Ha3MatchDocAllocatorPtr _allocator;
    common::MultiErrorResultPtr _errorResultPtr; 
    autil::mem_pool::Pool *_pool;
    const IndexPartitionReaderWrapperPtr indexPartReaderWrapper;
    CavaPluginManagerPtr _cavaPluginManagerPtr;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, SortExpressionCreatorTest);


SortExpressionCreatorTest::SortExpressionCreatorTest() { 
    _pool = new autil::mem_pool::Pool(1024);
}

SortExpressionCreatorTest::~SortExpressionCreatorTest() { 
    _allocator.reset();
    if (_pool) {
        delete _pool;
        _pool = NULL;
    }
}

void SortExpressionCreatorTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
    PlugInManagerPtr nullPtr;
    config::ResourceReaderPtr resourceReaderPtr;
    _rankProfileManagerPtr.reset(new RankProfileManager(nullPtr));
    _rankProfile = new RankProfile("DefaultProfile");
    ScorerInfo scorerInfo;
    scorerInfo.scorerName = "DefaultScorer";
    scorerInfo.rankSize = 100;
    _rankProfile->addScorerInfo(scorerInfo);
    scorerInfo.rankSize = 10;
    _rankProfile->addScorerInfo(scorerInfo);
    _rankProfileManagerPtr->addRankProfile(_rankProfile);
    _rankProfileManagerPtr->init(resourceReaderPtr, _cavaPluginManagerPtr, NULL);
    FakeAttributeExpressionFactory * fakeFactory =
        POOL_NEW_CLASS(_pool, FakeAttributeExpressionFactory,
                       "uid", "1, 2, 3, 4, 5, 6, 7, 8, 9, 10", _pool);
    fakeFactory->setInt32Attribute("uid1", "11, 12, 13, 14, 15, 16, 17, 18, 19, 20");
    fakeFactory->setInt32Attribute("uid2", "21, 22, 23, 24, 25, 26, 27, 28, 29, 30");
    fakeFactory->setInt32Attribute("uid3", "31, 32, 33, 34, 35, 36, 37, 38, 39, 40");
    
    _attrExprCreator = new FakeAttributeExpressionCreator(_pool, indexPartReaderWrapper, 
            NULL, NULL, NULL, NULL, NULL);
    _attrExprCreator->resetAttrExprFactory(fakeFactory);
    _allocator.reset(new common::Ha3MatchDocAllocator(_pool)); 
    _errorResultPtr.reset(new MultiErrorResult());
}

void SortExpressionCreatorTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
    if (_attrExprCreator) {
        delete _attrExprCreator;
        _attrExprCreator = NULL;
    }
}

TEST_F(SortExpressionCreatorTest, testCreateWithEmptyRankSortAndSort)
{ 
    HA3_LOG(DEBUG, "Begin Test!");
    SortExpressionCreator creator(_attrExprCreator, 
                                  _rankProfile, _allocator.get(), 
                                  _errorResultPtr, _pool);
    string query = "query=mp3";
    RequestPtr requestPtr = RequestCreator::prepareRequest(query);
    ASSERT_TRUE(creator.init(requestPtr.get()));
    ASSERT_TRUE(creator.getRankExpression());
    const std::vector<SortExpressionVector>& rankSortExpressions
        = creator.getRankSortExpressions();
    ASSERT_EQ((size_t)1, rankSortExpressions.size());
    ASSERT_EQ((size_t)1, rankSortExpressions[0].size());
    ASSERT_TRUE(creator.getRankExpression() == 
                   rankSortExpressions[0][0]->getAttributeExpression());
    ASSERT_EQ(false, rankSortExpressions[0][0]->getSortFlag());
}

TEST_F(SortExpressionCreatorTest, testCreateWithSort) {
    HA3_LOG(DEBUG, "Begin Test!");
    SortExpressionCreator creator(_attrExprCreator, 
                                  _rankProfile, _allocator.get(), 
                                  _errorResultPtr, _pool);
    string query = "query=mp3&&sort=+RANK;+uid1";
    map<string, ExprResultType> typeInfo;
    typeInfo["uid1"] = vt_int32;
    RequestPtr requestPtr = prepareRequest(query, typeInfo);
    
    ASSERT_TRUE(creator.init(requestPtr.get()));
    ASSERT_TRUE(creator.getRankExpression());
    const std::vector<SortExpressionVector>& rankSortExpressions
        = creator.getRankSortExpressions();
    ASSERT_EQ((size_t)1, rankSortExpressions.size());
    ASSERT_EQ((size_t)2, rankSortExpressions[0].size());
    ASSERT_TRUE(creator.getRankExpression() == 
                   rankSortExpressions[0][0]->getAttributeExpression());
    ASSERT_EQ(true, rankSortExpressions[0][0]->getSortFlag());
    ASSERT_EQ(true, rankSortExpressions[0][1]->getSortFlag());
}

TEST_F(SortExpressionCreatorTest, testCreateWithRankSort) {
    HA3_LOG(DEBUG, "Begin Test!");
    SortExpressionCreator creator(_attrExprCreator, 
                                  _rankProfile, _allocator.get(), 
                                  _errorResultPtr, _pool);
    string query = "query=phrase:with"
                   "&&rank_sort=sort:+uid,percent:10;sort:uid1,percent:60;sort:uid2#uid3,percent:30";
    map<string, ExprResultType> typeInfo;
    typeInfo["uid"] = vt_int32;
    typeInfo["uid1"] = vt_int32;
    typeInfo["uid2"] = vt_int32;
    typeInfo["uid3"] = vt_int32;
    RequestPtr requestPtr = prepareRequest(query, typeInfo);
    
    ASSERT_TRUE(creator.init(requestPtr.get()));
    const std::vector<SortExpressionVector>& rankSortExpressions
        = creator.getRankSortExpressions();
    ASSERT_EQ((size_t)3, rankSortExpressions.size());
    ASSERT_EQ((size_t)1, rankSortExpressions[0].size());
    ASSERT_EQ((size_t)1, rankSortExpressions[1].size());
    ASSERT_EQ((size_t)2, rankSortExpressions[2].size());
    ASSERT_TRUE(creator.getRankExpression() == NULL);
    ASSERT_EQ(true, rankSortExpressions[0][0]->getSortFlag());
    ASSERT_EQ(false, rankSortExpressions[1][0]->getSortFlag());
    ASSERT_EQ(false, rankSortExpressions[2][0]->getSortFlag());
    ASSERT_EQ(false, rankSortExpressions[2][1]->getSortFlag());
}

TEST_F(SortExpressionCreatorTest, testCreateWithDifferentSortType) {
    HA3_LOG(DEBUG, "Begin Test!");
    SortExpressionCreator creator(_attrExprCreator, 
                                  _rankProfile, _allocator.get(), 
                                  _errorResultPtr, _pool);
    string query = "query=phrase:with"
                   "&&rank_sort=sort:+RANK#+uid,percent:50;sort:-RANK#-uid,percent:50;";
    map<string, ExprResultType> typeInfo;
    typeInfo["uid"] = vt_int32;
    typeInfo["uid1"] = vt_int32;
    typeInfo["uid2"] = vt_int32;
    typeInfo["uid3"] = vt_int32;
    RequestPtr requestPtr = prepareRequest(query, typeInfo);
    
    ASSERT_TRUE(creator.init(requestPtr.get()));
    ASSERT_TRUE(creator.getRankExpression());
    const std::vector<SortExpressionVector>& rankSortExpressions
        = creator.getRankSortExpressions();
    ASSERT_EQ((size_t)2, rankSortExpressions.size());
    ASSERT_EQ((size_t)2, rankSortExpressions[0].size());
    ASSERT_EQ((size_t)2, rankSortExpressions[1].size());
    ASSERT_TRUE(creator.getRankExpression() == 
                   rankSortExpressions[0][0]->getAttributeExpression());
    ASSERT_EQ(true, rankSortExpressions[0][0]->getSortFlag());
    ASSERT_EQ(true, rankSortExpressions[0][1]->getSortFlag());
    ASSERT_EQ(false, rankSortExpressions[1][0]->getSortFlag());
    ASSERT_EQ(false, rankSortExpressions[1][1]->getSortFlag());

    ASSERT_TRUE(rankSortExpressions[0][0]->getAttributeExpression() ==
                   rankSortExpressions[1][0]->getAttributeExpression());
    ASSERT_TRUE(rankSortExpressions[0][1]->getAttributeExpression() ==
                   rankSortExpressions[1][1]->getAttributeExpression());
}

RequestPtr SortExpressionCreatorTest::prepareRequest(string &query, 
        const map<string, ExprResultType> &typeInfo) 
{
    RequestPtr requestPtr = RequestCreator::prepareRequest(query);
    SortClause *sortClause = requestPtr->getSortClause();
    RankSortClause *rankSortClause = requestPtr->getRankSortClause();
    if (sortClause) {
        uint32_t descCount = sortClause->getSortDescriptions().size();
        for (uint32_t i = 0; i < descCount; i++) {
            SortDescription *desc = sortClause->getSortDescription(i);
            if (desc->isRankExpression()) {
                continue;
            }
            SyntaxExpr *syntaxExp = desc->getRootSyntaxExpr();
            const ExprResultType &type = typeInfo.at(syntaxExp->getExprString());
            syntaxExp->setExprResultType(type);
        }
    }
    if (rankSortClause) {
        uint32_t rankSortDescCount = rankSortClause->getRankSortDescCount();
        for (uint32_t i = 0; i < rankSortDescCount; i++) {
            const RankSortDescription *rankSortDesc = rankSortClause->getRankSortDesc(i);
            uint32_t sortDescsCount = rankSortDesc->getSortDescCount();
            for (uint32_t j = 0; j < sortDescsCount; j++) {
                const SortDescription *desc = rankSortDesc->getSortDescription(j);
                if (desc->isRankExpression()) {
                    continue;
                }
                SyntaxExpr *syntaxExp = desc->getRootSyntaxExpr();
                const ExprResultType &type = typeInfo.at(syntaxExp->getExprString());
                syntaxExp->setExprResultType(type);
            }
        }
    }
    return requestPtr;
}

END_HA3_NAMESPACE(search);

