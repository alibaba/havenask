#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/MatchDataManager.h>
#include <ha3/search/test/SearcherTestHelper.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <autil/StringUtil.h>
#include <autil/StringTokenizer.h>
#include <ha3/search/QueryExecutorCreator.h>
#include <autil/mem_pool/Pool.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/search/test/QueryExecutorTestHelper.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(index);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(rank);

BEGIN_HA3_NAMESPACE(search);

class MatchDataManagerTest : public TESTBASE {
public:
    MatchDataManagerTest();
    ~MatchDataManagerTest();
public:
    void setUp();
    void tearDown();
protected:
    enum CheckType {
        CT_MatchData,
        CT_SimpleMatchData,
        CT_SubSimpleMatchData,
        CT_MatchValues,
    };
    void internalTest(index::FakeIndex &fakeIndex,
                      const std::string &queryStr,
                      const std::string &metaInfoStr,
                      const std::string &matchDataStr,
                      uint32_t layerCount,
                      CheckType checkType = CT_MatchData);
    void checkMetaInfo(const rank::MetaInfo &metaInfo, const std::string &metaInfoStr);
    void checkMatchData(MatchDataManager &manager,
                        const std::string &matchDataStr, uint32_t layer);
    void checkMatchValues(MatchDataManager &manager,
                        const std::string &matchValueStr, uint32_t layer);
    void checkSimpleMatchData(MatchDataManager &manager,
                              const std::string &matchDataStr, uint32_t layer);
    void checkSubSimpleMatchData(MatchDataManager &manager,
                                 const std::string &matchDataStr, uint32_t layer);
protected:
    vector<QueryExecutor *> _queryExecutors;
    autil::mem_pool::Pool _pool;
    common::Ha3MatchDocAllocatorPtr _allocator;
    common::Ha3MatchDocAllocatorPtr _subAllocator;
    IndexPartitionReaderWrapperPtr _indexPartReaderPtr;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, MatchDataManagerTest);


MatchDataManagerTest::MatchDataManagerTest() {
}

MatchDataManagerTest::~MatchDataManagerTest() {
}

void MatchDataManagerTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
    _allocator.reset(new common::Ha3MatchDocAllocator(&_pool));
    _subAllocator.reset(new common::Ha3MatchDocAllocator(&_pool, true));
}

void MatchDataManagerTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
    for (auto q: _queryExecutors) {
        POOL_DELETE_CLASS(q);
    }
    _allocator.reset();
    _subAllocator.reset();
}

TEST_F(MatchDataManagerTest, testMatchData) {
    HA3_LOG(DEBUG, "Begin Test!");
    FakeIndex fakeIndex;
    fakeIndex.indexes["default"] = "a:1;2;3;4\n"
                                   "b:1;3;4\n"
                                   "c:2;3;5\n";
    string queryStr = "a OR b OR c";
    string metaInfoStr = "default:a;default:b;default:c";
    string matchDataStr = "1:1,1,0;2:1,0,1;3:1,1,1;4:1,1,0;5:0,0,1";
    internalTest(fakeIndex, queryStr, metaInfoStr, matchDataStr, 1, CT_MatchData);
}

TEST_F(MatchDataManagerTest, testMatchValues) {
    HA3_LOG(DEBUG, "Begin Test!");
    FakeIndex fakeIndex;
    fakeIndex.indexes["default"] = "a:1^101;2^102;3^103;4^104\n"
                                   "b:1^201;3^203;4^204\n"
                                   "c:2^302;3^303;5^305\n";
    string queryStr = "a OR b OR c";
    string metaInfoStr = "default:a;default:b;default:c";
    //string matchDataStr = "1:1,1,0;2:1,0,1;3:1,1,1;4:1,1,0;5:0,0,1";
    string matchValueStr = "1:101,201,0;2:102,0,302;3:103,203,303;4:104,204,0;5:0,0,305";
    internalTest(fakeIndex, queryStr, metaInfoStr, matchValueStr, 1, CT_MatchValues);
}

TEST_F(MatchDataManagerTest, testSimpleMatchData) {
    HA3_LOG(DEBUG, "Begin Test!");
    FakeIndex fakeIndex;
    fakeIndex.indexes["default"] = "a:1;2;3;4\n"
                                   "b:1;3;4\n"
                                   "c:2;3;5\n";
    string queryStr = "a OR b OR c";
    string metaInfoStr = "default:a;default:b;default:c";
    string matchDataStr = "1:1,1,0;2:1,0,1;3:1,1,1;4:1,1,0;5:0,0,1";
    internalTest(fakeIndex, queryStr, metaInfoStr, matchDataStr, 1, CT_SimpleMatchData);
}

TEST_F(MatchDataManagerTest, testMultiQ) {
    HA3_LOG(DEBUG, "Begin Test!");
    FakeIndex fakeIndex;
    fakeIndex.indexes["default"] = "a:1;2;3;4;6\n"
                                   "b:1;3;4;\n"
                                   "c:2;3;5;6\n";
    string queryStr = "a OR b; c";
    string metaInfoStr = "default:a;default:b;default:c";
    string matchDataStr = "1:1,1,0;2:1,0,0|3:0,0,1;5:0,0,1|6:1,0,0";
    internalTest(fakeIndex, queryStr, metaInfoStr, matchDataStr, 3, CT_MatchData);
}

TEST_F(MatchDataManagerTest, testSimpleMultiQ) {
    HA3_LOG(DEBUG, "Begin Test!");
    FakeIndex fakeIndex;
    fakeIndex.indexes["default"] = "a:1;2;3;4;6\n"
                                   "b:1;3;4;\n"
                                   "c:2;3;5;6\n";
    string queryStr = "a OR b; c";
    string metaInfoStr = "default:a;default:b;default:c";
    string matchDataStr = "1:1,1,0;2:1,0,0|3:0,0,1;5:0,0,1|6:1,0,0";
    internalTest(fakeIndex, queryStr, metaInfoStr, matchDataStr, 3, CT_SimpleMatchData);
}

TEST_F(MatchDataManagerTest, testRequireSubSimpleMatchData) {
    FakeIndex fakeIndex;
    fakeIndex.tablePrefix.push_back("sub_");
    fakeIndex.indexes["sub_index_name"] = "term:0;1; 4; 11;13; 17;\n";
    fakeIndex.indexes["index_name"] = "term:1;2;\n";
    QueryExecutorTestHelper::addSubDocAttrMap(fakeIndex, "3,5,8,18");
    string queryStr = "sub_index_name:term OR index_name:term";
    string metaInfoStr = "sub_index_name:term;index_name:term";
    string matchDataStr = "0-0:1,0;0-1:1,0;1-3:0,1;1-4:1,1;2-5:0,1;2-6:0,1;2-7:0,1;3-11:1,0;3-13:1,0;3-17:1,0";
    internalTest(fakeIndex, queryStr, metaInfoStr, matchDataStr, 1, CT_SubSimpleMatchData);
}

TEST_F(MatchDataManagerTest, testRequireSubSimpleMatchDataAndMainDoc) {
    FakeIndex fakeIndex;
    fakeIndex.tablePrefix.push_back("sub_");
    fakeIndex.indexes["sub_index_name"] = "term:0;1; 4; 7;8; 13;17;\n";
    fakeIndex.indexes["index_name"] = "term:1;2;\n";
    fakeIndex.indexes["title"] = "iphone:2;3;\n";
    QueryExecutorTestHelper::addSubDocAttrMap(fakeIndex, "3,5,8,18");
    string queryStr = "index_name:term AND title:iphone";
    string metaInfoStr = "index_name:term;title:iphone";
    string matchDataStr = "2-5:1,1;2-6:1,1;2-7:1,1;";
    internalTest(fakeIndex, queryStr, metaInfoStr, matchDataStr, 1, CT_SubSimpleMatchData);
}

TEST_F(MatchDataManagerTest, testRequireSubSimpleMatchDataOrMainDoc) {
    FakeIndex fakeIndex;
    fakeIndex.tablePrefix.push_back("sub_");
    fakeIndex.indexes["sub_index_name"] = "term:0;1; 4; 7;8; 13;17;\n";
    fakeIndex.indexes["index_name"] = "term:1;2;\n";
    fakeIndex.indexes["title"] = "iphone:2;\n";
    QueryExecutorTestHelper::addSubDocAttrMap(fakeIndex, "3,5,8,18");
    string queryStr = "index_name:term OR title:iphone";
    string metaInfoStr = "index_name:term;title:iphone";
    string matchDataStr = "1-3:1,0;1-4:1,0;2-5:1,1;2-6:1,1;2-7:1,1;";
    internalTest(fakeIndex, queryStr, metaInfoStr, matchDataStr, 1, CT_SubSimpleMatchData);
}

TEST_F(MatchDataManagerTest, testRequireSubSimpleMatchDataRankMainDoc) {
    FakeIndex fakeIndex;
    fakeIndex.tablePrefix.push_back("sub_");
    fakeIndex.indexes["sub_index_name"] = "term:0;1; 4; 7;8; 13;17;\n";
    fakeIndex.indexes["index_name"] = "term:1;2;\n";
    fakeIndex.indexes["title"] = "iphone:2,3;\n";
    QueryExecutorTestHelper::addSubDocAttrMap(fakeIndex, "3,5,8,18");
    string queryStr = "index_name:term RANK title:iphone";
    string metaInfoStr = "index_name:term;title:iphone";
    string matchDataStr = "1-3:1,0;1-4:1,0;2-5:1,1;2-6:1,1;2-7:1,1;";
    internalTest(fakeIndex, queryStr, metaInfoStr, matchDataStr, 1, CT_SubSimpleMatchData);
}

TEST_F(MatchDataManagerTest, testRequireSubSimpleMatchDataAndnotMainDoc) {
    FakeIndex fakeIndex;
    fakeIndex.tablePrefix.push_back("sub_");
    fakeIndex.indexes["sub_index_name"] = "term:0;1; 4; 7;8; 13;17;\n";
    fakeIndex.indexes["index_name"] = "term:1;2;\n";
    fakeIndex.indexes["title"] = "iphone:2,3;\n";
    QueryExecutorTestHelper::addSubDocAttrMap(fakeIndex, "3,5,8,18");
    string queryStr = "index_name:term ANDNOT title:iphone";
    string metaInfoStr = "index_name:term";
    string matchDataStr = "1-3:1;1-4:1;";
    internalTest(fakeIndex, queryStr, metaInfoStr, matchDataStr, 1, CT_SubSimpleMatchData);
}

TEST_F(MatchDataManagerTest, testRankRequireSubSimpleMatchData1) {
    FakeIndex fakeIndex;
    fakeIndex.tablePrefix.push_back("sub_");
    fakeIndex.indexes["sub_index_name"] = "term:0;1; 4; 11;13; 17;\n";
    fakeIndex.indexes["index_name"] = "term:1;2;\n";
    QueryExecutorTestHelper::addSubDocAttrMap(fakeIndex, "3,5,8,18");
    string queryStr = "sub_index_name:term RANK index_name:term";
    string metaInfoStr = "sub_index_name:term;index_name:term";
    string matchDataStr = "0-0:1,0;0-1:1,0;1-4:1,1;3-11:1,0;3-13:1,0;3-17:1,0";
    internalTest(fakeIndex, queryStr, metaInfoStr, matchDataStr, 1, CT_SubSimpleMatchData);
}

TEST_F(MatchDataManagerTest, testRankRequireSubSimpleMatchData2) {
    FakeIndex fakeIndex;
    fakeIndex.tablePrefix.push_back("sub_");
    fakeIndex.indexes["sub_index_name"] = "term:0;1; 4; 11;13; 17;\n";
    fakeIndex.indexes["index_name"] = "term:1;2;\n";
    QueryExecutorTestHelper::addSubDocAttrMap(fakeIndex, "3,5,8,18");
    string queryStr = "index_name:term RANK sub_index_name:term";
    string metaInfoStr = "index_name:term;sub_index_name:term";
    string matchDataStr = "1-3:1,0;1-4:1,1;2-5:1,0;2-6:1,0;2-7:1,0";
    internalTest(fakeIndex, queryStr, metaInfoStr, matchDataStr, 1, CT_SubSimpleMatchData);
}

TEST_F(MatchDataManagerTest, testSubSimpleMultiQ) {
    FakeIndex fakeIndex;
    fakeIndex.tablePrefix.push_back("sub_");
    fakeIndex.indexes["sub_index_name"] = "term:0;1; 4; 7; 13;17;\n";
    fakeIndex.indexes["index_name"] = "term:1;2;\n";
    QueryExecutorTestHelper::addSubDocAttrMap(fakeIndex, "3,5,8,18");
    string queryStr = "sub_index_name:term OR index_name:term; sub_index_name:term AND index_name:term";
    string metaInfoStr = "sub_index_name:term;index_name:term;sub_index_name:term;index_name:term";
    string matchDataStr = "0-0:1,0,0,0;0-1:1,0,0,0;1-3:0,1,0,0;1-4:1,1,0,0|2-7:0,0,1,1";
    internalTest(fakeIndex, queryStr, metaInfoStr, matchDataStr, 2, CT_SubSimpleMatchData);
}

TEST_F(MatchDataManagerTest, testRequireFail) {
    {
        MatchDataManager manager;
        ASSERT_TRUE(manager.requireSimpleMatchData(_allocator.get(),
                        SIMPLE_MATCH_DATA_REF, SUB_DOC_DISPLAY_NO, &_pool) != NULL);
    }
    {
        MatchDataManager manager;
        ASSERT_TRUE(manager.requireSimpleMatchData(_allocator.get(),
                        SIMPLE_MATCH_DATA_REF, SUB_DOC_DISPLAY_GROUP, &_pool) != NULL);
    }
    {
        MatchDataManager manager;
        ASSERT_TRUE(manager.requireSimpleMatchData(_allocator.get(),
                        SIMPLE_MATCH_DATA_REF, SUB_DOC_DISPLAY_FLAT, &_pool) == NULL);
    }
    // matchdata
    {
        MatchDataManager manager;
        ASSERT_TRUE(manager.requireMatchData(_allocator.get(),
                        MATCH_DATA_REF, SUB_DOC_DISPLAY_NO, &_pool) != NULL);
    }
    {
        MatchDataManager manager;
        ASSERT_TRUE(manager.requireMatchData(_allocator.get(),
                        MATCH_DATA_REF, SUB_DOC_DISPLAY_GROUP, &_pool) != NULL);
    }
    {
        MatchDataManager manager;
        ASSERT_TRUE(manager.requireMatchData(_allocator.get(),
                        MATCH_DATA_REF, SUB_DOC_DISPLAY_FLAT, &_pool) == NULL);
    }
    // sub simple
    {
        MatchDataManager manager;
        ASSERT_TRUE(manager.requireSubSimpleMatchData(_subAllocator.get(),
                        SUB_SIMPLE_MATCH_DATA_REF, SUB_DOC_DISPLAY_NO, &_pool) == NULL);
    }
    {
        MatchDataManager manager;
        ASSERT_TRUE(manager.requireSubSimpleMatchData(_subAllocator.get(),
                        SUB_SIMPLE_MATCH_DATA_REF, SUB_DOC_DISPLAY_GROUP, &_pool) != NULL);
    }
    {
        MatchDataManager manager;
        ASSERT_TRUE(manager.requireSubSimpleMatchData(_subAllocator.get(),
                        SUB_SIMPLE_MATCH_DATA_REF, SUB_DOC_DISPLAY_FLAT, &_pool) == NULL);
    }
    // sub and 
    {
        MatchDataManager manager;
        ASSERT_TRUE(manager.requireMatchData(_allocator.get(),
                        MATCH_DATA_REF, SUB_DOC_DISPLAY_NO, &_pool) != NULL);
        ASSERT_TRUE(manager.requireSubSimpleMatchData(_subAllocator.get(),
                        SUB_SIMPLE_MATCH_DATA_REF, SUB_DOC_DISPLAY_GROUP, &_pool) != NULL);
    }
    {
        MatchDataManager manager;
        ASSERT_TRUE(manager.requireSimpleMatchData(_allocator.get(),
                        SIMPLE_MATCH_DATA_REF, SUB_DOC_DISPLAY_NO, &_pool) != NULL);
        ASSERT_TRUE(manager.requireSubSimpleMatchData(_subAllocator.get(),
                        SUB_SIMPLE_MATCH_DATA_REF, SUB_DOC_DISPLAY_GROUP, &_pool) != NULL);
    }
    // re require
    {
        MatchDataManager manager;
        ASSERT_TRUE(manager.requireSimpleMatchData(_allocator.get(),
                        SIMPLE_MATCH_DATA_REF, SUB_DOC_DISPLAY_NO, &_pool) != NULL);
        ASSERT_TRUE(manager.requireSimpleMatchData(_allocator.get(),
                        SIMPLE_MATCH_DATA_REF, SUB_DOC_DISPLAY_NO, &_pool) == NULL);
    }
    {
        MatchDataManager manager;
        ASSERT_TRUE(manager.requireMatchData(_allocator.get(),
                        MATCH_DATA_REF, SUB_DOC_DISPLAY_NO, &_pool) != NULL);
        ASSERT_TRUE(manager.requireSimpleMatchData(_allocator.get(),
                        SIMPLE_MATCH_DATA_REF, SUB_DOC_DISPLAY_NO, &_pool) == NULL);
    }
}

TEST_F(MatchDataManagerTest, testRequireMatchValues) {
    {
        MatchDataManager manager;
        ASSERT_TRUE(manager.requireMatchValues(_allocator.get(),
                        MATCH_VALUE_REF, SUB_DOC_DISPLAY_NO, &_pool) != NULL);
    }

    {
        MatchDataManager manager;
        ASSERT_TRUE(manager.requireMatchValues(_allocator.get(),
                        MATCH_VALUE_REF, SUB_DOC_DISPLAY_GROUP, &_pool) != NULL);
    }

    {
        MatchDataManager manager;
        ASSERT_TRUE(manager.requireMatchValues(_allocator.get(),
                        MATCH_VALUE_REF, SUB_DOC_DISPLAY_FLAT, &_pool) == NULL);
    }
}


void MatchDataManagerTest::internalTest(FakeIndex &fakeIndex,
                                        const string &queryStr,
                                        const string &metaInfoStr,
                                        const string &matchDataStr,
                                        uint32_t layerCount,
                                        CheckType checkType)
{
    const auto &queryStrs = StringUtil::split(queryStr, ";");
    _indexPartReaderPtr = FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    _indexPartReaderPtr->setTopK(1);
    vector<QueryPtr> querys;
    for (auto q : queryStrs) {
        querys.push_back(QueryPtr(SearcherTestHelper::createQuery(q)));
    }

    MatchDataManager manager;
    for (auto q : querys) {
        QueryExecutorCreator creator(&manager, _indexPartReaderPtr.get(), &_pool);
        q->accept(&creator);
        _queryExecutors.push_back(creator.stealQuery());
    }
    for (uint32_t i = queryStrs.size(); i < layerCount; i++) {
        QueryExecutorCreator creator(&manager, _indexPartReaderPtr.get(), &_pool);
        querys[0]->accept(&creator);
        _queryExecutors.push_back(creator.stealQuery());
    }
    manager.setQueryCount(queryStrs.size());

    MetaInfo metaInfo;
    manager.getQueryTermMetaInfo(&metaInfo);
    checkMetaInfo(metaInfo, metaInfoStr);
    const auto &matchDataStrs = StringUtil::split(matchDataStr, "|");
    for (uint32_t i = 0; i < matchDataStrs.size(); ++i) {
        switch (checkType) {
        case CT_MatchData:
            checkMatchData(manager, matchDataStrs[i], i);
            break;
        case CT_SimpleMatchData:
            checkSimpleMatchData(manager, matchDataStrs[i], i);
            break;
        case CT_SubSimpleMatchData:
            checkSubSimpleMatchData(manager, matchDataStrs[i], i);
            break;
        case CT_MatchValues:
            checkMatchValues(manager, matchDataStrs[i], i);
            break;
        default:
            break;
        }
    }
}

//metaInfoStr: indexname:termStr;indexname:termStr
void MatchDataManagerTest::checkMetaInfo(const MetaInfo &metaInfo, const string &metaInfoStr) {
    StringTokenizer st(metaInfoStr, ";", StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    ASSERT_EQ((size_t)metaInfo.size(), st.getNumTokens());

    for (uint32_t i = 0; i < metaInfo.size(); ++i) {
        StringTokenizer st1(st[i], ":", StringTokenizer::TOKEN_IGNORE_EMPTY
                            | StringTokenizer::TOKEN_TRIM);
        assert(st1.getNumTokens() == 2);
        ASSERT_EQ(metaInfo[i].getIndexName(), st1[0]);
        ASSERT_EQ(string(metaInfo[i].getTermText().c_str()), st1[1]);
    }
}

// matchDataStr docid:1,0;docid:1,0
void MatchDataManagerTest::checkMatchData(MatchDataManager &manager, const string &matchDataStr, uint32_t layer) {
    if (!manager._fetcher) {
        ASSERT_TRUE(manager.requireMatchData(_allocator.get(),
                        MATCH_DATA_REF, SUB_DOC_DISPLAY_NO, &_pool) != NULL);
    }
    matchdoc::Reference<MatchData> *ref =
        _allocator->findReference<MatchData>(MATCH_DATA_REF);
    StringTokenizer st(matchDataStr, ";", StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    manager.moveToLayer(layer);
    assert(layer < _queryExecutors.size());
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        pos_t pos = st[i].find(":");
        docid_t docId = StringUtil::fromString<docid_t>(st[i].substr(0,pos).c_str());
        StringTokenizer st1(st[i].substr(pos + 1), ",",
                           StringTokenizer::TOKEN_IGNORE_EMPTY
                           | StringTokenizer::TOKEN_TRIM);
        matchdoc::MatchDoc matchDoc = _allocator->allocate(docId);
        ASSERT_EQ(st1.getNumTokens(), manager._fetcher->_termCount);
        ASSERT_TRUE(!_queryExecutors[layer]->hasSubDocExecutor());
        ASSERT_EQ(docId, _queryExecutors[layer]->legacySeek(docId));
        manager.fillMatchData(matchDoc);
        MatchData &matchData = ref->getReference(matchDoc);
        for (size_t j = 0; j < st1.getNumTokens(); ++j) {
            bool match = st1[j] == "1";
            ASSERT_TRUE(matchData.getTermMatchData(j).isMatched() == match);
        }
        _allocator->deallocate(matchDoc);
    }
}

void MatchDataManagerTest::checkMatchValues(MatchDataManager &manager, const string &matchValueStr, uint32_t layer) {
    if (!manager._matchValuesFetcher) {
        ASSERT_TRUE(manager.requireMatchValues(_allocator.get(),
                        MATCH_VALUE_REF, SUB_DOC_DISPLAY_NO, &_pool) != NULL);
    }
    matchdoc::Reference<Ha3MatchValues> *ref =
        _allocator->findReference<Ha3MatchValues>(MATCH_VALUE_REF);
    StringTokenizer st(matchValueStr, ";", StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    manager.moveToLayer(layer);
    assert(layer < _queryExecutors.size());
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        pos_t pos = st[i].find(":");
        docid_t docId = StringUtil::fromString<docid_t>(st[i].substr(0,pos).c_str());
        StringTokenizer st1(st[i].substr(pos + 1), ",",
                           StringTokenizer::TOKEN_IGNORE_EMPTY
                           | StringTokenizer::TOKEN_TRIM);
        matchdoc::MatchDoc matchDoc = _allocator->allocate(docId);
        ASSERT_EQ(st1.getNumTokens(), manager._matchValuesFetcher->_termCount);
        ASSERT_TRUE(!_queryExecutors[layer]->hasSubDocExecutor());
        ASSERT_EQ(docId, _queryExecutors[layer]->legacySeek(docId));
        manager.fillMatchValues(matchDoc);
        Ha3MatchValues &matchValues = ref->getReference(matchDoc);
        for (size_t j = 0; j < st1.getNumTokens(); ++j) {
            matchvalue_t mvt;
            mvt.SetUint16(StringUtil::fromString<uint16_t>(st1[j]));
            ASSERT_EQ(mvt.GetUint16(), matchValues.getTermMatchValue(j).GetUint16());
        }
        _allocator->deallocate(matchDoc);
    }
}

// matchDataStr docid:1,0;docid:1,0
void MatchDataManagerTest::checkSimpleMatchData(MatchDataManager &manager,
        const string &matchDataStr, uint32_t layer)
{
    if (!manager._fetcher) {
        ASSERT_TRUE(manager.requireSimpleMatchData(_allocator.get(),
                        SIMPLE_MATCH_DATA_REF, SUB_DOC_DISPLAY_NO, &_pool) != NULL);
    }
    matchdoc::Reference<SimpleMatchData> *ref =
        _allocator->findReference<SimpleMatchData>(SIMPLE_MATCH_DATA_REF);
    StringTokenizer st(matchDataStr, ";", StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    manager.moveToLayer(layer);
    assert(layer < _queryExecutors.size());
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        pos_t pos = st[i].find(":");
        docid_t docId = StringUtil::fromString<docid_t>(st[i].substr(0,pos).c_str());
        StringTokenizer st1(st[i].substr(pos + 1), ",",
                           StringTokenizer::TOKEN_IGNORE_EMPTY
                           | StringTokenizer::TOKEN_TRIM);
        matchdoc::MatchDoc matchDoc = _allocator->allocate(docId);
        ASSERT_EQ(st1.getNumTokens(), manager._fetcher->_termCount);
        ASSERT_TRUE(!_queryExecutors[layer]->hasSubDocExecutor());
        ASSERT_EQ(docId, _queryExecutors[layer]->legacySeek(docId));
        manager.fillMatchData(matchDoc);
        SimpleMatchData &matchData = ref->getReference(matchDoc);
        for (size_t j = 0; j < st1.getNumTokens(); ++j) {
            bool match = st1[j] == "1";
            ASSERT_TRUE(matchData.isMatch(j) == match);
        }
    }
}

void MatchDataManagerTest::checkSubSimpleMatchData(MatchDataManager &manager,
        const string &matchDataStr, uint32_t layer)
{
    if (!manager._subFetcher) {
        ASSERT_TRUE(manager.requireSubSimpleMatchData(_subAllocator.get(),
                        SUB_SIMPLE_MATCH_DATA_REF, SUB_DOC_DISPLAY_GROUP, &_pool) != NULL);
    }
    matchdoc::Reference<SimpleMatchData> *ref =
        _subAllocator->findReference<SimpleMatchData>(SUB_SIMPLE_MATCH_DATA_REF);
    StringTokenizer st(matchDataStr, ";", StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    manager.moveToLayer(layer);
    assert(layer < _queryExecutors.size());
    auto mainToSubIter = _indexPartReaderPtr->getMainToSubIter();
    docid_t curSubDocId = 0;
    docid_t lastDocId = 0;
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        pos_t pos = st[i].find(":");
        std::string docIdsStr = st[i].substr(0,pos);
        const auto &docIds = StringUtil::split(docIdsStr, "-");
        ASSERT_EQ(docIds.size(), 2);
        docid_t docId = StringUtil::fromString<docid_t>(docIds[0]);
        docid_t subDocId = StringUtil::fromString<docid_t>(docIds[1]);
        StringTokenizer st1(st[i].substr(pos + 1), ",",
                           StringTokenizer::TOKEN_IGNORE_EMPTY
                           | StringTokenizer::TOKEN_TRIM);
        matchdoc::MatchDoc matchDoc = _subAllocator->allocate(docId);
        ASSERT_EQ(st1.getNumTokens(), manager._subFetcher->_termCount);
        ASSERT_EQ(docId, _queryExecutors[layer]->legacySeek(docId));
        if (docId != lastDocId) {
            mainToSubIter->Seek(docId - 1, curSubDocId);
            lastDocId = docId;
        }
        docid_t endSubDocId;
        mainToSubIter->Seek(docId, endSubDocId);
        ASSERT_EQ(subDocId, _queryExecutors[layer]->legacySeekSub(docId, curSubDocId, endSubDocId, true));
        auto subSlab = _subAllocator->allocateSub(matchDoc, subDocId);
        manager.fillSubMatchData(matchDoc, subSlab, subDocId, endSubDocId);
        SimpleMatchData &matchData = ref->getReference(matchDoc);
        for (size_t j = 0; j < st1.getNumTokens(); ++j) {
            bool match = st1[j] == "1";
            ASSERT_TRUE(matchData.isMatch(j) == match);
        }
        curSubDocId = subDocId + 1;
    }
}

END_HA3_NAMESPACE(search);
