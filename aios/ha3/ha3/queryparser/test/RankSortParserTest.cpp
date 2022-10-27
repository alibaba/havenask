#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/queryparser/ClauseParserContext.h>

using namespace std;

USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(queryparser);

class RankSortParserTest : public TESTBASE {
public:
    typedef common::SortDescription::RankSortType RankSortType;
    struct SortDescInfo {
        std::string sortFieldName;
        bool isAsc;
        RankSortType type;
        SortDescInfo(const std::string &aFieldName, 
                     bool aAsc,
                     RankSortType aType)
            : sortFieldName(aFieldName)
            , isAsc(aAsc)
            , type(aType)
        {
            
        }
    };
    typedef std::pair<std::vector<SortDescInfo>, float> RankSortInfo;
public:
    RankSortParserTest();
    ~RankSortParserTest();
public:
    void setUp();
    void tearDown();
protected:
    void checkRankSortClause(const common::RankSortClausePtr &clausePtr, 
                             const std::vector<RankSortInfo> &rankSortInfos);
protected:
    ClauseParserContext *_ctx;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(queryparser, RankSortParserTest);


RankSortParserTest::RankSortParserTest() { 
}

RankSortParserTest::~RankSortParserTest() { 
}

void RankSortParserTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
    _ctx = new ClauseParserContext;
}

void RankSortParserTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
    if (_ctx) {
        delete _ctx;
    }
}

TEST_F(RankSortParserTest, testRankSortClauseSimple) {
    vector<pair<string,bool> > querys;
    querys.push_back(make_pair(string("sort:RANK,percent:1"), true));
    querys.push_back(make_pair(string("sort:+RANK,percent:1"), true));
    querys.push_back(make_pair(string("sort:-RANK,percent:1"), true));

    querys.push_back(make_pair(string("sort:abc(dd),percent:1"), true));
    querys.push_back(make_pair(string("sort:abc(dd,percent:1"), false));

    querys.push_back(make_pair(string("sort:abc(dd,ac)#dd(a)#a,percent:1"), true));
    querys.push_back(make_pair(string("sort:abc(dd,ac,)#dd(a)#a,percent:1"), false));
    querys.push_back(make_pair(string("sort:abc(dd,ac#)#dd(a)#a,percent:1"), false));
    querys.push_back(make_pair(string("sort:abc(dd,ac)#dd(,percent:1"), false));

    querys.push_back(make_pair(string("sort:-abc(dd,ac),percent:10"), true));
    querys.push_back(make_pair(string("sort:+abc(dd,ac),percent:10"), true));
    querys.push_back(make_pair(string("sort:abc(dd,ac),percent:abc"), false));

    querys.push_back(make_pair(string("sort:abc(dd,ac),percent:10;sort:abc(dd,ac)#RANK,percent:10"), true));
    querys.push_back(make_pair(string("sort:abc(dd,ac),percent:10;sort:abc(dd,ac,)#RANK,percent:10"), false));
    for (size_t i = 0; i < querys.size(); ++i) {
        ClauseParserContext ctx;
        ASSERT_TRUE(ctx.parseRankSortClause(querys[i].first.c_str()) == querys[i].second)<<querys[i].first;
    }
}

TEST_F(RankSortParserTest, testRankSortClause) { 
    HA3_LOG(DEBUG, "Begin Test!");

    bool ret = _ctx->parseRankSortClause("sort:+field1#-field2,percent:1;sort:field3#+RANK,percent:99");
    ASSERT_TRUE(ret);
    RankSortClausePtr clausePtr(_ctx->stealRankSortClause());
    ASSERT_TRUE(clausePtr);
  
    vector<RankSortInfo> expectrankSortInfos;
    {
        RankSortInfo rankSortInfo;
        rankSortInfo.first.push_back(SortDescInfo("field1", true, SortDescription::RS_NORMAL));
        rankSortInfo.first.push_back(SortDescInfo("field2", false, SortDescription::RS_NORMAL));
        rankSortInfo.second = 1;
        expectrankSortInfos.push_back(rankSortInfo);
    }
    {
        RankSortInfo rankSortInfo;
        rankSortInfo.first.push_back(SortDescInfo("field3", false, SortDescription::RS_NORMAL));
        rankSortInfo.first.push_back(SortDescInfo("RANK", true, SortDescription::RS_RANK));
        rankSortInfo.second = 99;
        expectrankSortInfos.push_back(rankSortInfo);
    }
    checkRankSortClause(clausePtr, expectrankSortInfos);

    ret = _ctx->parseRankSortClause("sort:+field1#-field2,percent:1;");
    ASSERT_TRUE(ret);
    clausePtr.reset(_ctx->stealRankSortClause());
    ASSERT_TRUE(clausePtr);
}

TEST_F(RankSortParserTest, testRankSortClauseWithFunction) { 
    HA3_LOG(DEBUG, "Begin Test!");

    bool ret = _ctx->parseRankSortClause("sort:+func(field,\"strArg\")#-func1(field)+func2(field, \"#,;:+-\"),percent:0.1;sort:func()#-func(field4),percent:99.9");
    ASSERT_TRUE(ret);
    RankSortClausePtr clausePtr(_ctx->stealRankSortClause());
    ASSERT_TRUE(clausePtr);
  
    vector<RankSortInfo> expectrankSortInfos;
    {
        RankSortInfo rankSortInfo;
        rankSortInfo.first.push_back(SortDescInfo("func(field , \"strArg\")", true, SortDescription::RS_NORMAL));
        rankSortInfo.first.push_back(SortDescInfo("(func1(field)+func2(field , \"#,;:+-\"))", false, SortDescription::RS_NORMAL));
        rankSortInfo.second = 0.1;
        expectrankSortInfos.push_back(rankSortInfo);
    }
    {
        RankSortInfo rankSortInfo;
        rankSortInfo.first.push_back(SortDescInfo("func()", false, SortDescription::RS_NORMAL));
        rankSortInfo.first.push_back(SortDescInfo("func(field4)", false, SortDescription::RS_NORMAL));
        rankSortInfo.second = 99.9;
        expectrankSortInfos.push_back(rankSortInfo);
    }
    checkRankSortClause(clausePtr, expectrankSortInfos);
}

TEST_F(RankSortParserTest, testRankSortClauseWithSpecialExpr) { 
    HA3_LOG(DEBUG, "Begin Test!");

    bool ret = _ctx->parseRankSortClause("sort:RANK#field1+field2#+field1-field3,percent:50.1;sort:field4,percent:49.9");
    ASSERT_TRUE(ret);
    RankSortClausePtr clausePtr(_ctx->stealRankSortClause());
    ASSERT_TRUE(clausePtr);
  
    vector<RankSortInfo> expectrankSortInfos;
    {
        RankSortInfo rankSortInfo;
        rankSortInfo.first.push_back(SortDescInfo("RANK", false, SortDescription::RS_RANK));
        rankSortInfo.first.push_back(SortDescInfo("(field1+field2)", false, SortDescription::RS_NORMAL));
        rankSortInfo.first.push_back(SortDescInfo("(field1-field3)", true, SortDescription::RS_NORMAL));
        rankSortInfo.second = 50.1;
        expectrankSortInfos.push_back(rankSortInfo);
    }
    {
        RankSortInfo rankSortInfo;
        rankSortInfo.first.push_back(SortDescInfo("field4", false, SortDescription::RS_NORMAL));
        rankSortInfo.second = 49.9;
        expectrankSortInfos.push_back(rankSortInfo);
    }
    checkRankSortClause(clausePtr, expectrankSortInfos);
}

void RankSortParserTest::checkRankSortClause(const RankSortClausePtr &clausePtr, 
        const vector<RankSortInfo> &rankSortInfos) {
    size_t rankSortDesCount = clausePtr->getRankSortDescCount();
    ASSERT_EQ(rankSortInfos.size(), rankSortDesCount);
    for (size_t i = 0; i < rankSortDesCount; i++) {
        const RankSortDescription *rankSortDesc = clausePtr->getRankSortDesc(i);
        size_t sortDescCount = rankSortDesc->getSortDescCount();
        const RankSortInfo &expectRankSortInfo = rankSortInfos[i];
        const vector<SortDescInfo>& expectSortDescInfos = expectRankSortInfo.first;
        ASSERT_EQ(expectRankSortInfo.second, rankSortDesc->getPercent());
        ASSERT_EQ(expectRankSortInfo.first.size(), sortDescCount);
        for (size_t j = 0; j < sortDescCount; j++) {
            const SortDescription* sortDesc = rankSortDesc->getSortDescription(j);
            ASSERT_TRUE(!expectSortDescInfos[j].sortFieldName.empty());
            ASSERT_EQ(expectSortDescInfos[j].sortFieldName, sortDesc->getRootSyntaxExpr()->getExprString());
            ASSERT_EQ(expectSortDescInfos[j].isAsc, sortDesc->getSortAscendFlag());
            ASSERT_EQ(expectSortDescInfos[j].type, sortDesc->getExpressionType());         
        }
    }
}

END_HA3_NAMESPACE(queryparser);

