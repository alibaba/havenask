#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/CheckMatchVisitor.h>
#include <autil/StringUtil.h>
#include <ha3/search/test/SearcherTestHelper.h>

using namespace std;
using namespace autil;
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(qrs);

class CheckMatchVisitorTest : public TESTBASE {
public:
    CheckMatchVisitorTest();
    ~CheckMatchVisitorTest();
public:
    void setUp();
    void tearDown();
protected:
    void internalQueryTest(const std::string& queryStr, 
                           const std::string& matchStr, 
                           const std::string& expectStr
                           );
    std::map<std::string, bool> createMatchInfo(const std::string& matchStr);
    CheckResultInfo createCheckResult(const std::string& resultStr);
    void isPhraseEqual(const std::map<std::string, std::string>& phraseMap);
protected:
                       std::string _defaultIndex;
                       std::map<std::string, std::string> _phraseMap; 
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(qrs, CheckMatchVisitorTest);


CheckMatchVisitorTest::CheckMatchVisitorTest() { 
}

CheckMatchVisitorTest::~CheckMatchVisitorTest() { 
}

void CheckMatchVisitorTest::setUp() { 
    _defaultIndex = "default";
    _phraseMap.clear();
}

void CheckMatchVisitorTest::tearDown() { 
    _phraseMap.clear();
}

TEST_F(CheckMatchVisitorTest, testPhraseQuery){
    {
        setUp();
        _phraseMap["aaa bbb"] = "default";
        internalQueryTest("\"aaa bbb\"", "default:aaa:1#default:bbb:1", "0###");
        tearDown();
    }
    {
        setUp();
        internalQueryTest("\"aaa bbb\"", "default:aaa:1#default:bbb:0", "1#TOKENIZE#bbb#default");
        tearDown();
    }
    {
        setUp();
        _phraseMap["aaa bbb"] = "default";
        _phraseMap["aaa ccc"] = "default";
        internalQueryTest("\"aaa bbb\" AND \"aaa ccc\"", "default:aaa:1#default:bbb:1#default:ccc:1", "0###");
        tearDown();
    }
}

TEST_F(CheckMatchVisitorTest, testTermQuery){
    internalQueryTest("aaa", "default:aaa:1", "0###");
    internalQueryTest("aaa", "default:aaa:0", "1#TOKENIZE#aaa#default");
    internalQueryTest("aaa", "", "1#TOKENIZE#aaa#default");
    internalQueryTest("noindex:aaa", "","1#TOKENIZE#aaa#noindex");
}

TEST_F(CheckMatchVisitorTest, testAndNotQuery){
    internalQueryTest("aaa ANDNOT bbb", "default:aaa:1#default:bbb:0", "0###");
    internalQueryTest("aaa ANDNOT bbb", "default:aaa:1#default:bbb:1", "1#NOT#bbb#default");
    internalQueryTest("aaa ANDNOT bbb", "default:bbb:1", "1#TOKENIZE#aaa#default");
    internalQueryTest("aaa ANDNOT bbb", "", "1#TOKENIZE#aaa#default");
}

TEST_F(CheckMatchVisitorTest, testNumberQuery){
    internalQueryTest("1", "default:1:1", "0###");
    internalQueryTest("default:1", "default:1:0", "1#TOKENIZE#1#default");
    internalQueryTest("1", "", "1#TOKENIZE#1#default");
    internalQueryTest("noindex:aaa", "","1#TOKENIZE#aaa#noindex");
}

TEST_F(CheckMatchVisitorTest, testANDQuery){
    internalQueryTest("aaa AND bbb", "default:aaa:1#default:bbb:1", "0###");
    internalQueryTest("aaa AND bbb", "default:aaa:1#default:bbb:0", "1#TOKENIZE#bbb#default");
    internalQueryTest("aaa AND noindex:bbb", "default:aaa:1", "1#TOKENIZE#bbb#noindex");
    internalQueryTest("aaa AND noindex:bbb", "", "1#TOKENIZE#aaa#default");
}

TEST_F(CheckMatchVisitorTest, testOrQuery){
    internalQueryTest("aaa OR bbb", "default:aaa:1#default:bbb:1", "0###");
    internalQueryTest("aaa OR bbb", "default:aaa:1#default:bbb:0", "0###");
    internalQueryTest("aaa OR bbb", "default:aaa:0#default:bbb:0", "1#TOKENIZE#aaa#default");
    internalQueryTest("aaa OR bbb", "", "1#TOKENIZE#aaa#default");
    internalQueryTest("noindex:bbb OR aaa", "", "1#TOKENIZE#bbb#noindex");
    internalQueryTest("aaa OR noindex:bbb", "", "1#TOKENIZE#aaa#default");
}

TEST_F(CheckMatchVisitorTest, testRankQuery){
    internalQueryTest("aaa RANK bbb", "default:aaa:1#default:bbb:1", "0###");
    internalQueryTest("aaa RANK bbb", "default:aaa:1#default:bbb:0", "0###");
    internalQueryTest("aaa RANK bbb", "default:aaa:0#default:bbb:0", "1#TOKENIZE#aaa#default");
    internalQueryTest("aaa RANK bbb", "", "1#TOKENIZE#aaa#default");
}

TEST_F(CheckMatchVisitorTest, testMultiTermQuery){
    internalQueryTest("default:'aaa' & 'bbb'", "default:aaa:1#default:bbb:1", "0###");
    internalQueryTest("default:'aaa' & 'bbb'", "default:aaa:1#default:bbb:0", "1#TOKENIZE#bbb#default");
    internalQueryTest("default:'aaa' | 'bbb'", "default:aaa:1#default:bbb:1", "0###");
    internalQueryTest("default:'aaa' | 'bbb'", "default:aaa:1#default:bbb:0", "0###");
    internalQueryTest("default:'aaa' | 'bbb'", "default:aaa:0#default:bbb:1", "0###");
    internalQueryTest("default:'aaa' | 'bbb'", "default:aaa:0#default:bbb:0", "1#TOKENIZE#bbb#default");
    internalQueryTest("default:'aaa' | 'bbb'", "", "1#TOKENIZE#bbb#default");
    internalQueryTest("default:'aaa' | 'bbb'", "", "1#TOKENIZE#bbb#default");

    // weakand
    internalQueryTest("default:('a'||'b'||'c')^2", "default:a:1#default:b:1", "0###");
    internalQueryTest("default:('a'||'b'||'c')^2", "default:b:1#default:c:1", "0###");
    internalQueryTest("default:('a'||'b'||'c')^2", "default:a:1#default:b:1#default:c:1", "0###");
    internalQueryTest("default:('a'||'b'||'c')^2", "default:a:1", "1#TOKENIZE#b#default");
}

TEST_F(CheckMatchVisitorTest, testComplexQuery){
    internalQueryTest("aaa AND (bbb OR ccc)", "default:aaa:1#default:bbb:0#default:ccc:1", "0###");
    internalQueryTest("aaa AND (bbb OR ccc)", "default:aaa:1", "1#TOKENIZE#bbb#default");
    internalQueryTest("(aaa OR bbb) AND (ddd OR ccc)", "", "1#TOKENIZE#aaa#default");
    internalQueryTest("aaa AND (bbb OR ccc) AND noindex:ddd", "default:aaa:1#default:ccc:1", "1#TOKENIZE#ddd#noindex");
    internalQueryTest("aaa AND (bbb OR ccc) AND noindex:ddd", "default:aaa:1", "1#TOKENIZE#bbb#default");

    internalQueryTest("aaa ANDNOT (bbb OR ccc) AND noindex:ddd", "default:aaa:1", "1#TOKENIZE#ddd#noindex");
    internalQueryTest("aaa ANDNOT (bbb OR ccc) AND noindex:ddd", "default:aaa:1#default:ccc:1", "1#NOT#ccc#default");
}

void CheckMatchVisitorTest::internalQueryTest(const string& queryStr, 
        const string& matchStr, const string& expectStr) 
{
    QueryPtr query(SearcherTestHelper::createQuery(queryStr, 0, _defaultIndex));
    if(!query){
        ASSERT_TRUE(false);
    }
    map<string, bool> matchInfo = createMatchInfo(matchStr);
    CheckMatchVisitor queryVisitor(matchInfo);
    query->accept(&queryVisitor);
    CheckResultInfo checkRes = queryVisitor.getCheckResult(); 
    CheckResultInfo expectRes = createCheckResult(expectStr);
    ASSERT_EQ(expectRes.errorState, checkRes.errorState);
    ASSERT_EQ(expectRes.errorType, checkRes.errorType);
    ASSERT_EQ(expectRes.wordStr, checkRes.wordStr);
    ASSERT_EQ(expectRes.indexStr, checkRes.indexStr);
    isPhraseEqual(queryVisitor.getPhraseQuerys());
}
void CheckMatchVisitorTest::isPhraseEqual(const map<string, string>& phraseMap ){
    ASSERT_EQ(_phraseMap.size(), phraseMap.size());
    map<string, string>::const_iterator expectIter = _phraseMap.begin();
    map<string, string>::const_iterator actualIter = phraseMap.begin();
    for(;expectIter != _phraseMap.end(); expectIter++, actualIter++ ){
        ASSERT_EQ(expectIter->first, actualIter->first);
        ASSERT_EQ(expectIter->second, actualIter->second);
    }
}

map<string, bool> CheckMatchVisitorTest::createMatchInfo(const string& matchStr){
    map<string, bool> matchTermMap;
    vector<string> matchInfoVec = StringUtil::split(matchStr, "#", true);
    istringstream iss;
    string word, indexName, termStr;
    bool isMatch;
    for(size_t i = 0; i < matchInfoVec.size(); i++){
        string &str = matchInfoVec[i];
        std::replace(str.begin(), str.end(), ':', ' ');
        iss.clear();
        iss.str(str);
        iss >> indexName >> word >> isMatch;
        termStr = indexName + ":" + word;
        matchTermMap[termStr] = isMatch;
    }
    return matchTermMap;
}

CheckResultInfo CheckMatchVisitorTest::createCheckResult(const string& resultStr){
    const vector<string> &matchInfoVec = StringUtil::split(resultStr, "#", false);
    assert((size_t)4 == matchInfoVec.size());
    istringstream iss (matchInfoVec[0]);
    CheckResultInfo info;
    iss >> info.errorState;
    info.errorType = CheckResultInfo::getErrorType(matchInfoVec[1]);
    info.wordStr = matchInfoVec[2];
    info.indexStr = matchInfoVec[3];
    return info;
}


END_HA3_NAMESPACE(qrs);

