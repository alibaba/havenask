#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/PageDistinctSelector.h>
#include <ha3/qrs/PageDistinctHitConvertor.h>
#include <autil/StringUtil.h>
#include <autil/TimeUtility.h>
#include <autil/StringTokenizer.h>
#include <ha3/qrs/test/PageDistinctSelectorTest.h>

using namespace std;
using namespace autil;
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(qrs);

HA3_LOG_SETUP(qrs, PageDistinctSelectorTest);


PageDistinctSelectorTest::PageDistinctSelectorTest() { 
}

PageDistinctSelectorTest::~PageDistinctSelectorTest() { 
}

void PageDistinctSelectorTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void PageDistinctSelectorTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
    for (size_t i = 0; i < _pageDistHitsPool.size(); i++) {
        delete _pageDistHitsPool[i];
    }
    _pageDistHitsPool.clear();
}

#define CHECK_PAGE_DIST_HITS(posVec, posStr) {                          \
    StringTokenizer st(posStr, ",",                                     \
                       StringTokenizer::TOKEN_IGNORE_EMPTY |            \
                       StringTokenizer::TOKEN_TRIM);                    \
    ASSERT_EQ((size_t)st.getNumTokens(), posVec.size());     \
    StringTokenizer::Iterator it = st.begin();                          \
    vector<size_t>::const_iterator posIt = posVec.begin();              \
    for (;it != st.end(); it++, posIt++) {                              \
        ASSERT_EQ(StringUtil::fromString<size_t>(*it),       \
                             *posIt);                                   \
    }                                                                   \
    }


TEST_F(PageDistinctSelectorTest, testInternalSelectPagesWithoutGrade) { 
    HA3_LOG(DEBUG, "Begin Test!");

    string distKey = "dist_key";

    PageDistinctSelectorTyped<uint32_t> selector(
            5, distKey, 2, vector<bool>(1, false));

    vector<PageDistinctHit<uint32_t>*> hits; 
    vector<size_t> pageHitPosVec;
    selector.selectPages(hits, 1, pageHitPosVec);
    ASSERT_EQ((size_t)0, pageHitPosVec.size());

    hits = createPageDistHits("0,1,0;"
                              "1,1,0;"
                              "2,3,0;"
                              "3,2,0;"
                              "4,1,0;"
                              "5,3,0;"
                              "6,3,0;"
                              );

    pageHitPosVec.clear();
    selector.selectPages(hits, 0, pageHitPosVec);
    ASSERT_EQ((size_t)0, pageHitPosVec.size());    

    pageHitPosVec.clear();
    selector.selectPages(hits, 1, pageHitPosVec);
    CHECK_PAGE_DIST_HITS(pageHitPosVec, "0, 1, 2, 3, 5");

    pageHitPosVec.clear();
    selector.selectPages(hits, 2, pageHitPosVec);
    CHECK_PAGE_DIST_HITS(pageHitPosVec, "0, 1, 2, 3, 5, 4, 6");

    hits = createPageDistHits("0,1,0;"
                              "1,1,0;"
                              "2,1,0;"
                              "3,1,0;"
                              "4,1,0;"
                              "5,2,0;"
                              "6,3,0;"
                              );
    pageHitPosVec.clear();
    selector.selectPages(hits, 1, pageHitPosVec);
    CHECK_PAGE_DIST_HITS(pageHitPosVec, "0, 1, 2, 5, 6")

    hits = createPageDistHits("0,1,0;"
                              "1,1,0;"
                              "2,1,0;"
                              "3,1,0;"
                              "4,1,0;"
                              "5,2,0;"
                              "6,2,0;"
                              "7,2,0;"
                              "8,2,0;"
                              "9,2,0;"
                              "10,3,0;"
                              "11,3,0;"
                              "12,3,0;"
                              "13,3,0;"
                              "14,3,0;"
                              );
    pageHitPosVec.clear();
    selector.selectPages(hits, 3, pageHitPosVec);
    CHECK_PAGE_DIST_HITS(pageHitPosVec, 
                         "0, 1, 5, 6, 10,"
                         "2, 3, 7, 8, 11,"
                         "4, 9, 12, 13, 14");
}

TEST_F(PageDistinctSelectorTest, testInternalSelectPagesWithGrade) { 
    HA3_LOG(DEBUG, "Begin Test!");

    string distKey = "dist_key";

    PageDistinctSelectorTyped<uint32_t> selector(
            5, distKey, 2, vector<bool>(1, false));

    vector<PageDistinctHit<uint32_t>*> hits; 
    hits = createPageDistHits("0,1,1;"
                              "1,1,1;"
                              "2,3,1;"
                              "3,2,1;"
                              "4,1,1;"
                              "5,3,0;"
                              );
    vector<size_t> pageHitPosVec;
    selector.selectPages(hits, 1, pageHitPosVec);
    CHECK_PAGE_DIST_HITS(pageHitPosVec, "0, 1, 2, 3, 4,");

    hits = createPageDistHits("0,1,1;"
                              "1,1,1;"
                              "2,3,1;"
                              "3,2,1;"
                              "4,1,1;"
                              "5,1,1;"
                              "6,1,1;"
                              "7,1,1;"
                              "8,3,0;"
                              "9,2,0;"
                              );
    pageHitPosVec.clear();
    selector.selectPages(hits, 2, pageHitPosVec);
    CHECK_PAGE_DIST_HITS(pageHitPosVec, 
                         "0, 1, 2, 3, 4,"
                         "5, 6, 7, 8, 9,"
                         );
    
}

TEST_F(PageDistinctSelectorTest, testSelectPages) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string distKey = "distKey";
    PageDistinctSelectorPtr selector(
            new PageDistinctSelectorTyped<uint32_t>(
                    5, distKey, 2, vector<bool>(1, false)));
    vector<HitPtr> hits; 
    vector<HitPtr> pageHits; 

    selector->selectPages(hits, 0, 5, pageHits);
    checkHits(pageHits, "");
    hits = createHits(distKey, 
                      "10,2,2.5;"
                      "1,1,5.0;"
                      "3,2,3.2;"
                      "7,1,2.8;"
                      "8,1,2.7;"
                      "5,1,3.0;"
                      "4,3,3.1;"
                      "9,3,2.6;"
                      "6,2,2.9;"
                      "2,1,4.0;"
                      "11,3,2.4;");

    selector->selectPages(hits, 0, 0, pageHits);
    checkHits(pageHits, "");
    selector->selectPages(hits, 0, 5, pageHits);
    checkHits(pageHits, "1, 2, 3, 4, 6");
    selector->selectPages(hits, 5, 5, pageHits);
    checkHits(pageHits, "5, 7, 9, 10, 11");
    selector->selectPages(hits, 10, 5, pageHits);
    checkHits(pageHits, "8");
    selector->selectPages(hits, 2, 5, pageHits);
    checkHits(pageHits, "3, 4, 6, 5, 7");
    vector<bool> sortFlags;
    sortFlags.push_back(true);
    
    PageDistinctSelectorPtr selector2(
            new PageDistinctSelectorTyped<uint32_t>(
                    5, distKey, 2, 
                    sortFlags,
                    vector<double>()));
    selector2->selectPages(hits, 5, 5, pageHits);
    checkHits(pageHits, "6, 5, 4, 3, 2");
}

TEST_F(PageDistinctSelectorTest, testSelectPagesWithHitsNotEnough) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string distKey = "distKey";
    PageDistinctSelectorPtr selector(
            new PageDistinctSelectorTyped<uint32_t>(
                    5, distKey, 2, vector<bool>(1, false)));
    vector<HitPtr> hits; 
    vector<HitPtr> pageHits; 
    
    hits = createHits(distKey, 
                      "1,1,5.0;"
                      "2,1,4.0;"
                      "3,1,3.2;"
                      "4,1,3.1;");

    selector->selectPages(hits, 0, 5, pageHits);
    checkHits(pageHits, "1, 2, 3, 4");
    selector->selectPages(hits, 5, 5, pageHits);
    checkHits(pageHits, "");
}

TEST_F(PageDistinctSelectorTest, testSelectPagesWithSort) {
    HA3_LOG(DEBUG, "Begin Test!");
    string distKey = "distKey";
    PageDistinctSelectorPtr selector(
            new PageDistinctSelectorTyped<uint32_t>(
                    5, distKey, 2, vector<bool>(1, false)));
    vector<HitPtr> hits; 
    vector<HitPtr> pageHits; 
    hits = createHits(distKey, 
                      "1,1,5.0;"
                      "2,1,4.0;"
                      "3,2,3.2;"
                      "4,3,3.1;"
                      "5,1,3.0;"
                      "6,2,2.9;"
                      "7,1,2.8;"
                      "9,3,2.6;"
                      "10,2,2.5;"
                      "11,3,2.4;"
                      "8,1,2.7;");

    uint32_t startOffset = 5;
    uint32_t hitCount = 5;

    selector->selectPages(hits, startOffset, hitCount, pageHits);

    checkHits(pageHits, "5, 7, 9, 10, 11");
}

TEST_F(PageDistinctSelectorTest, testSelectPagesWithGradeAndMultiAttris) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string distKey = "distKey";
    vector<double> thresHolds;
    thresHolds.push_back(3.0);
    vector<bool> sortFlags(2, false);
    PageDistinctSelectorPtr selector(
            new PageDistinctSelectorTyped<uint32_t>(
                    5, distKey, 2, sortFlags, thresHolds));
    vector<HitPtr> hits; 
    vector<HitPtr> pageHits; 
    hits = createHits(distKey, 
                      "1,1,5.0#1.5;"
                      "2,1,4.0#1.5;"
                      "3,1,4.0#1.3;"
                      "4,2,3.0#1.2;"
                      "5,2,3.0#1.2;"
                      "6,2,2.9#1.5;"
                      "7,1,2.8#1.5;"
                      "8,1,2.8#1.5;"
                      "9,1,2.8#1.4;"
                      "10,2,2.5#2.4;"
                      "11,3,2.5#2.3;");

    selector->selectPages(hits, 0, 5, pageHits);
    checkHits(pageHits, "1, 2, 3, 5, 4");
    selector->selectPages(hits, 5, 5, pageHits);
    checkHits(pageHits, "6, 8, 7, 10, 11");
    selector->selectPages(hits, 10, 5, pageHits);
    checkHits(pageHits, "9");
    selector->selectPages(hits, 2, 5, pageHits);
    checkHits(pageHits, "3, 5, 4, 6, 8");

    sortFlags.clear();
    sortFlags.push_back(true);
    sortFlags.push_back(true);
    
    PageDistinctSelectorPtr selector2(
            new PageDistinctSelectorTyped<uint32_t>(
                    5, distKey, 2, sortFlags, thresHolds));
    selector2->selectPages(hits, 5, 5, pageHits);
    checkHits(pageHits, "7, 5, 4, 3, 2");
}

vector<PageDistinctHit<uint32_t>*> 
PageDistinctSelectorTest::createPageDistHits(const string &hitStr) 
{

    vector<PageDistinctHit<uint32_t>*> pageDistHits;
    StringTokenizer st1(hitStr, ";",
                        StringTokenizer::TOKEN_IGNORE_EMPTY | 
                        StringTokenizer::TOKEN_TRIM);
    for (StringTokenizer::Iterator hitIt = st1.begin();
         hitIt != st1.end(); hitIt++)
    {
        StringTokenizer st2(*hitIt, ",",
                            StringTokenizer::TOKEN_IGNORE_EMPTY | 
                            StringTokenizer::TOKEN_TRIM);
        assert(3 == st2.getNumTokens());
        PageDistinctHit<uint32_t> * pageDistHit = new PageDistinctHit<uint32_t>();
        pageDistHit->pos = StringUtil::fromString<size_t>(st2[0]);
        pageDistHit->keyValue = StringUtil::fromString<uint32_t>(st2[1]);
        pageDistHit->gradeValue = StringUtil::fromString<int32_t>(st2[2]);
        pageDistHit->hasKeyValue = true;
        pageDistHits.push_back(pageDistHit);
        _pageDistHitsPool.push_back(pageDistHit);
    }
    return pageDistHits;
}

void PageDistinctSelectorTest::checkHits(const vector<HitPtr> &hits,
                                         const string &expectedHitsStr)
{
    StringTokenizer st(expectedHitsStr, 
                       ",",
                       StringTokenizer::TOKEN_IGNORE_EMPTY | 
                       StringTokenizer::TOKEN_TRIM);
    ASSERT_EQ((size_t)st.getNumTokens(), hits.size());
    StringTokenizer::Iterator it = st.begin();
    vector<HitPtr>::const_iterator hitIt = hits.begin();
    for (;it != st.end(); it++, hitIt++) {
        ASSERT_TRUE(NULL != (*hitIt));
        ASSERT_EQ(StringUtil::fromString<docid_t>(*it),
                             (*hitIt)->getDocId());
    }
}

vector<HitPtr> PageDistinctSelectorTest::createHits(const string &keyName,
                                                  const string &hitStr) 
{
    vector<HitPtr> hits;
    StringTokenizer st1(hitStr, ";",
                        StringTokenizer::TOKEN_IGNORE_EMPTY | 
                        StringTokenizer::TOKEN_TRIM);
    for (StringTokenizer::Iterator hitIt = st1.begin();
         hitIt != st1.end(); hitIt++)
    {
        StringTokenizer st2(*hitIt, ",",
                            StringTokenizer::TOKEN_IGNORE_EMPTY | 
                            StringTokenizer::TOKEN_TRIM);
        assert(3 == st2.getNumTokens());
        addHit(hits, StringUtil::fromString<docid_t>(st2[0]),
               keyName, StringUtil::fromString<uint32_t>(st2[1]),
               st2[2]);
    }
    return hits;
}

void PageDistinctSelectorTest::addHit(vector<HitPtr> &hits, 
                                      docid_t docid, 
                                      const string &distKey, 
                                      uint32_t keyValue, 
                                      const string &sortValue)
{
    hits.push_back(createHit(docid, distKey, keyValue, sortValue));
}

HitPtr PageDistinctSelectorTest::createHit(docid_t docid, 
        const string &distKey, 
        uint32_t keyValue, 
        const string &sortValue) 
{
    HitPtr hitPtr(new Hit(docid));
    bool ret = hitPtr->addAttribute(distKey, keyValue);
    (void)ret;
    assert(ret);
    StringTokenizer st(sortValue, "#",
                       StringTokenizer::TOKEN_IGNORE_EMPTY | 
                       StringTokenizer::TOKEN_TRIM);
    for (StringTokenizer::Iterator it = st.begin();
         it != st.end(); it++) 
    {
        hitPtr->addSortExprValue(*it, vt_double);
    }
    if (st.getNumTokens() > 0) {
        double s;
        StringUtil::strToDouble(st.begin()->c_str(), s);
        hitPtr->setSortValue(s);
    }
    return hitPtr;
}

END_HA3_NAMESPACE(qrs);

