#include "indexlib/analyzer/StopWordFilter.h"

#include <set>

#include "unittest/unittest.h"
using namespace std;

namespace indexlibv2 { namespace analyzer {

class StopWordFilterTest : public TESTBASE
{
private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.analyzer, StopWordFilterTest);

TEST_F(StopWordFilterTest, testIsStopWordEmpty)
{
    set<string> emptySet;
    StopWordFilter stopWordFilter(emptySet);
    ASSERT_TRUE(!stopWordFilter.isStopWord(" "));
}

TEST_F(StopWordFilterTest, testIsStopWordOneSpace)
{
    set<string> stopWords;
    stopWords.insert(" ");
    StopWordFilter stopWordFilter(stopWords);
    ASSERT_TRUE(stopWordFilter.isStopWord(" "));
    ASSERT_TRUE(!stopWordFilter.isStopWord(","));
    ASSERT_TRUE(!stopWordFilter.isStopWord("　"));
    ASSERT_TRUE(!stopWordFilter.isStopWord(""));
}

TEST_F(StopWordFilterTest, testIsStopWord)
{
    set<string> stopWords;
    stopWords.insert("");
    stopWords.insert("　");
    stopWords.insert("multi chars");

    StopWordFilter stopWordFilter;
    stopWordFilter.setStopWords(stopWords);

    ASSERT_TRUE(stopWordFilter.isStopWord(""));
    ASSERT_TRUE(stopWordFilter.isStopWord("　"));
    ASSERT_TRUE(stopWordFilter.isStopWord("multi chars"));
    ASSERT_TRUE(!stopWordFilter.isStopWord("not stop word"));
}

}} // namespace indexlibv2::analyzer
