#include "build_service/analyzer/StopWordFilter.h"
#include "build_service/test/unittest.h"
#include <set>
using namespace std;

namespace build_service {
namespace analyzer {

class StopWordFilterTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
};

void StopWordFilterTest::setUp() {
}

void StopWordFilterTest::tearDown() {
}

TEST_F(StopWordFilterTest, testIsStopWordEmpty) {
    BS_LOG(DEBUG, "Begin Test!");

    set<string> emptySet;
    StopWordFilter stopWordFilter(emptySet);
    ASSERT_TRUE(!stopWordFilter.isStopWord(" "));
}

TEST_F(StopWordFilterTest, testIsStopWordOneSpace) {
    set<string> stopWords;
    stopWords.insert(" ");
    StopWordFilter stopWordFilter(stopWords);
    ASSERT_TRUE(stopWordFilter.isStopWord(" "));
    ASSERT_TRUE(!stopWordFilter.isStopWord(","));
    ASSERT_TRUE(!stopWordFilter.isStopWord("　"));
    ASSERT_TRUE(!stopWordFilter.isStopWord(""));
}

TEST_F(StopWordFilterTest, testIsStopWord) {
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


}
}
