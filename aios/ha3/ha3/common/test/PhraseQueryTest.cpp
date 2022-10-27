#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include "ha3/common/PhraseQuery.h"

using namespace std;
using namespace testing;

BEGIN_HA3_NAMESPACE(common);

class PhraseQueryTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, PhraseQueryTest);

void PhraseQueryTest::setUp() {
}

void PhraseQueryTest::tearDown() {
}

TEST_F(PhraseQueryTest, testConstructor) {
    {
        string label("");
        PhraseQuery phraseQuery(label);
        ASSERT_EQ(MDL_TERM, phraseQuery.getMatchDataLevel());
        ASSERT_EQ(label, phraseQuery.getQueryLabel());
    }
    {
        string label("phraseLabel");
        PhraseQuery phraseQuery(label);
        ASSERT_EQ(MDL_TERM, phraseQuery.getMatchDataLevel());
        ASSERT_EQ(label, phraseQuery.getQueryLabel());
    }
}

TEST_F(PhraseQueryTest, testSetQueryLabelWithDefaultLevel) {
    PhraseQuery phraseQuery("");
    phraseQuery.setQueryLabelWithDefaultLevel("phraseLabel");
    ASSERT_EQ(MDL_TERM, phraseQuery.getMatchDataLevel());
    ASSERT_EQ("phraseLabel", phraseQuery.getQueryLabel());
}

END_HA3_NAMESPACE();

