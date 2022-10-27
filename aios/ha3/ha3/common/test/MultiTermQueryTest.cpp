#include <unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/MultiTermQuery.h>
#include <autil/legacy/any.h>
#include <autil/legacy/jsonizable.h>
#include <autil/legacy/json.h>
#include <string>

using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace std;

BEGIN_HA3_NAMESPACE(common);

class MultiTermQueryTest : public TESTBASE {
public:
    MultiTermQueryTest() {}
    ~MultiTermQueryTest() {}
public:
    void setUp();
    void tearDown();
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, MultiTermQueryTest);

void MultiTermQueryTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
}

void MultiTermQueryTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(MultiTermQueryTest, testSerializeAndDeserialize) {
    HA3_LOG(DEBUG, "Begin Test!");

    MultiTermQuery multiTermQuery("");
    ASSERT_EQ(1u, multiTermQuery.getMinShouldMatch());
    RequiredFields requiredFields;
    TermPtr term1(new Term("one", "indexname", requiredFields));
    TermPtr term2(new Term("two", "indexname", requiredFields));
    TermPtr term3(new Term("three", "indexname", requiredFields));
    multiTermQuery.addTerm(term1);
    multiTermQuery.addTerm(term2);
    multiTermQuery.addTerm(term3);
    multiTermQuery.setMinShouldMatch(2);
    multiTermQuery.setMatchDataLevel(MDL_SUB_QUERY);
    multiTermQuery.setQueryLabel("MultiTermLabel");

    autil::DataBuffer buffer;
    multiTermQuery.serialize(buffer);
    MultiTermQuery expectQuery("");
    expectQuery.deserialize(buffer);
    ASSERT_EQ(expectQuery, multiTermQuery);
    ASSERT_EQ(2, expectQuery.getMinShouldMatch());
    ASSERT_EQ(multiTermQuery.getMatchDataLevel(), expectQuery.getMatchDataLevel());
    ASSERT_EQ(multiTermQuery.getQueryLabel(), expectQuery.getQueryLabel());
}

TEST_F(MultiTermQueryTest, testConstructor) {
    HA3_LOG(DEBUG, "Begin Test!");
    {
        MultiTermQuery multiTermQuery("");
        ASSERT_EQ(MDL_TERM, multiTermQuery.getMatchDataLevel());
        ASSERT_EQ(string(""), multiTermQuery.getQueryLabel());
    }
    {
        MultiTermQuery multiTermQuery("MultiLabel");
        ASSERT_EQ(MDL_TERM, multiTermQuery.getMatchDataLevel());
        ASSERT_EQ(string("MultiLabel"), multiTermQuery.getQueryLabel());
    }
}

END_HA3_NAMESPACE(common);
