#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Term.h>
#include <build_service/analyzer/Token.h>

using namespace std;
using namespace build_service::analyzer;

BEGIN_HA3_NAMESPACE(common);

class TermTest : public TESTBASE {
public:
    TermTest();
    ~TermTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, TermTest);


TermTest::TermTest() { 
}

TermTest::~TermTest() { 
}

void TermTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void TermTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(TermTest, testTermEqual) { 
    HA3_LOG(DEBUG, "Begin Test!");
    RequiredFields requiredFields;
    requiredFields.isRequiredAnd = true;
    requiredFields.fields.push_back("field1");
    requiredFields.fields.push_back("field2");
    Term term1("one", "indexname1", requiredFields, 100, "truncateName");
    Term term2("one", "indexname1", requiredFields, 100, "truncateName");
    ASSERT_TRUE(term1 == term2);
    term2._requiredFields.isRequiredAnd = false;
    ASSERT_TRUE(!(term1 == term2));

    term2._requiredFields.isRequiredAnd = true;
    term2._requiredFields.fields.push_back("field3");
    ASSERT_TRUE(!(term1 == term2));

    term1._requiredFields.fields.push_back("field4");
    ASSERT_TRUE(!(term1 == term2));
}

TEST_F(TermTest, testSerializeAndDeserialize) { 
    HA3_LOG(DEBUG, "Begin Test!");

    autil::DataBuffer buf1;
    RequiredFields requiredFields;
    requiredFields.isRequiredAnd = false;
    requiredFields.fields.push_back("field1");
    requiredFields.fields.push_back("field2");
    Term term1("one", "indexname1", requiredFields, 100, "truncateName");
    term1.serialize(buf1);
    Term deserializedTerm1;
    deserializedTerm1.deserialize(buf1);

    ASSERT_EQ(string("one"), deserializedTerm1.getWord());
    ASSERT_EQ(string("indexname1"), deserializedTerm1.getIndexName());
    ASSERT_EQ(100, deserializedTerm1.getBoost());
    ASSERT_EQ(string("truncateName"), deserializedTerm1.getTruncateName());
    ASSERT_EQ(false,
                         deserializedTerm1.getRequiredFields().isRequiredAnd);
    ASSERT_EQ(size_t(2), 
                         deserializedTerm1.getRequiredFields().fields.size());
    ASSERT_EQ(string("field1"),
                         deserializedTerm1.getRequiredFields().fields[0]);
    ASSERT_EQ(string("field2"), 
                         deserializedTerm1.getRequiredFields().fields[1]);

    Token token2("TWO", 2, "two", true);
    Term term2(token2, "indexname2", requiredFields, 200);
    autil::DataBuffer buf2;
    term2.serialize(buf2);
    Term deserializedTerm2;
    deserializedTerm2.deserialize(buf2);

    ASSERT_EQ(string("two"), deserializedTerm2.getWord());
    ASSERT_EQ(string("indexname2"), deserializedTerm2.getIndexName());
    ASSERT_EQ(200, deserializedTerm2.getBoost());
    ASSERT_EQ(string(""), deserializedTerm2.getTruncateName());
    ASSERT_EQ(string("TWO"), deserializedTerm2.getToken().getText());
    ASSERT_EQ(string("two"), deserializedTerm2.getToken().getNormalizedText());
    ASSERT_EQ(true, deserializedTerm2.getToken().isStopWord());
}

TEST_F(TermTest, testTermComp) {
    RequiredFields requiredFields;
    Term term1("term1", "index_name1", requiredFields);
    Term term2("term2", "index_name1", requiredFields);
    Term term3("term2", "index_name2", requiredFields);
    Term term4("term1", "index_name1", requiredFields);

    ASSERT_TRUE(term1 < term2);
    ASSERT_TRUE(term1 < term3);
    ASSERT_TRUE(!(term1 < term4));
    ASSERT_TRUE(term2 < term3);
    ASSERT_TRUE(term2 > term1);
    ASSERT_TRUE(!(term1 < term4));
    ASSERT_TRUE(!(term1 > term4));
}

END_HA3_NAMESPACE(common);

