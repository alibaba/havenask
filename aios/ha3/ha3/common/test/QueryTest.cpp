#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Query.h>
#include <ha3/common/test/QueryConstructor.h>
#include <ha3/common/PhraseQuery.h>
#include <ha3/common/AndQuery.h>
#include <ha3/common/OrQuery.h>
#include <ha3/common/AndNotQuery.h>
#include <ha3/common/RankQuery.h>
#include <ha3/common/MultiTermQuery.h>
#include <autil/DataBuffer.h>

using namespace std;

BEGIN_HA3_NAMESPACE(common);

class QueryTest : public TESTBASE {
public:
    QueryTest();
    ~QueryTest();
public:
    void setUp();
    void tearDown();
protected:
    template <typename QueryType>
    void innerTestSerializeAndDeserialize(MatchDataLevel level,
            const string&queryLabel);
    template <typename QueryType>
    void innerTestLabelAndLevel(const string& queryLabel, bool isTerm);
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, QueryTest);


QueryTest::QueryTest() {
}

QueryTest::~QueryTest() {
}

void QueryTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
}

void QueryTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(QueryTest, testSerializeAndDeserialize) {
    HA3_LOG(DEBUG, "Begin Test!");
    innerTestSerializeAndDeserialize<AndQuery>(MDL_TERM, "AndLabel");
    innerTestSerializeAndDeserialize<OrQuery>(MDL_SUB_QUERY, "OrLabel");
    innerTestSerializeAndDeserialize<AndNotQuery>(MDL_TERM, "AndNotLabel");
    innerTestSerializeAndDeserialize<RankQuery>(MDL_SUB_QUERY, "RankLabel");
    innerTestSerializeAndDeserialize<PhraseQuery>(MDL_NONE, "PhraseLabel");
    innerTestSerializeAndDeserialize<MultiTermQuery>(MDL_NONE, "MultiTermLabel");

    innerTestLabelAndLevel<AndQuery>("AndLabel", false);
    innerTestLabelAndLevel<OrQuery>("OrLabel", false);
    innerTestLabelAndLevel<AndNotQuery>("AndNotLabel", false);
    innerTestLabelAndLevel<RankQuery>("RankLabel", false);
    innerTestLabelAndLevel<PhraseQuery>("PhraseLabel", true);
    innerTestLabelAndLevel<MultiTermQuery>("MultiTermLabel", true);

    innerTestLabelAndLevel<AndQuery>("", false);
    innerTestLabelAndLevel<OrQuery>("", false);
    innerTestLabelAndLevel<AndNotQuery>("", false);
    innerTestLabelAndLevel<RankQuery>("", false);
    innerTestLabelAndLevel<PhraseQuery>("", true);
    innerTestLabelAndLevel<MultiTermQuery>("", true);
}

template <typename QueryType>
void QueryTest::innerTestSerializeAndDeserialize(MatchDataLevel level,
        const string& queryLabel)
{
    Query *query = new QueryType("");
    QueryConstructor::prepare(query, "default", "first", "second", "third");
    query->setMatchDataLevel(level);
    query->setQueryLabel(queryLabel);
    autil::DataBuffer dataBuffer;
    dataBuffer.write(query);

    Query *query2 = NULL;
    dataBuffer.read(query2);

    ASSERT_TRUE(*query == *query2);
    ASSERT_TRUE(query->getMatchDataLevel() == query2->getMatchDataLevel());
    ASSERT_TRUE(query->getQueryLabel() == query2->getQueryLabel());
    DELETE_AND_SET_NULL(query);
    DELETE_AND_SET_NULL(query2);
}

template <typename QueryType>
void QueryTest::innerTestLabelAndLevel(const string& queryLabel, bool isTerm) {
    Query *query = new QueryType("");
    if (isTerm) {
        query->setQueryLabelTerm(queryLabel);
        ASSERT_EQ(MDL_TERM, query->_matchDataLevel);
    } else {
        query->setQueryLabelBinary(queryLabel);
        MatchDataLevel mdl = (queryLabel.empty()) ? MDL_NONE : MDL_SUB_QUERY;
        ASSERT_EQ(mdl, query->_matchDataLevel);
    }
    ASSERT_EQ(queryLabel, query->_queryLabel);

    autil::DataBuffer dataBuffer;
    query->serializeMDLandQL(dataBuffer);
    Query *query2 = new QueryType(queryLabel + "_diff");
    query2->deserializeMDLandQL(dataBuffer);
    ASSERT_TRUE(query->getMatchDataLevel() == query2->getMatchDataLevel());
    ASSERT_TRUE(query->getQueryLabel() == query2->getQueryLabel());
    DELETE_AND_SET_NULL(query);
    DELETE_AND_SET_NULL(query2);
}

END_HA3_NAMESPACE(common);
