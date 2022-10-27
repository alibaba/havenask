#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/DocIdClause.h>

using namespace std;

BEGIN_HA3_NAMESPACE(common);

class DocIdClauseTest : public TESTBASE {
public:
    DocIdClauseTest();
    ~DocIdClauseTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, DocIdClauseTest);


DocIdClauseTest::DocIdClauseTest() { 
}

DocIdClauseTest::~DocIdClauseTest() { 
}

void DocIdClauseTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void DocIdClauseTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(DocIdClauseTest, testSerialization) { 
    HA3_LOG(DEBUG, "Begin Test!");

    DocIdClause docIdClause;
    docIdClause.addGlobalId(GlobalIdentifier(5, 1, versionid_t(1)));
    docIdClause.addGlobalId(GlobalIdentifier(7, 1, versionid_t(2)));
    docIdClause.addGlobalId(GlobalIdentifier(11, 1, versionid_t(3)));
    docIdClause.addGlobalId(GlobalIdentifier(677, 2, versionid_t(4)));

    TermVector terms;
    RequiredFields requiredFields;

    terms.push_back(Term("aaa", "", requiredFields));
    terms.push_back(Term("bbb", "", requiredFields));
    docIdClause.setTermVector(terms);

    DocIdClause resultDocIdClause;
    autil::DataBuffer buffer;
    buffer.write(docIdClause);
    buffer.read(resultDocIdClause);

    const TermVector &resultTerms = resultDocIdClause.getTermVector();
    ASSERT_EQ((size_t)2, resultTerms.size());
    ASSERT_EQ(Term("aaa", "", requiredFields), resultTerms[0]);
    ASSERT_EQ(Term("bbb", "", requiredFields), resultTerms[1]);

    const GlobalIdVector& globalIdVector = resultDocIdClause.getGlobalIdVector();
    ASSERT_EQ((size_t)4, globalIdVector.size());
    ASSERT_EQ(versionid_t(1), globalIdVector[0].getIndexVersion());
    ASSERT_EQ(versionid_t(2), globalIdVector[1].getIndexVersion());
    ASSERT_EQ(versionid_t(3), globalIdVector[2].getIndexVersion());    
    ASSERT_EQ(versionid_t(4), globalIdVector[3].getIndexVersion());
}

TEST_F(DocIdClauseTest, testGetGlobalIdVectorMap) { 
    HA3_LOG(DEBUG, "Begin Test!");

    DocIdClause docIdClause;
    docIdClause.addGlobalId(GlobalIdentifier(5, 1, versionid_t(1)));
    docIdClause.addGlobalId(GlobalIdentifier(7, 1, versionid_t(1)));
    docIdClause.addGlobalId(GlobalIdentifier(11, 1, versionid_t(2)));
    docIdClause.addGlobalId(GlobalIdentifier(677, 2, versionid_t(3)));

    proto::Range range;
    range.set_from(1);
    range.set_to(1);
    GlobalIdVectorMap globalIdVectorMap;
    docIdClause.getGlobalIdVectorMap(range, 0, globalIdVectorMap);
    ASSERT_EQ((size_t)2, globalIdVectorMap.size());
    GlobalIdVector globalIdVector = globalIdVectorMap[versionid_t(1)];
    ASSERT_EQ((size_t)2, globalIdVector.size());
    ASSERT_EQ((docid_t)5, globalIdVector[0].getDocId());
    ASSERT_EQ((docid_t)7, globalIdVector[1].getDocId());

    globalIdVector = globalIdVectorMap[versionid_t(2)];
    ASSERT_EQ((size_t)1, globalIdVector.size());
    ASSERT_EQ((docid_t)11, globalIdVector[0].getDocId());

    globalIdVectorMap.clear();
    range.set_from(2);
    range.set_to(2);
    docIdClause.getGlobalIdVectorMap(range, 0, globalIdVectorMap);
    ASSERT_EQ((size_t)1, globalIdVectorMap.size());
    globalIdVector = globalIdVectorMap[versionid_t(3)];
    ASSERT_EQ((size_t)1, globalIdVector.size());
    ASSERT_EQ((docid_t)677, globalIdVector[0].getDocId());
}

TEST_F(DocIdClauseTest, testGetGlobalIdVector) {
    DocIdClause docIdClause;
    docIdClause.addGlobalId(GlobalIdentifier(1, 1, versionid_t(1), 0, 0, 1));
    docIdClause.addGlobalId(GlobalIdentifier(2, 1, versionid_t(1), 0, 0, 2));
    docIdClause.addGlobalId(GlobalIdentifier(3, 1, versionid_t(2), 0, 0, 1));
    docIdClause.addGlobalId(GlobalIdentifier(4, 2, versionid_t(1), 0, 0, 2));
    docIdClause.addGlobalId(GlobalIdentifier(5, 1, versionid_t(2), 0, 0, 1));
    docIdClause.addGlobalId(GlobalIdentifier(6, 2, versionid_t(1), 0, 0, 1));

    GlobalIdVector gids;
    proto::Range range;
    range.set_from(2);
    range.set_to(2);    
    gids.clear();
    docIdClause.getGlobalIdVector(range, gids);
    ASSERT_EQ((size_t)2, gids.size());
    ASSERT_EQ((docid_t)4, gids[0].getDocId());
    ASSERT_EQ((docid_t)6, gids[1].getDocId());    
}

TEST_F(DocIdClauseTest, testGetHashidVersionSet) {
    DocIdClause docIdClause;
    docIdClause.addGlobalId(GlobalIdentifier(1, 1, versionid_t(1), 0, 0, 1));
    docIdClause.addGlobalId(GlobalIdentifier(2, 1, versionid_t(1), 0, 0, 2));
    docIdClause.addGlobalId(GlobalIdentifier(3, 1, versionid_t(2), 0, 0, 1));
    docIdClause.addGlobalId(GlobalIdentifier(4, 2, versionid_t(1), 0, 0, 2));
    docIdClause.addGlobalId(GlobalIdentifier(5, 1, versionid_t(2), 0, 0, 1));
    docIdClause.addGlobalId(GlobalIdentifier(6, 2, versionid_t(1), 0, 0, 1));

    HashidVersionSet hasidVersionSet;
    docIdClause.getHashidVersionSet(hasidVersionSet);
    ASSERT_EQ((size_t)4, hasidVersionSet.size());
    ASSERT_TRUE(hasidVersionSet.find(std::make_pair(1,1)) != hasidVersionSet.end());
    ASSERT_TRUE(hasidVersionSet.find(std::make_pair(1,2)) != hasidVersionSet.end());
    ASSERT_TRUE(hasidVersionSet.find(std::make_pair(2,1)) != hasidVersionSet.end());
    ASSERT_TRUE(hasidVersionSet.find(std::make_pair(2,2)) != hasidVersionSet.end());
}

TEST_F(DocIdClauseTest, testGetGlobalIdVectorMapWithVersion) {
    DocIdClause docIdClause;
    docIdClause.addGlobalId(GlobalIdentifier(1, 1, versionid_t(1), 0, 0, 1));
    docIdClause.addGlobalId(GlobalIdentifier(2, 1, versionid_t(1), 0, 0, 2));
    docIdClause.addGlobalId(GlobalIdentifier(3, 1, versionid_t(2), 0, 0, 1));
    docIdClause.addGlobalId(GlobalIdentifier(4, 2, versionid_t(2), 0, 0, 2));
    docIdClause.addGlobalId(GlobalIdentifier(5, 1, versionid_t(2), 0, 0, 1));
    docIdClause.addGlobalId(GlobalIdentifier(6, 2, versionid_t(3), 0, 0, 1));

    GlobalIdVectorMap globalIdVectorMap;
    proto::Range range;
    range.set_from(1);
    range.set_to(1);
    docIdClause.getGlobalIdVectorMap(range, 1, globalIdVectorMap);
    ASSERT_EQ(size_t(2), globalIdVectorMap.size());
    ASSERT_TRUE(globalIdVectorMap.find(versionid_t(1)) != globalIdVectorMap.end());
    ASSERT_TRUE(globalIdVectorMap.find(versionid_t(2)) != globalIdVectorMap.end());

    globalIdVectorMap.clear();
    docIdClause.getGlobalIdVectorMap(range, 2, globalIdVectorMap);
    ASSERT_EQ(size_t(1), globalIdVectorMap.size());
    ASSERT_TRUE(globalIdVectorMap.find(versionid_t(1)) != globalIdVectorMap.end());

    globalIdVectorMap.clear();
    range.set_from(2);
    range.set_to(2);    
    docIdClause.getGlobalIdVectorMap(range, 1, globalIdVectorMap);
    ASSERT_EQ(size_t(1), globalIdVectorMap.size());
    ASSERT_TRUE(globalIdVectorMap.find(versionid_t(3)) != globalIdVectorMap.end());

    globalIdVectorMap.clear();
    docIdClause.getGlobalIdVectorMap(range, 2, globalIdVectorMap);
    ASSERT_EQ(size_t(1), globalIdVectorMap.size());
    ASSERT_TRUE(globalIdVectorMap.find(versionid_t(2)) != globalIdVectorMap.end());
}

END_HA3_NAMESPACE(common);

