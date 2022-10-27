#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/PKFilterClause.h>

using namespace std;
BEGIN_HA3_NAMESPACE(common);

class PKFilterClauseTest : public TESTBASE {
public:
    PKFilterClauseTest();
    ~PKFilterClauseTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, PKFilterClauseTest);


PKFilterClauseTest::PKFilterClauseTest() { 
}

PKFilterClauseTest::~PKFilterClauseTest() { 
}

void PKFilterClauseTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void PKFilterClauseTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(PKFilterClauseTest, testSerialize) { 
    HA3_LOG(DEBUG, "Begin Test!");
    PKFilterClause pkFilterClause1;
    pkFilterClause1.setOriginalString("abcd");
    PKFilterClause pkFilterClause2;

    autil::DataBuffer buffer;
    pkFilterClause1.serialize(buffer);
    pkFilterClause2.deserialize(buffer);
    ASSERT_EQ(pkFilterClause1.getOriginalString(),
                         pkFilterClause2.getOriginalString());
}
END_HA3_NAMESPACE(common);

