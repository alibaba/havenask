#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/FilterClause.h>
#include <ha3/common/AggregateDescription.h>
#include <suez/turing/expression/syntax/AtomicSyntaxExpr.h>
#include <suez/turing/expression/syntax/BinarySyntaxExpr.h>
#include <ha3/queryparser/ClauseParserContext.h>

using namespace std;

USE_HA3_NAMESPACE(queryparser);
BEGIN_HA3_NAMESPACE(common);

class AggregateDescriptionTest : public TESTBASE {
public:
    AggregateDescriptionTest();
    ~AggregateDescriptionTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, AggregateDescriptionTest);


AggregateDescriptionTest::AggregateDescriptionTest() { 
}

AggregateDescriptionTest::~AggregateDescriptionTest() { 
}

void AggregateDescriptionTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void AggregateDescriptionTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

END_HA3_NAMESPACE(common);
