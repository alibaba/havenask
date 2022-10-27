#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/AggFunDescription.h>
#include <string>

using namespace std;
BEGIN_HA3_NAMESPACE(common);

class AggFunDescriptionTest : public TESTBASE {
public:
    AggFunDescriptionTest();
    ~AggFunDescriptionTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, AggFunDescriptionTest);


AggFunDescriptionTest::AggFunDescriptionTest() { 
}

AggFunDescriptionTest::~AggFunDescriptionTest() { 
}

void AggFunDescriptionTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void AggFunDescriptionTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(AggFunDescriptionTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");
}

END_HA3_NAMESPACE(common);

