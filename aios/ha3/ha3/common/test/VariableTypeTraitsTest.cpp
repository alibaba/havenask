#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/framework/VariableTypeTraits.h>

BEGIN_HA3_NAMESPACE(common);

class VariableTypeTraitsTest : public TESTBASE {
public:
    VariableTypeTraitsTest();
    ~VariableTypeTraitsTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, VariableTypeTraitsTest);


VariableTypeTraitsTest::VariableTypeTraitsTest() { 
}

VariableTypeTraitsTest::~VariableTypeTraitsTest() { 
}

void VariableTypeTraitsTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void VariableTypeTraitsTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(VariableTypeTraitsTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");

    //ASSERT_TRUE(false);
    //ASSERT_EQ(0, 1);
}

END_HA3_NAMESPACE(common);

