#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/ReferenceIdManager.h>
#include <autil/StringUtil.h>

using namespace std;

BEGIN_HA3_NAMESPACE(common);

class ReferenceIdManagerTest : public TESTBASE {
public:
    ReferenceIdManagerTest();
    ~ReferenceIdManagerTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, ReferenceIdManagerTest);


ReferenceIdManagerTest::ReferenceIdManagerTest() { 
}

ReferenceIdManagerTest::~ReferenceIdManagerTest() { 
}

void ReferenceIdManagerTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void ReferenceIdManagerTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(ReferenceIdManagerTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");

    ReferenceIdManager refIdManager;
    for (uint32_t i = 1; i <= ReferenceIdManager::MAX_REFERENCE_ID; i++)
    {
        string refName(autil::StringUtil::toString(i));
        ASSERT_EQ(i, refIdManager.requireReferenceId(refName));
    }
    uint32_t id = refIdManager.requireReferenceId(
            autil::StringUtil::toString(ReferenceIdManager::MAX_REFERENCE_ID + 1));
    ASSERT_TRUE(!ReferenceIdManager::isReferenceIdValid(id));    
}

END_HA3_NAMESPACE(common);

