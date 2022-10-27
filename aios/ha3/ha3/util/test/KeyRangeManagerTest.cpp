#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/util/KeyRangeManager.h>
using namespace std;
BEGIN_HA3_NAMESPACE(util);

class KeyRangeManagerTest : public TESTBASE {
public:
    KeyRangeManagerTest();
    ~KeyRangeManagerTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

 HA3_LOG_SETUP(util, KeyRangeManagerTest);


KeyRangeManagerTest::KeyRangeManagerTest() { 
}

KeyRangeManagerTest::~KeyRangeManagerTest() { 
}

void KeyRangeManagerTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void KeyRangeManagerTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(KeyRangeManagerTest, testSimpleProcess) {
    int partCount = 4;
    AutoHashKeyRangeManager hashKeyManager(partCount);
    
    string keys2[] = {"", "\0", "\3", "AA", "AXXX", "aaa", "zzz"};
    int indices2[] = {0, 0, 0, 2, 2, 3, 3};
    for (uint32_t i = 0; i < sizeof(keys2) / sizeof(keys2[0]); ++i)
    {
        int index = hashKeyManager.GetRangeIndexByKey(keys2[i]);
        ASSERT_EQ(index, indices2[i]);
    }
}

END_HA3_NAMESPACE(util);

