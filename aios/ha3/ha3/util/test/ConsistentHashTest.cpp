#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/util/ConsistentHash.h>

using namespace std;
BEGIN_HA3_NAMESPACE(util);

class ConsistentHashTest : public TESTBASE {
public:
    ConsistentHashTest();
    ~ConsistentHashTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(util, ConsistentHashTest);


ConsistentHashTest::ConsistentHashTest() { 
}

ConsistentHashTest::~ConsistentHashTest() { 
}

void ConsistentHashTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void ConsistentHashTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(ConsistentHashTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");
    ConsistentHash<string> hash(20);
    hash.add(0, "abc");
    ASSERT_EQ((size_t)20, hash.size());
    hash.add(1, "abc");
    ASSERT_EQ((size_t)40, hash.size());
    hash.add(0, "abcd");
    ASSERT_EQ((size_t)60, hash.size());

    hash.construct();
    
    string value1 = (hash.get(0x87654321))->second;
    ASSERT_TRUE(value1 == "abc" || value1 == "abcd");
    
    string value2 = (hash.get(0x12345678))->second;
    ASSERT_TRUE(value2 == "abc" || value2 == "abcd");
}

END_HA3_NAMESPACE(util);

