#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/config/GenerationMeta.h>
#include <ha3/test/test.h>

using namespace std;
BEGIN_HA3_NAMESPACE(config);

class GenerationMetaTest : public TESTBASE {
public:
    GenerationMetaTest();
    ~GenerationMetaTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(config, GenerationMetaTest);


GenerationMetaTest::GenerationMetaTest() { 
}

GenerationMetaTest::~GenerationMetaTest() { 
}

void GenerationMetaTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void GenerationMetaTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(GenerationMetaTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");

    string indexRoot = string(TEST_DATA_PATH) + "/index_root";
    GenerationMeta gm;
    ASSERT_TRUE(gm.loadFromFile(indexRoot));
    ASSERT_EQ(string("value1"), gm.getValue("key1"));
    ASSERT_EQ(string(""), gm.getValue("key2"));
    ASSERT_EQ(string("value3"), gm.getValue("key3"));

    // compatible mode, in generation dir
    GenerationMeta gm2;
    indexRoot = string(TEST_DATA_PATH) + "/zookeeper";
    ASSERT_TRUE(gm2.loadFromFile(indexRoot));
    ASSERT_EQ(string("value"), gm2.getValue("key"));
    ASSERT_EQ(string(""), gm2.getValue("key1"));
    ASSERT_EQ(string("value2"), gm2.getValue("key2"));
}

END_HA3_NAMESPACE(config);

