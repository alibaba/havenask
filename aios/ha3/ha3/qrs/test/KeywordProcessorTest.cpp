#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/test/KeywordFilterProcessor.h>
#include <ha3/qrs/test/KeywordReplaceProcessor.h>
using namespace std;
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(qrs);

class KeywordProcessorTest : public TESTBASE {
public:
    KeywordProcessorTest();
    ~KeywordProcessorTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(qrs, KeywordProcessorTest);


KeywordProcessorTest::KeywordProcessorTest() { 
}

KeywordProcessorTest::~KeywordProcessorTest() { 
}

void KeywordProcessorTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void KeywordProcessorTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(KeywordProcessorTest, testKeyWordFilter) { 
    HA3_LOG(DEBUG, "Begin Test!");
    KeywordFilterProcessor processor;
    RequestPtr requestPtr(new Request);
    ResultPtr resultPtr(new Result);

    requestPtr->setOriginalString("config=cluster:daogou&&query=phrase:none");
    processor.process(requestPtr, resultPtr);
    ASSERT_EQ(string("config=cluster:daogou&&query=phrase:none"), 
                         requestPtr->getOriginalString());

    requestPtr->setOriginalString("config=cluster:daogou&&query=phrase:中国的中国人的老虎abcd");
    processor.process(requestPtr, resultPtr);
    ASSERT_EQ(string("config=cluster:daogou&&query=phrase:的人的abcd"), 
                         requestPtr->getOriginalString());
}

TEST_F(KeywordProcessorTest, testKeyWordReplace) { 
    HA3_LOG(DEBUG, "Begin Test!");
    KeywordReplaceProcessor processor;
    RequestPtr requestPtr(new Request);
    ResultPtr resultPtr(new Result);

    requestPtr->setOriginalString("config=cluster:daogou&&query=phrase:none");
    processor.process(requestPtr, resultPtr);
    ASSERT_EQ(string("config=cluster:daogou&&query=phrase:none"), 
                         requestPtr->getOriginalString());

    requestPtr->setOriginalString("config=cluster:daogou&&query=phrase:\'boy and girl huangruihua 黄色 黄色 huangrh \'");
    processor.process(requestPtr, resultPtr);
    ASSERT_EQ(string("config=cluster:daogou&&query=phrase:\'girl and girl 黄锐华 白色 白色 黄锐华 \'"),
                         requestPtr->getOriginalString());
}

END_HA3_NAMESPACE(qrs);

