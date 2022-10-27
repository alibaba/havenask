#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/AttributeExtractor.h>

BEGIN_HA3_NAMESPACE(common);

class AttributeExtractorTest : public TESTBASE {
public:
    AttributeExtractorTest();
    ~AttributeExtractorTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, AttributeExtractorTest);


AttributeExtractorTest::AttributeExtractorTest() { 
}

AttributeExtractorTest::~AttributeExtractorTest() { 
}

void AttributeExtractorTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void AttributeExtractorTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(AttributeExtractorTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");

    AttributeContainerTyped<int32_t> container;
    container.push(1);
    container.push(2);
    container.push(3);

    Hit hit0(0);
    hit0.setAttributeOffset(0);
    Hit hit1(1);
    hit1.setAttributeOffset(2);
    Hit hit2(2);
    hit2.setAttributeOffset(1);

    AttributeExtractor<int32_t> extractor(&container);

    ASSERT_EQ(1, extractor.extractAttribute(hit0));
    ASSERT_EQ(3, extractor.extractAttribute(hit1));
    ASSERT_EQ(2, extractor.extractAttribute(hit2));
}

END_HA3_NAMESPACE(common);

