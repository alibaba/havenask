#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/AttributeClause.h>
#include <ha3/test/test.h>
#include <ha3_sdk/testlib/qrs/RequestCreator.h>

using namespace std;

USE_HA3_NAMESPACE(util);
BEGIN_HA3_NAMESPACE(common);

class AttributeClauseTest : public TESTBASE {
public:
    AttributeClauseTest();
    ~AttributeClauseTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, AttributeClauseTest);


AttributeClauseTest::AttributeClauseTest() { 
}

AttributeClauseTest::~AttributeClauseTest() { 
}

void AttributeClauseTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void AttributeClauseTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(AttributeClauseTest, testSerializeAndDeserialize) { 
    HA3_LOG(DEBUG, "Begin Test!");

    AttributeClause attributeClause;    
    AttributeClause attributeClause2;
    string originalString = "attr1, attr2";
    string str[2] = {"attr1", "attr2"};

    attributeClause.setOriginalString(originalString);
    attributeClause.setAttributeNames(str, str + 2); 

    autil::DataBuffer buffer;
    buffer.write(attributeClause);
    buffer.read(attributeClause2);

    ASSERT_EQ(originalString, attributeClause2.getOriginalString());
    set<string> attrNames = attributeClause2.getAttributeNames();
    ASSERT_TRUE(attrNames.size() == 2);
    ASSERT_TRUE(attrNames.find("attr1") != attrNames.end());
    ASSERT_TRUE(attrNames.find("attr2") != attrNames.end());
}

TEST_F(AttributeClauseTest, testToString) {
    string requestStr = "attribute=attr1,attr3,attr2";
    RequestPtr requestPtr = qrs::RequestCreator::prepareRequest(requestStr);
    ASSERT_TRUE(requestPtr);
    AttributeClause* attributeClause 
        = requestPtr->getAttributeClause();
    ASSERT_EQ(string("attr1|attr2|attr3|"), attributeClause->toString());

}

END_HA3_NAMESPACE(common);

