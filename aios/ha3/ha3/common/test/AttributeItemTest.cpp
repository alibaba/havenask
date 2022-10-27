#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/AttributeItem.h>
#include <vector>

using namespace std;
USE_HA3_NAMESPACE(util);
BEGIN_HA3_NAMESPACE(common);

class AttributeItemTest : public TESTBASE {
public:
    AttributeItemTest();
    ~AttributeItemTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, AttributeItemTest);


AttributeItemTest::AttributeItemTest() { 
}

AttributeItemTest::~AttributeItemTest() { 
}

void AttributeItemTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void AttributeItemTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(AttributeItemTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");

    AttributeItemTyped<int32_t> attrInt32;
    attrInt32.setItem(1);
    ASSERT_EQ(1, attrInt32.getItem());
    ASSERT_EQ(vt_int32, attrInt32.getType());
    ASSERT_EQ(false, attrInt32.isMultiValue());

    AttributeItemTyped<string> attrString;
    attrString.setItem("string attribute");
    ASSERT_EQ(string("string attribute"), attrString.getItem());
    ASSERT_EQ(vt_string, attrString.getType());
    ASSERT_EQ(false, attrString.isMultiValue());

    AttributeItemTyped<int64_t> *attrInt64p = new AttributeItemTyped<int64_t>();
    AttributeItemPtr attrPtr(attrInt64p);
    attrInt64p->setItem(100);
    ASSERT_EQ((int64_t)100, attrInt64p->getItem());
    ASSERT_EQ(vt_int64, attrPtr->getType());
    ASSERT_EQ(false, attrPtr->isMultiValue());

    autil::DataBuffer dataBuffer;
    dataBuffer.write(attrPtr);
    AttributeItemPtr attrItemPtr;
    dataBuffer.read(attrItemPtr);
    ASSERT_EQ(attrPtr->getType(), attrItemPtr->getType());
    ASSERT_EQ(attrPtr->isMultiValue(), attrItemPtr->isMultiValue());

    AttributeItemTyped<int64_t> *attrItemp = (AttributeItemTyped<int64_t>*)attrItemPtr.get();
    ASSERT_EQ(attrInt64p->getItem(), attrItemp->getItem());
    
    AttributeItemTyped<string> *pAttrString = new AttributeItemTyped<string>();
    pAttrString->setItem("string attribute 2");
    AttributeItemPtr attrStringPtr(pAttrString);
    autil::DataBuffer dataBuffer2;
    dataBuffer2.write(attrStringPtr);
    AttributeItemPtr attrStringPtr2;
    dataBuffer2.read(attrStringPtr2);
    ASSERT_EQ(attrStringPtr2->getType(), pAttrString->getType());
    AttributeItemTyped<string> *pAttrString2 = (AttributeItemTyped<string>*)attrStringPtr2.get();
    ASSERT_EQ(pAttrString2->getItem(), pAttrString->getItem());
}

TEST_F(AttributeItemTest, testMultiValue) { 
    HA3_LOG(DEBUG, "Begin Test!");

    AttributeItemTyped<vector<int32_t> > attrInt32;
    vector<int32_t> int32Vector;
    int32Vector.push_back(1);
    int32Vector.push_back(2);
    attrInt32.setItem(int32Vector);
    ASSERT_EQ((size_t)2, attrInt32.getItem().size());
    ASSERT_EQ(1, attrInt32.getItem()[0]);
    ASSERT_EQ(2, attrInt32.getItem()[1]);
    ASSERT_EQ(vt_int32, attrInt32.getType());
    ASSERT_EQ(true, attrInt32.isMultiValue());

    // AttributeItemTyped<vector<string> > attrString;
    // vector<string> stringVector;
    // stringVector.push_back("string1");
    // stringVector.push_back("string2");
    // stringVector.push_back("string3");
    // attrString.setItem(stringVector);
    // ASSERT_EQ((size_t)3, attrString.getItem().size());
    // ASSERT_EQ(string("string3"), attrString.getItem()[2]);
    // ASSERT_EQ(vt_string, attrString.getType());
    // ASSERT_EQ(true, attrString.isMultiValue());

    AttributeItemTyped<vector<int64_t> > *attrInt64p = new AttributeItemTyped<vector<int64_t> >();
    AttributeItemPtr attrPtr(attrInt64p);
    attrInt64p->setItem(vector<int64_t>(3, 100));
    ASSERT_EQ((int64_t)100, attrInt64p->getItem()[0]);
    ASSERT_EQ(vt_int64, attrPtr->getType());
    ASSERT_EQ(true, attrPtr->isMultiValue());

    autil::DataBuffer dataBuffer;
    dataBuffer.write(attrPtr);
    AttributeItemPtr attrItemPtr;
    dataBuffer.read(attrItemPtr);
    ASSERT_EQ(attrPtr->getType(), attrItemPtr->getType());
    ASSERT_EQ(attrPtr->isMultiValue(), attrItemPtr->isMultiValue());

    AttributeItemTyped<vector<int64_t> > *attrItemp = (AttributeItemTyped<vector<int64_t> >*)attrItemPtr.get();
    ASSERT_TRUE(attrInt64p->getItem() == attrItemp->getItem());
    
    // AttributeItemTyped<vector<string> > *pAttrString = new AttributeItemTyped<vector<string> >();
    // pAttrString->setItem(vector<string>(3, "string attribute 2"));
    // AttributeItemPtr attrStringPtr(pAttrString);
    // autil::DataBuffer dataBuffer2;
    // dataBuffer2.write(attrStringPtr);
    // AttributeItemPtr attrStringPtr2;
    // dataBuffer2.read(attrStringPtr2);
    // ASSERT_EQ(attrStringPtr2->getType(), pAttrString->getType());
    // ASSERT_EQ(attrStringPtr2->isMultiValue(), pAttrString->isMultiValue());
    // AttributeItemTyped<vector<string> > *pAttrString2 = (AttributeItemTyped<vector<string> >*)attrStringPtr2.get();
    // ASSERT_TRUE(pAttrString2->getItem() == pAttrString->getItem());
}

END_HA3_NAMESPACE(common);

