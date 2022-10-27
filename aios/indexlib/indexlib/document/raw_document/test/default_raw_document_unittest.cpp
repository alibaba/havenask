#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/document/raw_document/default_raw_document.h"
#include "indexlib/document/raw_document/raw_document_define.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(document);

class DefaultRawDocumentTest : public INDEXLIB_TESTBASE {
public:
    void CaseSetUp();
    void CaseTearDown();
    
private:
    KeyMapManagerPtr hashMapManager;
    size_t _maxKeySize = 1000;
};


void DefaultRawDocumentTest::CaseSetUp() {
    _maxKeySize = 4096;
    hashMapManager.reset(new KeyMapManager(_maxKeySize));
}

void DefaultRawDocumentTest::CaseTearDown() {
}

TEST_F(DefaultRawDocumentTest, testDefaultConstruct) {
    DefaultRawDocument rawDoc;
    string key1("ha3_base_key_field"),value1("123456789yuan");
    string key2("key_doc0"),value2("some_value");
    EXPECT_EQ(string((const char *)NULL, (size_t)0), rawDoc.getField(key1));
    rawDoc.setField(key1,value1);
    rawDoc.setField(key2,value2);
    EXPECT_EQ(value1, rawDoc.getField(key1));
    EXPECT_EQ(value2, rawDoc.getField(key2));
    EXPECT_EQ(size_t(2),rawDoc.getFieldCount());
}

TEST_F(DefaultRawDocumentTest, testCopyConstruct) {
    DefaultRawDocument rawDoc1(hashMapManager);
    rawDoc1.setDocTimestamp(100);
    rawDoc1.setDocSource("123");
    rawDoc1.setField("field1", "field1_value");
    rawDoc1.setField("field2", "field2_value");

    common::IndexLocator locator(1, 2);
    rawDoc1.setLocator(locator);
    DefaultRawDocument rawDoc2(rawDoc1);

    ASSERT_TRUE(rawDoc1.getField("field1") == rawDoc2.getField("field1"));
    ASSERT_TRUE(rawDoc1.getField("field2") == rawDoc2.getField("field2"));

    ASSERT_TRUE(rawDoc1.getDocOperateType() == rawDoc2.getDocOperateType());
    ASSERT_TRUE(rawDoc1.getDocTimestamp() == rawDoc2.getDocTimestamp());
    ASSERT_TRUE(rawDoc1.getLocator() == rawDoc2.getLocator());
    ASSERT_TRUE(rawDoc1.getDocSource() == rawDoc2.getDocSource());
    ASSERT_EQ(locator, rawDoc2.getLocator());
}

TEST_F(DefaultRawDocumentTest, testSimpleGetSet) {
    string key1("ha3_base_key_field"),value1("123456789yuan");
    string key2("key_doc0"),value2("some_value");
    string key3("key_doc1"),value3("another_one");
    string key4("key_doc2"),value4("");

    // test get/set of doc0
    DefaultRawDocument *rawDoc0 = new DefaultRawDocument(hashMapManager);
    EXPECT_EQ(size_t(0), rawDoc0->mHashMapPrimary->size());
    EXPECT_EQ(string((const char *)NULL, (size_t)0), rawDoc0->getField(key1));
    rawDoc0->setField(key1,value1);
    rawDoc0->setField(key2,value2);
    EXPECT_EQ(value1, rawDoc0->getField(key1));
    EXPECT_EQ(value2, rawDoc0->getField(key2));
    EXPECT_EQ(size_t(2),rawDoc0->getFieldCount());
    delete rawDoc0;

    // test update primary, test get/set of doc1.
    DefaultRawDocument *rawDoc1 = new DefaultRawDocument(hashMapManager);
    EXPECT_EQ(string((const char *)NULL, (size_t)0), rawDoc1->getField(key3));
    EXPECT_EQ(size_t(2), rawDoc1->mHashMapPrimary->size());
    rawDoc1->setField(key1,value1);
    rawDoc1->setField(key3,value3);
    rawDoc1->setField(key4,value4);
    EXPECT_EQ(value1, rawDoc1->getField(key1));
    EXPECT_EQ(value3, rawDoc1->getField(key3));
    EXPECT_EQ(size_t(3),rawDoc1->getFieldCount());
    // test exist
    EXPECT_TRUE(rawDoc1->exist(key1));
    EXPECT_FALSE(rawDoc1->exist(key2));
    EXPECT_TRUE(rawDoc1->exist(key3));
    EXPECT_TRUE(rawDoc1->exist(key4));
    rawDoc1->eraseField(key4);
    EXPECT_EQ(size_t(2),rawDoc1->getFieldCount());
    EXPECT_FALSE(rawDoc1->exist(key4));

    for (size_t i = 0; i < _maxKeySize; ++i)
    {
        rawDoc1->setField(string("key")
                        + StringUtil::toString(i + 2), value2);
    }
    //update primary
    delete rawDoc1;

    DefaultRawDocument *rawDoc2 = new DefaultRawDocument(hashMapManager);
    EXPECT_EQ(size_t(4 + _maxKeySize), rawDoc2->mHashMapPrimary->size());
    EXPECT_EQ(rawDoc2->mFieldsPrimary.size(), rawDoc2->mHashMapPrimary->size());
    EXPECT_EQ(size_t(0), rawDoc2->mHashMapIncrement->size());
    EXPECT_EQ(rawDoc2->mFieldsIncrement.size(), rawDoc2->mHashMapIncrement->size());
    EXPECT_EQ(size_t(0), rawDoc2->mFieldCount);

    bool res = true;
    for (size_t i = 0; i < _maxKeySize; ++i) {
        res = res && (string((const char *)NULL, (size_t)0) == rawDoc2->getField(string("key") +
                        StringUtil::toString(i + 2)));
    }
    EXPECT_TRUE(res);

    for (size_t i = 0; i < _maxKeySize; ++i)
    {
        rawDoc2->setField(ConstString(string("key")
                        + StringUtil::toString(i + 2)), ConstString(value2));
    }
    EXPECT_EQ(size_t(0), rawDoc2->mHashMapIncrement->size());
    EXPECT_EQ(rawDoc2->mFieldsIncrement.size(), 
              rawDoc2->mHashMapIncrement->size());
    EXPECT_EQ(size_t(_maxKeySize), rawDoc2->mFieldCount);
    //update primary
    delete rawDoc2;

    DefaultRawDocument *rawDoc3 = new DefaultRawDocument(hashMapManager);
    EXPECT_EQ(size_t(4 + _maxKeySize), rawDoc3->mHashMapPrimary->size());
    EXPECT_EQ(rawDoc3->mFieldsPrimary.size(), rawDoc3->mHashMapPrimary->size());
    EXPECT_EQ(size_t(0), rawDoc3->mHashMapIncrement->size());
    EXPECT_EQ(rawDoc3->mFieldsIncrement.size(), rawDoc3->mHashMapIncrement->size());
    EXPECT_EQ(size_t(0), rawDoc3->mFieldCount);

    for (size_t i = 0; i < _maxKeySize; ++i)
    {
        rawDoc3->setField(ConstString(string("new_key")
                        + StringUtil::toString(i + 2)), ConstString(value2));
    }
    EXPECT_EQ(_maxKeySize, rawDoc3->mHashMapIncrement->size());
    EXPECT_EQ(rawDoc3->mFieldsIncrement.size(), rawDoc3->mHashMapIncrement->size());
    EXPECT_EQ(size_t(_maxKeySize), rawDoc3->mFieldCount);
    delete rawDoc3;

    DefaultRawDocument *rawDoc4 = new DefaultRawDocument(hashMapManager);
    EXPECT_FALSE(rawDoc4->mHashMapPrimary);
    EXPECT_EQ(size_t(0), rawDoc4->mFieldsPrimary.size());
    EXPECT_EQ(size_t(0), rawDoc4->mHashMapIncrement->size());
    EXPECT_EQ(rawDoc4->mFieldsIncrement.size(), 
              rawDoc4->mHashMapIncrement->size());
    EXPECT_EQ(size_t(0), rawDoc4->mFieldCount);

    for (size_t i = 0; i < _maxKeySize; ++i)
    {
        rawDoc4->setField(ConstString(string("new_key")
                        + StringUtil::toString(i + 2)), ConstString(value2));
    }

    EXPECT_EQ(_maxKeySize, rawDoc4->mHashMapIncrement->size());
    EXPECT_EQ(rawDoc4->mFieldsIncrement.size(), rawDoc4->mHashMapIncrement->size());
    EXPECT_EQ(_maxKeySize, rawDoc4->mFieldCount);
    rawDoc4->toString();

    for (size_t i = 0; i < _maxKeySize; ++i)
    {
        EXPECT_EQ(ConstString(value2), rawDoc4->getField(ConstString(string("new_key")
                                + StringUtil::toString(i + 2))));
    }
    
    delete rawDoc4;
}

TEST_F(DefaultRawDocumentTest, testGetField) {
    DefaultRawDocument rawDoc(hashMapManager);
    for (size_t i = 0; i < 100; ++i) {
        string fieldName = "field_name_" + StringUtil::toString(i);
        string fieldValue = "field_value_" + StringUtil::toString(i);
        rawDoc.setField(fieldName.data(), fieldName.size(),
                           fieldValue.data(), fieldValue.size());
    }
    EXPECT_EQ(size_t(100),rawDoc.getFieldCount());

    for (size_t i = 0; i < 100; ++i) {
        string fieldName = "field_name_" + StringUtil::toString(i);
        string fieldValue = "field_value_" + StringUtil::toString(i);
        EXPECT_EQ(fieldValue, rawDoc.getField(fieldName));
    }
    EXPECT_EQ(size_t(100),rawDoc.getFieldCount());

    for (size_t i = 0; i < 100; ++i) {
        string fieldName = "field_name_" + StringUtil::toString(i);
        string fieldValue = "field_value_" + StringUtil::toString(i);
        EXPECT_EQ(fieldValue, rawDoc.getField(fieldName));
    }
    EXPECT_EQ(size_t(100),rawDoc.getFieldCount());

    for (size_t i = 100; i < 200; ++i) {
        string fieldName = "field_name_" + StringUtil::toString(i);
        string fieldValue = "field_value_" + StringUtil::toString(i);
        rawDoc.setField(fieldName.data(), fieldName.size(),
                           fieldValue.data(), fieldValue.size());
    }
    EXPECT_EQ(size_t(200),rawDoc.getFieldCount());

    for (size_t i = 0; i < 200; ++i) {
        string fieldName = "field_name_" + StringUtil::toString(i);
        string fieldValue = "field_value_" + StringUtil::toString(i);
        EXPECT_EQ(fieldValue, rawDoc.getField(fieldName));
    }
    EXPECT_EQ(size_t(200),rawDoc.getFieldCount());
}

TEST_F(DefaultRawDocumentTest, testSetField) {
    DefaultRawDocument rawDoc(hashMapManager);
    for (size_t i = 0; i < 100; ++i) {
        string fieldName = "field_name_" + StringUtil::toString(i);
        string fieldValue = "field_value_" + StringUtil::toString(i);
        rawDoc.setField(fieldName.data(), fieldName.size(),
                        fieldValue.data(), fieldValue.size());
    }
    EXPECT_EQ(size_t(100),rawDoc.getFieldCount());

    for (size_t i = 100; i < 200; ++i) {
        string fieldName = "field_name_" + StringUtil::toString(i);
        string fieldValue = "field_value_" + StringUtil::toString(i);
        rawDoc.setField(fieldName.data(), fieldName.size(),
                        fieldValue.data(), fieldValue.size());
    }
    EXPECT_EQ(size_t(200),rawDoc.getFieldCount());

    for (size_t i = 0; i < 200; ++i) {
        string fieldName = "field_name_" + StringUtil::toString(i);
        string fieldValue = "field_value_" + StringUtil::toString(i);
        EXPECT_EQ(fieldValue, rawDoc.getField(fieldName));
    }
    EXPECT_EQ(size_t(200),rawDoc.getFieldCount());

    rawDoc.setField(string("field_name_50"), string("set_value1"));
    EXPECT_EQ(string("set_value1"), rawDoc.getField("field_name_50"));
    rawDoc.setField(string("field_name_150"), string("set_value2"));
    EXPECT_EQ(string("set_value2"), rawDoc.getField("field_name_150"));

    EXPECT_EQ(size_t(200),rawDoc.getFieldCount());
}

TEST_F(DefaultRawDocumentTest, testToString) {
    DefaultRawDocument rawDoc(hashMapManager);
    rawDoc.setField("field1", "field1_value");
    rawDoc.setField("field2", "field2_value");
    rawDoc.setField("field3", "field3_value");
    string rawDocStr = "field1 = field1_value\n"
                       "field2 = field2_value\n"
                       "field3 = field3_value\n";
    EXPECT_EQ(rawDocStr, rawDoc.toString());
}

IE_NAMESPACE_END(document);
