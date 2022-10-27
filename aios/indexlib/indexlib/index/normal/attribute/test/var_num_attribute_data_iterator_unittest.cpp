#include "indexlib/index/normal/attribute/test/var_num_attribute_data_iterator_unittest.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_accessor.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, VarNumAttributeDataIteratorTest);

VarNumAttributeDataIteratorTest::VarNumAttributeDataIteratorTest()
{
}

VarNumAttributeDataIteratorTest::~VarNumAttributeDataIteratorTest()
{
}

void VarNumAttributeDataIteratorTest::CaseSetUp()
{
}

void VarNumAttributeDataIteratorTest::CaseTearDown()
{
}

void VarNumAttributeDataIteratorTest::TestNext()
{
    uint64_t dataLength;
    uint8_t* data;
    uint64_t offset;
    //normal case
    {
        const char* expectedData = "hello";
        VarNumAttributeDataIteratorPtr iter = PrepareIterator(expectedData, 5);

        for (size_t i = 0; i < 5; i++)
        {
            ASSERT_TRUE(iter->HasNext());
            iter->Next();
            iter->GetCurrentData(dataLength, data);
            iter->GetCurrentOffset(offset);
            ASSERT_EQ((uint64_t)1, dataLength);
            ASSERT_EQ((uint64_t)5*i, offset);    
            ASSERT_EQ(expectedData[i], data[0]);
        }
        
        iter->Next();
        iter->GetCurrentData(dataLength, data);
        iter->GetCurrentOffset(offset);

        ASSERT_EQ((uint64_t)0, dataLength);
        ASSERT_EQ((uint64_t)0, offset);    
        ASSERT_TRUE(!data);
    }

    {
        //abnormal case
        VarNumAttributeDataIteratorPtr iter = PrepareIterator("", 0);
        iter->Next();
        iter->GetCurrentData(dataLength, data);
        iter->GetCurrentOffset(offset);

        ASSERT_EQ((uint64_t)0, dataLength);
        ASSERT_EQ((uint64_t)0, offset);    
        ASSERT_TRUE(!data);
    }
}

void VarNumAttributeDataIteratorTest::TestHasNext()
{
    {
        //normal case
        const char* expectedData = "he";
        VarNumAttributeDataIteratorPtr iter = PrepareIterator(expectedData, 2);
        ASSERT_TRUE(iter->HasNext());
        iter->Next();

        ASSERT_TRUE(iter->HasNext());
        iter->Next();

        ASSERT_FALSE(iter->HasNext());
    }

    {
        //no data
        VarNumAttributeDataIteratorPtr iter = PrepareIterator("", 0);
        ASSERT_FALSE(iter->HasNext());
    }

}

VarNumAttributeDataIteratorPtr VarNumAttributeDataIteratorTest::PrepareIterator(
        const char* data, size_t length)
{
    VarNumAttributeAccessor accessor;
    accessor.Init(&mPool);
    for (size_t i = 0; i < length; i++)
    {
        string value;
        value.push_back(data[i]);
        common::AttrValueMeta attrMeta(1, value);
        accessor.AddNormalField(attrMeta);
    }
    return accessor.CreateVarNumAttributeDataIterator();
}

IE_NAMESPACE_END(index);

