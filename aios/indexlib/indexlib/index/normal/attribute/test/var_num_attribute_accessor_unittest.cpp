#include "indexlib/index/normal/attribute/test/var_num_attribute_accessor_unittest.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, VarNumAttributeAccessorTest);

VarNumAttributeAccessorTest::VarNumAttributeAccessorTest()
{
    
}

VarNumAttributeAccessorTest::~VarNumAttributeAccessorTest()
{
}

void VarNumAttributeAccessorTest::CaseSetUp()
{
}

void VarNumAttributeAccessorTest::CaseTearDown()
{
}

void VarNumAttributeAccessorTest::TestAddEncodedField()
{
    VarNumAttributeAccessor accessor;
    accessor.Init(&mPool);
    VarNumAttributeAccessor::EncodeMapPtr encodeMap(
            new VarNumAttributeAccessor::EncodeMap(HASHMAP_INIT_SIZE));
    ConstString str("1", 1);
    common::AttrValueMeta attrMeta(1, str);
    accessor.AddEncodedField(attrMeta, encodeMap);
    ASSERT_EQ((size_t)1, accessor.mOffsets->Size());
    ASSERT_EQ((size_t)1, encodeMap->Size());
    ASSERT_EQ((*(accessor.mOffsets))[0], *(encodeMap->Find(1)));
    ASSERT_EQ((uint64_t)0, (uint64_t)(*(accessor.mOffsets))[0]);

    accessor.AddEncodedField(attrMeta, encodeMap);
    ASSERT_EQ((size_t)2, accessor.mOffsets->Size());
    ASSERT_EQ((size_t)1, encodeMap->Size());
    ASSERT_EQ((uint64_t)0, (uint64_t)(*(accessor.mOffsets))[1]);

    attrMeta.hashKey = 2;
    attrMeta.data.reset("2", 1);
    accessor.AddEncodedField(attrMeta, encodeMap);
    ASSERT_EQ((size_t)3, accessor.mOffsets->Size());
    ASSERT_EQ((size_t)2, encodeMap->Size());
    ASSERT_EQ((*(accessor.mOffsets))[2], *(encodeMap->Find(2)));
    //sizeof(uint32_t) + datasize
    ASSERT_EQ((uint64_t)5, (uint64_t)(*(accessor.mOffsets))[2]);
}

void VarNumAttributeAccessorTest::TestAddNormalField()
{
    VarNumAttributeAccessor accessor;
    accessor.Init(&mPool);

    ConstString str("1", 1);
    common::AttrValueMeta attrMeta(1, str);

    accessor.AddNormalField(attrMeta);
    ASSERT_EQ((size_t)1, accessor.mOffsets->Size());
    ASSERT_EQ((uint64_t)0, (uint64_t)(*(accessor.mOffsets))[0]);

    accessor.AddNormalField(attrMeta);
    ASSERT_EQ((size_t)2, accessor.mOffsets->Size());
    ASSERT_EQ((uint64_t)5, (uint64_t)(*(accessor.mOffsets))[1]);
}

void VarNumAttributeAccessorTest::TestReadData()
{
    VarNumAttributeAccessor accessor;
    accessor.Init(&mPool);

    ConstString str("1", 1);
    common::AttrValueMeta attrMeta(1, str);
    accessor.AddNormalField(attrMeta);
    
    uint8_t* data = NULL;
    uint32_t dataLength = 0;
    accessor.ReadData((docid_t)0, data, dataLength);
    ASSERT_EQ((uint32_t)1, dataLength);
    ASSERT_EQ((char)'1', data[0]);

    //test read non exist doc
    accessor.ReadData((docid_t)1, data, dataLength);
    ASSERT_EQ((uint32_t)0, dataLength);
    ASSERT_TRUE(!data);
}

void VarNumAttributeAccessorTest::TestUpdateNormalField()
{
    VarNumAttributeAccessor accessor;
    accessor.Init(&mPool);
    //docid: 0, value 1
    //docid: 1, value 1
    ConstString str("1", 1);
    common::AttrValueMeta attrMeta(1, str);
    accessor.AddNormalField(attrMeta);

    attrMeta.data.reset("12", 2);
    accessor.UpdateNormalField((docid_t)0, attrMeta);
    ASSERT_EQ((size_t)1, accessor.mOffsets->Size());
    ASSERT_EQ((uint64_t)5, (*(accessor.mOffsets))[0]);

    attrMeta.data.reset("25", 2);
    accessor.UpdateNormalField((docid_t)0, attrMeta);
    ASSERT_EQ((size_t)1, accessor.mOffsets->Size());
    ASSERT_EQ((uint64_t)11, (*(accessor.mOffsets))[0]);
}

void VarNumAttributeAccessorTest::TestUpdateEncodedField()
{
    VarNumAttributeAccessor accessor;
    accessor.Init(&mPool);
    VarNumAttributeAccessor::EncodeMapPtr encodeMap(
            new VarNumAttributeAccessor::EncodeMap(HASHMAP_INIT_SIZE));
    //docid: 0, value 1
    //docid: 1, value 1
    ConstString str("1", 1);
    common::AttrValueMeta attrMeta(1, str);
    accessor.AddEncodedField(attrMeta, encodeMap);
    accessor.AddEncodedField(attrMeta, encodeMap);

    //docid: 2, value 2
    //begin offset 5
    attrMeta.hashKey = 2;
    attrMeta.data.reset("2", 1);
    accessor.AddEncodedField(attrMeta, encodeMap);
    
    //update: doc2 with uniq value
    attrMeta.hashKey = 1;
    attrMeta.data.reset("1", 1);
    accessor.UpdateEncodedField((docid_t)2, attrMeta, encodeMap);
    ASSERT_EQ((size_t)3, accessor.mOffsets->Size());
    ASSERT_EQ((uint64_t)0, (*(accessor.mOffsets))[2]);

    //update: doc1 with uniq value
    attrMeta.hashKey = 2;
    attrMeta.data.reset("2", 1);
    accessor.UpdateEncodedField((docid_t)1, attrMeta, encodeMap);
    ASSERT_EQ((size_t)3, accessor.mOffsets->Size());
    ASSERT_EQ((uint64_t)5, (*(accessor.mOffsets))[1]);

    //update: doc0 without uniq value
    attrMeta.hashKey = 3;
    attrMeta.data.reset("3", 1);
    accessor.UpdateEncodedField((docid_t)0, attrMeta, encodeMap);
    ASSERT_EQ((size_t)3, encodeMap->Size());
    ASSERT_EQ((uint64_t)10, *(encodeMap->Find(3)));
    ASSERT_EQ((size_t)3, accessor.mOffsets->Size());
    ASSERT_EQ((uint64_t)10, (*(accessor.mOffsets))[0]);

    //update: docid too large
    ASSERT_NO_THROW(
            accessor.UpdateEncodedField((docid_t)10000, attrMeta, encodeMap));
}

void VarNumAttributeAccessorTest::TestAppendData()
{
    //normal case
    VarNumAttributeAccessor accessor;
    accessor.Init(&mPool);
    ConstString str("1", 1);
    common::AttrValueMeta attrMeta(1, str);
    uint64_t offset = accessor.AppendData(attrMeta.data);
    ASSERT_EQ((uint64_t)0, offset);
    uint8_t* buffer = accessor.mData->Search(offset);
    ASSERT_EQ((uint32_t)1, *(uint32_t*)buffer);
    ASSERT_EQ('1', *((char*)buffer + 4));

    attrMeta.data.reset("12", 2);
    offset = accessor.AppendData(attrMeta.data);
    ASSERT_EQ((uint64_t)5, offset);
    buffer = accessor.mData->Search(offset);
    ASSERT_EQ((uint32_t)2, *(uint32_t*)buffer);
    ASSERT_EQ('1', *((char*)buffer + 4));
    ASSERT_EQ('2', *((char*)buffer + 5));

    //test append large data
    buffer = (uint8_t*) mPool.allocate(1024*1024*1024);
    attrMeta.data.reset((char*)buffer, 1024*1024*1024);
    offset = accessor.AppendData(attrMeta.data);
    ASSERT_EQ((uint64_t)1024*1024, offset);
    buffer = accessor.mData->Search(offset);
    ASSERT_EQ((uint32_t)1024*1024*1024, *(uint32_t*)buffer);
}

IE_NAMESPACE_END(index);
