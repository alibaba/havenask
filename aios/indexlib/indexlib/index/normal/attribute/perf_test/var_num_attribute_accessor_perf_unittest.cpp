#include "indexlib/index/normal/attribute/perf_test/var_num_attribute_accessor_perf_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, VarNumAttributeAccessorPerfTest);

VarNumAttributeAccessorPerfTest::VarNumAttributeAccessorPerfTest()
{
}

VarNumAttributeAccessorPerfTest::~VarNumAttributeAccessorPerfTest()
{
}

void VarNumAttributeAccessorPerfTest::CaseSetUp()
{
}

void VarNumAttributeAccessorPerfTest::CaseTearDown()
{
}

void VarNumAttributeAccessorPerfTest::TestAddNormalField()
{
    VarNumAttributeAccessor accessor;
    accessor.Init(&mPool);
    
    for (size_t i = 0; i < 100000000; i++)
    {
        common::AttrValueMeta attrMeta(i, "12345");
        accessor.AddNormalField(attrMeta);
    }
}

void VarNumAttributeAccessorPerfTest::TestAddEncodedField()
{
    VarNumAttributeAccessor accessor;
    accessor.Init(&mPool);
    VarNumAttributeAccessor::EncodeMapPtr encodeMap(
            new VarNumAttributeAccessor::EncodeMap(HASHMAP_INIT_SIZE));
    
    for (size_t i = 0; i < 100000000; i++)
    {
        common::AttrValueMeta attrMeta(i%10000, "12345");
        accessor.AddEncodedField(attrMeta, encodeMap);
    }
}

IE_NAMESPACE_END(index);

