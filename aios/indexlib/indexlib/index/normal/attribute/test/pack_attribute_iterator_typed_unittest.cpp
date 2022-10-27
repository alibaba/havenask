#include <autil/StringUtil.h>
#include "indexlib/index/normal/attribute/test/pack_attribute_iterator_typed_unittest.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/test/partition_state_machine.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(test);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(attribute, PackAttributeIteratorTypedTest);

PackAttributeIteratorTypedTest::PackAttributeIteratorTypedTest()
{
}

PackAttributeIteratorTypedTest::~PackAttributeIteratorTypedTest()
{
}

void PackAttributeIteratorTypedTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    mSchema = index_base::SchemaAdapter::LoadSchema(
        string(TEST_DATA_PATH), "schema_with_all_type_pack_attributes.json");

    mDoc =
        "cmd=add,"
        "int8_single=0,int8_multi=0 1,"
        "int16_single=1,int16_multi=1 2,"
        "int32_single=2,int32_multi=2 3,"
        "int64_single=3,int64_multi=3 4,"
        "uint8_single=5,uint8_multi=5 6,"
        "uint16_single=6,uint16_multi=6 7,"
        "uint32_single=7,uint32_multi=7 8,"
        "uint64_single=8,uint64_multi=8 9,"
        "float_single=9,float_multi=9 10,"
        "double_single=10,double_multi=10 11,"
        "str_single=abc,str_multi=abc def,"
        "int8_single_nopack=23,str_multi_nopack=i don't pack";
}

void PackAttributeIteratorTypedTest::CaseTearDown()
{
}

void PackAttributeIteratorTypedTest::TestSimpleProcess()
{
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, mDoc, "", ""));

    PackAttributeReaderPtr packAttrReader = 
        psm.GetIndexPartition()->GetReader()->GetPackAttributeReader("pack_attr");
    assert(packAttrReader);
    PackAttributeReaderPtr uniqPackAttrReader = 
        psm.GetIndexPartition()->GetReader()->GetPackAttributeReader("uniq_pack_attr");
    assert(packAttrReader);
    CheckSingleIterator<int8_t>(packAttrReader, "int8_single", 0);
    CheckSingleIterator<int16_t>(packAttrReader, "int16_single", 1);
    CheckSingleIterator<int32_t>(packAttrReader, "int32_single", 2);
    CheckSingleIterator<int64_t>(packAttrReader, "int64_single", 3);
    CheckSingleIterator<uint8_t>(uniqPackAttrReader, "uint8_single", 5);
    CheckSingleIterator<uint16_t>(uniqPackAttrReader, "uint16_single", 6);
    CheckSingleIterator<uint32_t>(uniqPackAttrReader, "uint32_single", 7);
    CheckSingleIterator<uint64_t>(uniqPackAttrReader, "uint64_single", 8);
    CheckSingleIterator<float>(packAttrReader, "float_single", 9);
    CheckSingleIterator<double>(uniqPackAttrReader, "double_single", 10);

    CheckMultiIterator<int8_t>(packAttrReader, "int8_multi", "0;1");
    CheckMultiIterator<int16_t>(packAttrReader, "int16_multi", "1;2");
    CheckMultiIterator<int32_t>(packAttrReader, "int32_multi", "2;3");
    CheckMultiIterator<int64_t>(packAttrReader, "int64_multi", "3;4");
    CheckMultiIterator<uint8_t>(uniqPackAttrReader, "uint8_multi", "5;6");
    CheckMultiIterator<uint16_t>(uniqPackAttrReader, "uint16_multi", "6;7");
    CheckMultiIterator<uint32_t>(uniqPackAttrReader, "uint32_multi", "7;8");
    CheckMultiIterator<uint64_t>(uniqPackAttrReader, "uint64_multi", "8;9");
    CheckMultiIterator<float>(packAttrReader, "float_multi", "9;10");
    CheckMultiIterator<double>(uniqPackAttrReader, "double_multi", "10;11");

    {
        Pool pool;
        AttributeIteratorBase* iter = packAttrReader->CreateIterator(
                &pool, "str_single");
        assert(iter);
        PackAttributeIteratorTyped<MultiChar>* iterTyped = 
            dynamic_cast<PackAttributeIteratorTyped<MultiChar>* >(iter);
        assert(iterTyped);
        MultiChar value;
        ASSERT_TRUE(iterTyped->Seek(0, value));
        ASSERT_EQ(string("abc"), string(value.data(), value.size()));
        POOL_DELETE_CLASS(iter);
    }
    {
        Pool pool;
        AttributeIteratorBase* iter = uniqPackAttrReader->CreateIterator(
                &pool, "str_multi");
        assert(iter);
        PackAttributeIteratorTyped<MultiString>* iterTyped = 
            dynamic_cast<PackAttributeIteratorTyped<MultiString>* >(iter);
        assert(iterTyped);
        MultiString value;
        ASSERT_TRUE(iterTyped->Seek(0, value));
        ASSERT_EQ(string("abc"), string(value[0].data(), value[0].size()));
        ASSERT_EQ(string("def"), string(value[1].data(), value[1].size()));
        POOL_DELETE_CLASS(iter);
    }
}

template<typename T>
void PackAttributeIteratorTypedTest::CheckSingleIterator(
        const PackAttributeReaderPtr& packAttrReader,
        const string& attrName, T expectValue)
{
    Pool pool;
    AttributeIteratorBase* iter = packAttrReader->CreateIterator(
            &pool, attrName);
    assert(iter);
    PackAttributeIteratorTyped<T>* iterTyped = 
        dynamic_cast<PackAttributeIteratorTyped<T>* >(iter);
    assert(iterTyped);
    T value;
    ASSERT_TRUE(iterTyped->Seek(0, value));
    ASSERT_TRUE(iterTyped->Seek(0, value));
    ASSERT_EQ(expectValue, value);
    POOL_DELETE_CLASS(iter);
}

template<typename T>
void PackAttributeIteratorTypedTest::CheckMultiIterator(
        const PackAttributeReaderPtr& packAttrReader,
        const string& attrName, const string& expectValueStr)
{
    vector<T> expectValue;
    StringUtil::fromString(expectValueStr, expectValue, ";");
    Pool pool;
    AttributeIteratorBase* iter = packAttrReader->CreateIterator(
            &pool, attrName);
    assert(iter);
    PackAttributeIteratorTyped<MultiValueType<T> >* iterTyped = 
        dynamic_cast<PackAttributeIteratorTyped<MultiValueType<T> > * >(iter);
    assert(iterTyped);
    MultiValueType<T> value;
    ASSERT_TRUE(iterTyped->Seek(0, value));
    ASSERT_TRUE(iterTyped->Seek(0, value));
    ASSERT_EQ(expectValue.size(), value.size());
    for (size_t i = 0; i < expectValue.size(); ++i)
    {
        ASSERT_EQ(expectValue[i], value[i]);
    }
    POOL_DELETE_CLASS(iter);
}

IE_NAMESPACE_END(index);

