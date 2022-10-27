#include "indexlib/index/normal/attribute/test/building_attribute_reader_unittest.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, BuildingAttributeReaderTest);

BuildingAttributeReaderTest::BuildingAttributeReaderTest()
{
}

BuildingAttributeReaderTest::~BuildingAttributeReaderTest()
{
}

void BuildingAttributeReaderTest::CaseSetUp()
{
    string field = "pk:uint64:pk;string1:string;text1:text;multi_uint:uint32:true;multi_str:string:true;";
    string index = "pk:primarykey64:pk;index1:string:string1;pack1:pack:text1;";
    mSchema = SchemaMaker::MakeSchema(field, index, "pk;string1;multi_uint;multi_str", "");

    IndexPartitionOptions options;

    mPdc.reset(new FakePartitionDataCreator);
    mPdc->Init(mSchema, options, GET_TEST_DATA_PATH(), false);

    string fullDocs = "cmd=add,pk=1,string1=A,multi_uint=1 2,multi_str=123 234;"
                      "cmd=add,pk=2,string1=B,multi_uint=2 3,multi_str=234 345;";
    mPdc->AddFullDocs(fullDocs);

    string builtRtDocs = "cmd=add,pk=3,string1=B,multi_uint=3 4,multi_str=345 456,ts=0;"
                         "cmd=add,pk=4,string1=A,multi_uint=4 5,multi_str=456 567,ts=0;";
    mPdc->AddBuiltSegment(builtRtDocs);

    string inMemSegment1 = "cmd=add,pk=5,string1=B,multi_uint=5 6,multi_str=567 678,ts=0;"
                           "cmd=add,pk=6,string1=A,multi_uint=6 7,multi_str=678 789,ts=0;";
    mPdc->AddInMemSegment(inMemSegment1);

    string inMemSegment2 = "cmd=add,pk=7,string1=B,multi_uint=7 8,multi_str=789 8910,ts=0;"
                           "cmd=add,pk=8,string1=A,multi_uint=8 9,multi_str=8910 91011,ts=0;"
                           "cmd=add,pk=9,string1=C,multi_uint=9 10,multi_str=91011 101112,ts=0;";
    mPdc->AddInMemSegment(inMemSegment2);

    string buildingDocs = "cmd=add,pk=10,string1=A,multi_uint=10 11,multi_str=101112 111213,ts=0;"
                          "cmd=add,pk=11,string1=C,multi_uint=11 12,multi_str=111213 121314,ts=0;";
    mPdc->AddBuildingData(buildingDocs);
    mPartitionData = mPdc->CreatePartitionData(false);
    usleep(1000000);
}

void BuildingAttributeReaderTest::CaseTearDown()
{
    mPartitionData.reset();
    mPdc.reset();
}

void BuildingAttributeReaderTest::TestSingleAttributeReader()
{
    AttributeConfigPtr attrConfig = mSchema->GetAttributeSchema()->GetAttributeConfig("pk");
    assert(attrConfig);

    SingleValueAttributeReader<uint64_t> reader;
    ASSERT_TRUE(reader.Open(attrConfig, mPartitionData));
    CheckSingleReader(reader, "1,2,3,4,5,6,7,8,9,10,11");
}

void BuildingAttributeReaderTest::TestVarNumAttributeReader()
{
    AttributeConfigPtr attrConfig =
        mSchema->GetAttributeSchema()->GetAttributeConfig("multi_uint");
    assert(attrConfig);

    VarNumAttributeReader<uint32_t> reader;
    ASSERT_TRUE(reader.Open(attrConfig, mPartitionData));
    CheckVarNumReader(reader, "1 2,2 3,3 4,4 5,5 6,6 7,"
                      "7 8,8 9,9 10,10 11,11 12");
}

void BuildingAttributeReaderTest::TestSingleStringAttributeReader()
{
    AttributeConfigPtr attrConfig =
        mSchema->GetAttributeSchema()->GetAttributeConfig("string1");
    assert(attrConfig);

    StringAttributeReader reader;
    ASSERT_TRUE(reader.Open(attrConfig, mPartitionData));
    CheckStringReader(reader, "A,B,B,A,B,A,B,A,C,A,C");
}

void BuildingAttributeReaderTest::TestMultiStringAttributeReader()
{
    AttributeConfigPtr attrConfig =
        mSchema->GetAttributeSchema()->GetAttributeConfig("multi_str");
    assert(attrConfig);

    MultiStringAttributeReader reader;
    ASSERT_TRUE(reader.Open(attrConfig, mPartitionData));
    CheckMultiStringReader(reader, "123 234,234 345,345 456,456 567,"
                           "567 678,678 789,789 8910,8910 91011,91011 101112,"
                           "101112 111213,111213 121314");
}

void BuildingAttributeReaderTest::CheckSingleReader(
        SingleValueAttributeReader<uint64_t>& reader, const string& resultStr)
{
    vector<uint64_t> resultInfo;
    StringUtil::fromString(resultStr, resultInfo, ",");
    Pool pool;

    AttributeIteratorTyped<uint64_t>* attrIter =
        dynamic_cast<AttributeIteratorTyped<uint64_t>*>(reader.CreateIterator(&pool));
    ASSERT_TRUE(attrIter);

    for (size_t i = 0; i < resultInfo.size(); i++)
    {
        uint64_t value;
        ASSERT_TRUE(reader.Read(i, value));
        ASSERT_EQ(resultInfo[i], value);

        ASSERT_TRUE(attrIter->Seek(i, value));
        ASSERT_EQ(resultInfo[i], value);
    }

    POOL_DELETE_CLASS(attrIter);
}

void BuildingAttributeReaderTest::CheckVarNumReader(
        VarNumAttributeReader<uint32_t>& reader, const string& resultStr)
{
    vector<vector<uint32_t>> resultInfo;
    StringUtil::fromString(resultStr, resultInfo, " ", ",");
    Pool pool;

    AttributeIteratorTyped<MultiValueType<uint32_t>>* attrIter =
        dynamic_cast<AttributeIteratorTyped<MultiValueType<uint32_t>>*>(reader.CreateIterator(&pool));
    ASSERT_TRUE(attrIter);

    for (size_t i = 0; i < resultInfo.size(); i++)
    {
        MultiValueType<uint32_t> value;
        ASSERT_TRUE(reader.Read(i, value));
        ASSERT_EQ((uint32_t)resultInfo[i].size(), value.size());
        for (size_t j = 0; j < resultInfo[i].size(); j++)
        {
            ASSERT_EQ(resultInfo[i][j], value[j]);
        }

        ASSERT_TRUE(attrIter->Seek(i, value));
        ASSERT_EQ((uint32_t)resultInfo[i].size(), value.size());
        for (size_t j = 0; j < resultInfo[i].size(); j++)
        {
            ASSERT_EQ(resultInfo[i][j], value[j]);
        }
    }

    for (size_t i = 0; i < resultInfo.size(); i++)
    {
        CountedMultiValueType<uint32_t> value;
        ASSERT_TRUE(attrIter->Seek(i, value));
        ASSERT_EQ((uint32_t)resultInfo[i].size(), value.size());
        for (size_t j = 0; j < resultInfo[i].size(); j++)
        {
            ASSERT_EQ(resultInfo[i][j], value[j]);
        }
    }
    POOL_DELETE_CLASS(attrIter);
}

void BuildingAttributeReaderTest::CheckStringReader(
        StringAttributeReader& reader, const string& resultStr)
{
    vector<string> resultInfo;
    StringUtil::fromString(resultStr, resultInfo, ",");
    Pool pool;

    AttributeIteratorTyped<MultiChar>* attrIter =
        dynamic_cast<AttributeIteratorTyped<MultiChar>*>(reader.CreateIterator(&pool));
    ASSERT_TRUE(attrIter);

    for (size_t i = 0; i < resultInfo.size(); i++)
    {
        MultiChar value;
        ASSERT_TRUE(reader.Read(i, value));
        ASSERT_EQ(resultInfo[i], string(value.data(), value.size()));

        ASSERT_TRUE(attrIter->Seek(i, value));
        ASSERT_EQ(resultInfo[i], string(value.data(), value.size()));
    }
    POOL_DELETE_CLASS(attrIter);
}

void BuildingAttributeReaderTest::CheckMultiStringReader(
        MultiStringAttributeReader& reader, const string& resultStr)
{
    vector<vector<string>> resultInfo;
    StringUtil::fromString(resultStr, resultInfo, " ", ",");
    Pool pool;

    AttributeIteratorTyped<MultiValueType<MultiChar>>* attrIter =
        dynamic_cast<AttributeIteratorTyped<MultiValueType<MultiChar>>*>(reader.CreateIterator(&pool));
    ASSERT_TRUE(attrIter);

    for (size_t i = 0; i < resultInfo.size(); i++)
    {
        MultiValueType<MultiChar> value;
        ASSERT_TRUE(reader.Read(i, value));
        ASSERT_EQ((uint32_t)resultInfo[i].size(), value.size());
        for (size_t j = 0; j < resultInfo[i].size(); j++)
        {
            ASSERT_EQ(resultInfo[i][j], string(value[j].data(), value[j].size()));
        }

        ASSERT_TRUE(attrIter->Seek(i, value));
        ASSERT_EQ((uint32_t)resultInfo[i].size(), value.size());
        for (size_t j = 0; j < resultInfo[i].size(); j++)
        {
            ASSERT_EQ(resultInfo[i][j], string(value[j].data(), value[j].size()));
        }
    }
    POOL_DELETE_CLASS(attrIter);
}

IE_NAMESPACE_END(index);
