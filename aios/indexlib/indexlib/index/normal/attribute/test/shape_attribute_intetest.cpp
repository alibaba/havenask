#include "indexlib/index/normal/attribute/test/shape_attribute_intetest.h"
#include "indexlib/index/normal/attribute/accessor/polygon_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/line_attribute_reader.h"
#include "indexlib/test/single_field_partition_data_provider.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, ShapeAttributeInteTest);

ShapeAttributeInteTest::ShapeAttributeInteTest()
{
}

ShapeAttributeInteTest::~ShapeAttributeInteTest()
{
}

void ShapeAttributeInteTest::CaseSetUp()
{
}

void ShapeAttributeInteTest::CaseTearDown()
{
}

void ShapeAttributeInteTest::TestInputIllegalPolygon()
{
    SingleFieldPartitionDataProvider provider("|");
    provider.Init(GET_TEST_DATA_PATH(), "polygon", SFP_ATTRIBUTE);
    string doc = "0 0,0 10,10 10,10 0,0 01 0,1 10,10 10,10 1|"
                 "1 0,1 10,10 10,10 1,1|a1 0,1 10,10 10,10 1,1 0";
    provider.Build(doc, SFP_OFFLINE);
    AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    PolygonAttributeReader reader;
    reader.Open(attrConfig, provider.GetPartitionData());
    string resultStr;
    reader.Read(0, resultStr);
    ASSERT_EQ("", resultStr);
    reader.Read(1, resultStr);
    ASSERT_EQ("", resultStr);
    reader.Read(2, resultStr);
    ASSERT_EQ("", resultStr);
}

void ShapeAttributeInteTest::TestInputIllegalLine()
{
    SingleFieldPartitionDataProvider provider("|");
    provider.Init(GET_TEST_DATA_PATH(), "line", SFP_ATTRIBUTE);
    string doc = "1 0,1 10,10 10,10 1,1|a1 0,1 10,10 10,10 1,1 0";
    provider.Build(doc, SFP_OFFLINE);
    AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    LineAttributeReader reader;
    reader.Open(attrConfig, provider.GetPartitionData());
    string resultStr;
    reader.Read(0, resultStr);
    ASSERT_EQ("", resultStr);
    reader.Read(1, resultStr);
    ASSERT_EQ("", resultStr);
}

void ShapeAttributeInteTest::TestPolygonAttr()
{
    SingleFieldPartitionDataProvider provider("|");
    provider.Init(GET_TEST_DATA_PATH(), "polygon", SFP_ATTRIBUTE);
    string doc = "0 0,0 10,10 10,10 0,0 01 0,1 10,10 10,10 1,1 0";
    provider.Build(doc, SFP_OFFLINE);
    AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    PolygonAttributeReader reader;
    reader.Open(attrConfig, provider.GetPartitionData());
    string resultStr;
    reader.Read(0, resultStr);
    ASSERT_EQ(doc, resultStr);
    VarNumAttributeReader<double>* doubleReader =
        (VarNumAttributeReader<double>*)&reader;
    MultiValueType<double> value;
    doubleReader->Read((docid_t)0, value);
    ASSERT_EQ(22u, value.size());
    ASSERT_EQ(5.0f, value[0]);
    ASSERT_EQ(0.0f, value[1]);
    ASSERT_EQ(0.0f, value[2]);
    ASSERT_EQ(0.0f, value[3]);
    ASSERT_EQ(10.0f, value[4]);
    ASSERT_EQ(10.0f, value[5]);
    ASSERT_EQ(10.0f, value[6]);
    ASSERT_EQ(10.0f, value[7]);
    ASSERT_EQ(0.0f, value[8]);
    ASSERT_EQ(0.0f, value[9]);
    ASSERT_EQ(0.0f, value[10]);
    ASSERT_EQ(5.0f, value[11]);
    ASSERT_EQ(1.0f, value[12]);
    ASSERT_EQ(0.0f, value[13]);
    ASSERT_EQ(1.0f, value[14]);
    ASSERT_EQ(10.0f, value[15]);
    ASSERT_EQ(10.0f, value[16]);
    ASSERT_EQ(10.0f, value[17]);
    ASSERT_EQ(10.0f, value[18]);
    ASSERT_EQ(1.0f, value[19]);
    ASSERT_EQ(1.0f, value[20]);
    ASSERT_EQ(0.0f, value[21]);
}

void ShapeAttributeInteTest::TestLineAttr()
{
    SingleFieldPartitionDataProvider provider("|");
    provider.Init(GET_TEST_DATA_PATH(), "line", SFP_ATTRIBUTE);
    string doc = "0 0,0 10,10 10,10 0,0 01 0,1 10,10 10,10 1,1 0";
    provider.Build(doc, SFP_OFFLINE);
    AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    LineAttributeReader reader;
    reader.Open(attrConfig, provider.GetPartitionData());
    string resultStr;
    reader.Read(0, resultStr);
    ASSERT_EQ(doc, resultStr);
    VarNumAttributeReader<double>* doubleReader =
        (VarNumAttributeReader<double>*)&reader;
    MultiValueType<double> value;
    doubleReader->Read((docid_t)0, value);
    ASSERT_EQ(22u, value.size());
    ASSERT_EQ(5.0f, value[0]);
    ASSERT_EQ(0.0f, value[1]);
    ASSERT_EQ(0.0f, value[2]);
    ASSERT_EQ(0.0f, value[3]);
    ASSERT_EQ(10.0f, value[4]);
    ASSERT_EQ(10.0f, value[5]);
    ASSERT_EQ(10.0f, value[6]);
    ASSERT_EQ(10.0f, value[7]);
    ASSERT_EQ(0.0f, value[8]);
    ASSERT_EQ(0.0f, value[9]);
    ASSERT_EQ(0.0f, value[10]);
    ASSERT_EQ(5.0f, value[11]);
    ASSERT_EQ(1.0f, value[12]);
    ASSERT_EQ(0.0f, value[13]);
    ASSERT_EQ(1.0f, value[14]);
    ASSERT_EQ(10.0f, value[15]);
    ASSERT_EQ(10.0f, value[16]);
    ASSERT_EQ(10.0f, value[17]);
    ASSERT_EQ(10.0f, value[18]);
    ASSERT_EQ(1.0f, value[19]);
    ASSERT_EQ(1.0f, value[20]);
    ASSERT_EQ(0.0f, value[21]);
}

IE_NAMESPACE_END(index);

