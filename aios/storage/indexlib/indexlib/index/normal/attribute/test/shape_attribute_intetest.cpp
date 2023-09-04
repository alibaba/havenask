#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_reader.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/test/build_test_util.h"

using namespace std;
using namespace autil;
using namespace indexlib::test;
using namespace indexlib::config;

namespace indexlib { namespace index {

class ShapeAttributeInteTest : public INDEXLIB_TESTBASE_WITH_PARAM<int>
{
public:
    ShapeAttributeInteTest() {}
    ~ShapeAttributeInteTest() {}
    typedef VarNumAttributeReader<double> PolygonAttributeReader;
    typedef VarNumAttributeReader<double> LocationAttributeReader;
    typedef VarNumAttributeReader<double> LineAttributeReader;

    DECLARE_CLASS_NAME(ShapeAttributeInteTest);

public:
    void CaseSetUp() override
    {
        // BuildMode
        int buildMode = GetParam();
        _options = util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(buildMode);
    }
    void CaseTearDown() override {}

private:
    config::IndexPartitionOptions _options;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, ShapeAttributeInteTest);

TEST_P(ShapeAttributeInteTest, TestInputIllegalPolygon)
{
    SingleFieldPartitionDataProvider provider;
    provider.EnableSpatialField();
    provider.Init(GET_TEMP_DATA_PATH(), "polygon", SFP_ATTRIBUTE);
    string doc = "0 0,0 10,10 10,10 0,0 01 0,1 10,10 10,10 1|"
                 "1 0,1 10,10 10,10 1,1|a1 0,1 10,10 10,10 1,1 0";
    provider.Build(doc, SFP_OFFLINE);
    AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    PolygonAttributeReader reader;
    reader.Open(attrConfig, provider.GetPartitionData(), PatchApplyStrategy::PAS_APPLY_NO_PATCH);
    string resultStr;
    reader.Read(0, resultStr);
    ASSERT_EQ("", resultStr);
    reader.Read(1, resultStr);
    ASSERT_EQ("", resultStr);
    reader.Read(2, resultStr);
    ASSERT_EQ("", resultStr);
}

TEST_P(ShapeAttributeInteTest, TestInputIllegalLine)
{
    SingleFieldPartitionDataProvider provider;
    provider.EnableSpatialField();
    provider.Init(GET_TEMP_DATA_PATH(), "line", SFP_ATTRIBUTE);
    string doc = "1 0,1 10,10 10,10 1,1|a1 0,1 10,10 10,10 1,1 0";
    provider.Build(doc, SFP_OFFLINE);
    AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    LineAttributeReader reader;
    reader.Open(attrConfig, provider.GetPartitionData(), PatchApplyStrategy::PAS_APPLY_NO_PATCH);
    string resultStr;
    reader.Read(0, resultStr);
    ASSERT_EQ("", resultStr);
    reader.Read(1, resultStr);
    ASSERT_EQ("", resultStr);
}

TEST_P(ShapeAttributeInteTest, TestPolygonAttr)
{
    SingleFieldPartitionDataProvider provider;
    provider.EnableSpatialField();
    provider.Init(GET_TEMP_DATA_PATH(), "polygon", SFP_ATTRIBUTE);
    string doc = "0 0,0 10,10 10,10 0,0 01 0,1 10,10 10,10 1,1 0";
    provider.Build(doc, SFP_OFFLINE);
    AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    PolygonAttributeReader reader;
    reader.Open(attrConfig, provider.GetPartitionData(), PatchApplyStrategy::PAS_APPLY_NO_PATCH);
    string resultStr;
    reader.Read(0, resultStr);
    ASSERT_EQ(doc, resultStr);
    VarNumAttributeReader<double>* doubleReader = (VarNumAttributeReader<double>*)&reader;
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

TEST_P(ShapeAttributeInteTest, TestLineAttr)
{
    SingleFieldPartitionDataProvider provider;
    provider.EnableSpatialField();
    provider.Init(GET_TEMP_DATA_PATH(), "line", SFP_ATTRIBUTE);
    string doc = "0 0,0 10,10 10,10 0,0 01 0,1 10,10 10,10 1,1 0";
    provider.Build(doc, SFP_OFFLINE);
    AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    LineAttributeReader reader;
    reader.Open(attrConfig, provider.GetPartitionData(), PatchApplyStrategy::PAS_APPLY_NO_PATCH);
    string resultStr;
    reader.Read(0, resultStr);
    ASSERT_EQ(doc, resultStr);
    VarNumAttributeReader<double>* doubleReader = (VarNumAttributeReader<double>*)&reader;
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

// PartitionWriter::BuildMode
INSTANTIATE_TEST_CASE_P(BuildMode, ShapeAttributeInteTest, testing::Values(0, 1, 2));

}} // namespace indexlib::index
