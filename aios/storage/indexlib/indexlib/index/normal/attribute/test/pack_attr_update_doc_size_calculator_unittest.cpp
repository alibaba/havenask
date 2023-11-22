#include "indexlib/index/normal/attribute/test/pack_attr_update_doc_size_calculator_unittest.h"

#include "indexlib/config/test/schema_maker.h"
#include "indexlib/index/normal/attribute/test/attribute_test_util.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/util/test/build_test_util.h"

using namespace std;
using namespace indexlib::test;
using namespace indexlib::index_base;
using namespace indexlib::partition;
using namespace indexlib::config;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, PackAttrUpdateDocSizeCalculatorTest);

PackAttrUpdateDocSizeCalculatorTest::PackAttrUpdateDocSizeCalculatorTest() {}

PackAttrUpdateDocSizeCalculatorTest::~PackAttrUpdateDocSizeCalculatorTest() {}

void PackAttrUpdateDocSizeCalculatorTest::CaseSetUp()
{
    string field = "pk:string;id:uint32;price:uint32";
    string index = "pk:primarykey64:pk";
    string attribute = "packAttr:id,price";
    // size of one pack is 4 + 4 + 1(count) = 9
    mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");

    string sub_field = "sub_pk:string;sub_id:uint64;sub_price:uint32";
    string sub_index = "sub_pk:primarykey64:sub_pk";
    string sub_attribute = "sub_packAttr:sub_id,sub_price:uniq";
    // size of one pack is 8 + 4 + 1(count) = 13

    mSchema->SetSubIndexPartitionSchema(SchemaMaker::MakeSchema(sub_field, sub_index, sub_attribute, ""));
}

void PackAttrUpdateDocSizeCalculatorTest::CaseTearDown() {}

void PackAttrUpdateDocSizeCalculatorTest::TestEstimateUpdateDocSize()
{
    IndexPartitionOptions options;
    util::BuildTestUtil::SetupIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam(), &options);
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, options, GET_TEMP_DATA_PATH()));

    // full : pk1(spk11,spk12) + pk2(spk21,spk22)
    string fullDoc = "cmd=add,pk=pk1,id=1,price=1,sub_pk=spk11^spk12,sub_id=1^1,sub_price=11^12,ts=1;"
                     "cmd=add,pk=pk2,id=2,price=2,sub_pk=spk21^spk22,sub_id=1^2,sub_price=11^22,ts=1;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDoc, "", ""));

    // inc1 : pk3(spk31) + update pk1(id)/ spk11(sub_id)
    string inc1Doc = "cmd=add,pk=pk3,id=3,price=3,sub_pk=spk31,sub_id=3,sub_price=31,ts=2;"
                     "cmd=update_field,pk=pk1,id=11,sub_pk=spk11,sub_id=3,ts=2;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, inc1Doc, "", ""));
    size_t expectUpdateDocSize = 9 + 13;

    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    OnDiskPartitionDataPtr partData = OnDiskPartitionData::CreateOnDiskPartitionData(GET_FILE_SYSTEM(), mSchema);
    PackAttrUpdateDocSizeCalculator calculator(partData, mSchema);
    ASSERT_EQ(expectUpdateDocSize, calculator.EstimateUpdateDocSize(index_base::Version(INVALID_VERSIONID)));

    // inc2 : update pk3, pk2/spk22
    string inc2Doc = "cmd=update_field,pk=pk2,id=22,sub_pk=spk22,sub_price=32,ts=3;"
                     "cmd=update_field,pk=pk3,id=31,ts=3;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, inc2Doc, "", ""));

    Version lastLoadVersion = partData->GetVersion();
    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    partData = OnDiskPartitionData::CreateOnDiskPartitionData(GET_FILE_SYSTEM(), mSchema);
    PackAttrUpdateDocSizeCalculator inc2Calculator(partData, mSchema);

    expectUpdateDocSize = 9 * 2 + 13;
    ASSERT_EQ(expectUpdateDocSize, inc2Calculator.EstimateUpdateDocSize(lastLoadVersion));

    expectUpdateDocSize = 9 * 3 + 13 * 2;
    ASSERT_EQ(expectUpdateDocSize, inc2Calculator.EstimateUpdateDocSize(index_base::Version(INVALID_VERSIONID)));
}

void PackAttrUpdateDocSizeCalculatorTest::TestEstimateUpdateDocSizeInUnobseleteRtSeg()
{
    IndexPartitionOptions options;
    options.GetOnlineConfig().buildConfig.maxDocCount = 1;
    util::BuildTestUtil::SetupIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam(), &options);
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, options, GET_TEMP_DATA_PATH()));

    // full : pk1(spk11,spk12) + pk2(spk21,spk22)
    string fullDoc = "cmd=add,pk=pk1,id=1,price=1,sub_pk=spk11^spk12,sub_id=1^1,sub_price=11^12,ts=1;"
                     "cmd=add,pk=pk2,id=2,price=2,sub_pk=spk21^spk22,sub_id=1^2,sub_price=11^22,ts=1;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDoc, "", ""));

    // rt : pk3(spk31)
    string rtDoc1 = "cmd=add,pk=pk3,id=3,price=3,sub_pk=spk31,sub_id=3,sub_price=31,ts=2;";
    string rtDoc2 = "cmd=update_field,pk=pk1,id=11,sub_pk=spk11,sub_id=3,ts=3;";
    string rtDoc3 = "cmd=update_field,pk=pk2,id=22,sub_pk=spk22,sub_price=32,ts=4;";
    string rtDoc4 = "cmd=update_field,pk=pk3,id=31,ts=5;";

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDoc1, "", ""));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDoc2, "", ""));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDoc3, "", ""));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDoc4, "", ""));

    OnlinePartitionPtr onlinePartition = DYNAMIC_POINTER_CAST(OnlinePartition, psm.GetIndexPartition());
    assert(onlinePartition);
    PackAttrUpdateDocSizeCalculator calculator(onlinePartition->GetPartitionData(), mSchema);

    size_t expectUpdateDocSize = 9 * 3 + 13 * 2;
    ASSERT_EQ(expectUpdateDocSize, calculator.EstimateUpdateDocSizeInUnobseleteRtSeg(INVALID_TIMESTAMP));
    ASSERT_EQ(expectUpdateDocSize, calculator.EstimateUpdateDocSizeInUnobseleteRtSeg(3));

    expectUpdateDocSize = 9 * 2 + 13;
    ASSERT_EQ(expectUpdateDocSize, calculator.EstimateUpdateDocSizeInUnobseleteRtSeg(4));

    expectUpdateDocSize = 9;
    ASSERT_EQ(expectUpdateDocSize, calculator.EstimateUpdateDocSizeInUnobseleteRtSeg(5));

    expectUpdateDocSize = 0;
    ASSERT_EQ(expectUpdateDocSize, calculator.EstimateUpdateDocSizeInUnobseleteRtSeg(6));
}

void PackAttrUpdateDocSizeCalculatorTest::TestCheckSchemaHasUpdatablePackAttribute()
{
    // check schema with no pack attribute
    PartitionDataPtr partitionData;
    IndexPartitionSchemaPtr indexPartitionSchema;
    PackAttrUpdateDocSizeCalculator packAttrUpdateDocSizeCalc(partitionData, indexPartitionSchema);

    IndexPartitionSchemaPtr schema = MakeSchemaWithPackAttribute(false, false, false);
    ASSERT_FALSE(packAttrUpdateDocSizeCalc.HasUpdatablePackAttribute(schema));

    // check shema with un-updatable pack attribute
    schema = MakeSchemaWithPackAttribute(true, false, false);
    ASSERT_FALSE(packAttrUpdateDocSizeCalc.HasUpdatablePackAttribute(schema));

    // check schema with updatable pack attribute
    schema = MakeSchemaWithPackAttribute(true, true, false);
    ASSERT_TRUE(packAttrUpdateDocSizeCalc.HasUpdatablePackAttribute(schema));

    // check sub-schema with no pack attribute
    schema = MakeSchemaWithPackAttribute(false, false, false);
    IndexPartitionSchemaPtr subSchema = MakeSchemaWithPackAttribute(false, false, true);
    schema->SetSubIndexPartitionSchema(subSchema);
    ASSERT_FALSE(packAttrUpdateDocSizeCalc.HasUpdatablePackAttribute(schema));

    // check sub-schema with un-updatable pack attribute
    schema = MakeSchemaWithPackAttribute(false, false, false);
    subSchema = MakeSchemaWithPackAttribute(true, false, true);
    schema->SetSubIndexPartitionSchema(subSchema);
    ASSERT_FALSE(packAttrUpdateDocSizeCalc.HasUpdatablePackAttribute(schema));

    // check sub-schema with updatable pack attribute
    schema = MakeSchemaWithPackAttribute(false, false, false);
    subSchema = MakeSchemaWithPackAttribute(true, true, true);
    schema->SetSubIndexPartitionSchema(subSchema);
    ASSERT_TRUE(packAttrUpdateDocSizeCalc.HasUpdatablePackAttribute(schema));
}

IndexPartitionSchemaPtr PackAttrUpdateDocSizeCalculatorTest::MakeSchemaWithPackAttribute(bool hasPackAttr,
                                                                                         bool isPackUpdatable,
                                                                                         bool isSubSchema)
{
    string field, index, attribute;
    if (!isSubSchema) {
        field = "pk:string;id:uint32;price:uint32";
        index = "pk:primarykey64:pk";
        attribute = "id;price";
    } else {
        field = "sub_pk:string;sub_id:uint64;sub_price:uint32";
        index = "sub_pk:primarykey64:sub_pk";
        attribute = "sub_id;sub_price";
    }
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");

    if (hasPackAttr) {
        string packField1 = isSubSchema ? "sub_fid1" : "fid1";
        string packField2 = isSubSchema ? "sub_fid2" : "fid2";
        string packAttrName = isSubSchema ? "sub_pack_attr" : "pack_attr";

        AttributeConfigPtr attr1 = AttributeTestUtil::CreateAttrConfig(ft_string, true, packField1, 3, false);
        AttributeConfigPtr attr2 = AttributeTestUtil::CreateAttrConfig(ft_string, true, packField2, 3, false);

        attr1->SetUpdatableMultiValue(isPackUpdatable);
        attr2->SetUpdatableMultiValue(isPackUpdatable);

        PackAttributeConfigPtr packAttr(
            new PackAttributeConfig(packAttrName, CompressTypeOption(), 50, std::shared_ptr<FileCompressConfig>()));

        auto status = packAttr->AddAttributeConfig(attr1);
        assert(status.IsOK());
        status = packAttr->AddAttributeConfig(attr2);
        assert(status.IsOK());

        AttributeSchemaPtr attrSchema = schema->GetAttributeSchema();
        assert(attrSchema);
        attrSchema->AddPackAttributeConfig(packAttr);
    }
    return schema;
}
}} // namespace indexlib::index
