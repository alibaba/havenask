#include "indexlib/index/calculator/test/on_disk_segment_size_calculator_unittest.h"

#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/normal/source/source_define.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::util;
using namespace indexlib::test;
using namespace indexlib::file_system;
using namespace indexlib::file_system;
using namespace indexlib::partition;
using namespace indexlib::index_base;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, OnDiskSegmentSizeCalculatorTest);

OnDiskSegmentSizeCalculatorTest::OnDiskSegmentSizeCalculatorTest() {}

OnDiskSegmentSizeCalculatorTest::~OnDiskSegmentSizeCalculatorTest() {}

void OnDiskSegmentSizeCalculatorTest::CaseSetUp()
{
    string field = "title:text;cat:uint32:true;dog:uint32:true;mice:uint32:true;abs:uint32:true";
    string index = "pk:primarykey64:title;title:pack:title";
    string attr = "cat;dog;mice";
    string summary = "abs";
    mSchema = SchemaMaker::MakeSchema(field, index, attr, summary);
    mRootDir = GET_TEMP_DATA_PATH();
}

void OnDiskSegmentSizeCalculatorTest::CaseTearDown() {}

void OnDiskSegmentSizeCalculatorTest::TestSimpleProcess()
{
    auto segmentDir = GET_PARTITION_DIRECTORY()->MakeDirectory("segment_0_level_0");
    ASSERT_NE(segmentDir, nullptr);
    PrepareSegmentData(segmentDir);
    SegmentData segmentData = CreateSegmentData(0);
    OnDiskSegmentSizeCalculator::SizeInfoMap sizeInfoMap;
    OnDiskSegmentSizeCalculator calculator;
    ASSERT_EQ((int64_t)16, calculator.CollectSegmentSizeInfo(segmentData, mSchema, sizeInfoMap));
    ASSERT_EQ(1, sizeInfoMap["attribute.cat(patch)"]);
    ASSERT_EQ(1, sizeInfoMap["attribute.dog(patch)"]);
    ASSERT_EQ(1, sizeInfoMap["attribute.mice(patch)"]);
    ASSERT_EQ(2, sizeInfoMap["summary"]);

    summarygroupid_t groupId = mSchema->GetRegionSchema(DEFAULT_REGIONID)->CreateSummaryGroup("group");
    mSchema->AddSummaryConfig("title", groupId);
    mSchema->GetRegionSchema(DEFAULT_REGIONID)->SetNeedStoreSummary();
    sizeInfoMap.clear();
    ASSERT_EQ((int64_t)18, calculator.CollectSegmentSizeInfo(segmentData, mSchema, sizeInfoMap));
    ASSERT_EQ(4, sizeInfoMap["summary"]);
}

void OnDiskSegmentSizeCalculatorTest::TestGetSegmentSize()
{
    {
        // test build segment
        auto segmentDir = GET_PARTITION_DIRECTORY()->MakeDirectory("segment_0_level_0");
        ASSERT_NE(segmentDir, nullptr);
        PrepareSegmentData(segmentDir, false);
        SegmentData segmentData = CreateSegmentData(0);
        OnDiskSegmentSizeCalculator calculator;
        ASSERT_EQ((int64_t)16, calculator.GetSegmentSize(segmentData, mSchema));
    }

    {
        // test merged segment
        tearDown();
        setUp();
        auto segmentDir = GET_PARTITION_DIRECTORY()->MakeDirectory("segment_0_level_0");
        ASSERT_NE(segmentDir, nullptr);
        PrepareSegmentData(segmentDir, true);
        SegmentData segmentData = CreateSegmentData(0);
        OnDiskSegmentSizeCalculator calculator;
        // ASSERT_EQ((int64_t)17, calculator.GetSegmentSize(segmentData, mSchema));
        ASSERT_EQ((int64_t)15, calculator.GetSegmentSize(segmentData, mSchema));
    }
}

void OnDiskSegmentSizeCalculatorTest::TestGetSegmentSourceSize()
{
    string field = "title:text;cat:uint32:true;dog:uint32:true;mice:uint32:true;abs:uint32:true";
    string index = "pk:primarykey64:title;title:pack:title";
    string attr = "cat;dog;mice";
    string summary = "abs";
    string source = "title:cat#mice:abs";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attr, summary, "", source);

    auto segmentDir = GET_PARTITION_DIRECTORY()->MakeDirectory("segment_0_level_0");
    PrepareSegmentData(segmentDir, false);
    SegmentData segmentData = CreateSegmentData(0);
    OnDiskSegmentSizeCalculator calculator;
    ASSERT_EQ((int64_t)22, calculator.GetSegmentSize(segmentData, schema));

    // test disable source, not effect on disk size calculater
    schema->GetSourceSchema()->DisableAllFields();
    ASSERT_EQ((int64_t)22, calculator.GetSegmentSize(segmentData, schema));
}

void OnDiskSegmentSizeCalculatorTest::TestCalculateWithSubSchema()
{
    auto segmentDir = GET_PARTITION_DIRECTORY()->MakeDirectory("segment_0_level_0");
    ASSERT_NE(segmentDir, nullptr);
    PrepareSegmentData(segmentDir);
    auto subSegmentDir = segmentDir->MakeDirectory(SUB_SEGMENT_DIR_NAME);
    ASSERT_NE(subSegmentDir, nullptr);
    PrepareSegmentData(subSegmentDir);

    IndexPartitionSchemaPtr subSchema(mSchema->Clone());
    mSchema->SetSubIndexPartitionSchema(subSchema);

    segmentDir->RemoveFile(SEGMENT_FILE_LIST);
    segmentDir->RemoveFile(DEPLOY_INDEX_FILE_NAME);

    DeployIndexWrapper::DumpSegmentDeployIndex(segmentDir, "");

    SegmentData segmentData = CreateSegmentData(0);
    OnDiskSegmentSizeCalculator::SizeInfoMap sizeInfoMap;
    OnDiskSegmentSizeCalculator calculator;
    ASSERT_EQ((int64_t)32, calculator.CollectSegmentSizeInfo(segmentData, mSchema, sizeInfoMap));
    ASSERT_EQ(2, sizeInfoMap["sub_summary"]);
}

void OnDiskSegmentSizeCalculatorTest::TestCalculateWithPackAttributes()
{
    auto segmentDir = GET_PARTITION_DIRECTORY()->MakeDirectory("segment_0_level_0");
    ASSERT_NE(segmentDir, nullptr);
    PrepareSegmentData(segmentDir);

    const std::string schemaPath =
        PathUtil::JoinPath(GET_PRIVATE_TEST_DATA_PATH(), "pack_attribute/schema_for_segment_lock_size_calculator.json");
    IndexPartitionSchemaPtr schema = SchemaAdapter::TEST_LoadSchema(schemaPath);
    ASSERT_NE(schema, nullptr);

    OnDiskSegmentSizeCalculator calculator;
    DeployIndexWrapper::DumpSegmentDeployIndex(segmentDir, "");
    SegmentData segmentData = CreateSegmentData(0);
    ASSERT_EQ((int64_t)6, calculator.GetSegmentSize(segmentData, schema));

    auto packDir = segmentDir->MakeDirectory(std::string(ATTRIBUTE_DIR_NAME) + "/pack_attr");
    ASSERT_NE(packDir, nullptr);
    MakeOneByteFile(packDir, ATTRIBUTE_DATA_FILE_NAME);
    MakeOneByteFile(packDir, ATTRIBUTE_OFFSET_FILE_NAME);
    segmentDir->RemoveFile(SEGMENT_FILE_LIST);
    segmentDir->RemoveFile(DEPLOY_INDEX_FILE_NAME);
    DeployIndexWrapper::DumpSegmentDeployIndex(segmentDir, "");
    SegmentData segmentData2 = CreateSegmentData(0);
    ASSERT_EQ((int64_t)8, calculator.GetSegmentSize(segmentData2, schema));
}

void OnDiskSegmentSizeCalculatorTest::TestCalculateWithPackageFile()
{
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema("pk:string;long:uint32", "pk:primarykey64:pk", "long", "pk");
    {
        PartitionStateMachine psm;
        IndexPartitionOptions options;
        ASSERT_TRUE(psm.Init(schema, options, mRootDir));
        string docString = "cmd=add,pk=1,long=1,ts=1;";
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
        docString = "cmd=update_field,pk=1,long=2,ts=2;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, docString, "", ""));
    }

    RESET_FILE_SYSTEM();
    Version version;
    VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version, INVALID_VERSION);
    merger::SegmentDirectory segDir(GET_PARTITION_DIRECTORY(), version);
    segDir.Init(false);
    SegmentData segmentData = segDir.GetPartitionData()->GetSegmentData(0);
    OnDiskSegmentSizeCalculator calculator;
    int64_t segmentSize = calculator.GetSegmentSize(segmentData, schema);
    ASSERT_GT(segmentSize, 0);

    segmentData = segDir.GetPartitionData()->GetSegmentData(1);
    OnDiskSegmentSizeCalculator::SizeInfoMap sizeInfoMap;
    OnDiskSegmentSizeCalculator seg1Calculator;
    segmentSize = seg1Calculator.CollectSegmentSizeInfo(segmentData, schema, sizeInfoMap);
    ASSERT_GT(sizeInfoMap["attribute.long(patch)"], 0);
}

void OnDiskSegmentSizeCalculatorTest::PrepareSegmentData(const file_system::DirectoryPtr& segmentDir,
                                                         bool mergedSegment)
{
    auto indexDir = segmentDir->MakeDirectory(INDEX_DIR_NAME);
    ASSERT_NE(indexDir, nullptr);
    auto titleIndexDir = indexDir->MakeDirectory("title");
    ASSERT_NE(titleIndexDir, nullptr);
    MakeOneByteFile(titleIndexDir, POSTING_FILE_NAME);
    MakeOneByteFile(titleIndexDir, DICTIONARY_FILE_NAME);
    MakeOneByteFile(titleIndexDir, BITMAP_DICTIONARY_FILE_NAME);
    MakeOneByteFile(titleIndexDir, BITMAP_POSTING_FILE_NAME);
    auto pkIndexDir = indexDir->MakeDirectory("pk");
    ASSERT_NE(pkIndexDir, nullptr);
    MakeOneByteFile(pkIndexDir, PRIMARY_KEY_DATA_FILE_NAME);
    auto pkAttrDir = pkIndexDir->MakeDirectory(string(PK_ATTRIBUTE_DIR_NAME_PREFIX) + "_title");
    ASSERT_NE(pkAttrDir, nullptr);
    MakeOneByteFile(pkAttrDir, PRIMARY_KEY_DATA_FILE_NAME);

    auto titleIndexSection = indexDir->MakeDirectory("title_section");
    ASSERT_NE(titleIndexSection, nullptr);
    MakeOneByteFile(titleIndexSection, ATTRIBUTE_DATA_FILE_NAME);
    MakeOneByteFile(titleIndexSection, ATTRIBUTE_OFFSET_FILE_NAME);

    auto attrDir = segmentDir->MakeDirectory(ATTRIBUTE_DIR_NAME);
    ASSERT_NE(attrDir, nullptr);
    auto catAttrDir = attrDir->MakeDirectory("cat");
    ASSERT_NE(catAttrDir, nullptr);
    MakeOneByteFile(catAttrDir, ATTRIBUTE_DATA_FILE_NAME);
    if (!mergedSegment) {
        MakeOneByteFile(catAttrDir, ATTRIBUTE_OFFSET_FILE_NAME);
    }
    MakeOneByteFile(catAttrDir, string("0_1.") + PATCH_FILE_NAME);

    auto dogAttrDir = attrDir->MakeDirectory("dog");
    ASSERT_NE(dogAttrDir, nullptr);
    MakeOneByteFile(dogAttrDir, ATTRIBUTE_DATA_FILE_NAME);
    MakeOneByteFile(dogAttrDir, ATTRIBUTE_OFFSET_FILE_NAME);
    MakeOneByteFile(dogAttrDir, string("0_1.") + PATCH_FILE_NAME);

    auto miceAttrDir = attrDir->MakeDirectory("mice");
    ASSERT_NE(miceAttrDir, nullptr);
    MakeOneByteFile(miceAttrDir, ATTRIBUTE_DATA_FILE_NAME);
    MakeOneByteFile(miceAttrDir, ATTRIBUTE_OFFSET_FILE_NAME);
    MakeOneByteFile(miceAttrDir, string("0_1.") + PATCH_FILE_NAME);

    auto summaryDir = segmentDir->MakeDirectory(SUMMARY_DIR_NAME);
    ASSERT_NE(summaryDir, nullptr);
    MakeOneByteFile(summaryDir, SUMMARY_DATA_FILE_NAME);
    MakeOneByteFile(summaryDir, SUMMARY_OFFSET_FILE_NAME);

    auto summaryGroupDir = summaryDir->MakeDirectory("group");
    ASSERT_NE(summaryGroupDir, nullptr);
    MakeOneByteFile(summaryGroupDir, SUMMARY_DATA_FILE_NAME);
    MakeOneByteFile(summaryGroupDir, SUMMARY_OFFSET_FILE_NAME);

    // source
    {
        auto sourceDir = segmentDir->MakeDirectory(SOURCE_DIR_NAME);
        auto sourceMetaDir = sourceDir->MakeDirectory(SOURCE_META_DIR);
        MakeOneByteFile(sourceMetaDir, SOURCE_DATA_FILE_NAME);
        MakeOneByteFile(sourceMetaDir, SOURCE_OFFSET_FILE_NAME);
        auto sourceGroupDir = sourceDir->MakeDirectory(SourceDefine::GetDataDir(0));
        MakeOneByteFile(sourceGroupDir, SOURCE_DATA_FILE_NAME);
        MakeOneByteFile(sourceGroupDir, SOURCE_OFFSET_FILE_NAME);
        sourceGroupDir = sourceDir->MakeDirectory(SourceDefine::GetDataDir(1));
        MakeOneByteFile(sourceGroupDir, SOURCE_DATA_FILE_NAME);
        MakeOneByteFile(sourceGroupDir, SOURCE_OFFSET_FILE_NAME);
    }
    DeployIndexWrapper::DumpSegmentDeployIndex(segmentDir, "");
    SegmentInfo segInfo;
    segInfo.mergedSegment = mergedSegment;
    segInfo.Store(segmentDir);
}

void OnDiskSegmentSizeCalculatorTest::PrepareKVSegmentData(const file_system::DirectoryPtr& segmentDir,
                                                           size_t columnCount, bool mergedSegment)
{
    if (!mergedSegment && columnCount > 1) {
        for (size_t i = 0; i < columnCount; ++i) {
            auto columnDir = segmentDir->MakeDirectory("column_" + std::to_string(i));
            ASSERT_NE(columnDir, nullptr);
            PrepareKVSegmentData(columnDir, 1, mergedSegment);
        }
    } else {
        auto indexDir = segmentDir->MakeDirectory(INDEX_DIR_NAME);
        ASSERT_NE(indexDir, nullptr);
        auto kvDir = indexDir->MakeDirectory("key");
        ASSERT_NE(kvDir, nullptr);
        MakeOneByteFile(kvDir, KV_KEY_FILE_NAME);
        MakeOneByteFile(kvDir, KV_VALUE_FILE_NAME);

        DeployIndexWrapper::DumpSegmentDeployIndex(segmentDir, "");
    }

    SegmentInfo segInfo;
    segInfo.shardCount = columnCount;
    segInfo.mergedSegment = mergedSegment;
    if (mergedSegment) {
        segInfo.shardId = 0;
    } else {
        segInfo.shardId = SegmentInfo::INVALID_SHARDING_ID;
    }
    segInfo.Store(segmentDir);
}

void OnDiskSegmentSizeCalculatorTest::PrepareKKVSegmentData(const file_system::DirectoryPtr& segmentDir,
                                                            size_t columnCount, bool mergedSegment)
{
    if (!mergedSegment && columnCount > 1) {
        for (size_t i = 0; i < columnCount; ++i) {
            auto columnDir = segmentDir->MakeDirectory("column_" + std::to_string(i));
            ASSERT_NE(columnDir, nullptr);
            PrepareKKVSegmentData(columnDir, 1, mergedSegment);
        }
    } else {
        auto indexDir = segmentDir->MakeDirectory(INDEX_DIR_NAME);
        ASSERT_NE(indexDir, nullptr);
        auto kkvDir = indexDir->MakeDirectory("pkey");
        ASSERT_NE(kkvDir, nullptr);
        MakeOneByteFile(kkvDir, PREFIX_KEY_FILE_NAME);
        MakeOneByteFile(kkvDir, SUFFIX_KEY_FILE_NAME);
        MakeOneByteFile(kkvDir, KKV_VALUE_FILE_NAME);

        DeployIndexWrapper::DumpSegmentDeployIndex(segmentDir, "");
    }

    SegmentInfo segInfo;
    segInfo.shardCount = columnCount;
    segInfo.mergedSegment = mergedSegment;
    if (mergedSegment) {
        segInfo.shardId = 0;
    } else {
        segInfo.shardId = SegmentInfo::INVALID_SHARDING_ID;
    }
    segInfo.Store(segmentDir);
}

void OnDiskSegmentSizeCalculatorTest::MakeOneByteFile(const file_system::DirectoryPtr& dir, const string& fileName)
{
    dir->Store(fileName, "0");
}

SegmentData OnDiskSegmentSizeCalculatorTest::CreateSegmentData(segmentid_t segmentId)
{
    Version version(0);
    version.AddSegment(segmentId);
    mSegDir.reset(new merger::SegmentDirectory(GET_PARTITION_DIRECTORY(), version));
    bool hasSub = mSchema->GetSubIndexPartitionSchema() != nullptr;
    mSegDir->Init(hasSub);
    return mSegDir->GetPartitionData()->GetSegmentData(0);
}

void OnDiskSegmentSizeCalculatorTest::TestGetKvSegmentSize()
{
    string field = "key:int32;value:uint64;";
    auto schema = SchemaMaker::MakeKVSchema(field, "key", "value");
    {
        // test build segment
        auto segmentDir = GET_PARTITION_DIRECTORY()->MakeDirectory("segment_0_level_0");
        ASSERT_NE(segmentDir, nullptr);
        PrepareKVSegmentData(segmentDir, 4, false);
        SegmentData segmentData = CreateSegmentData(0);
        OnDiskSegmentSizeCalculator calculator;
        ASSERT_EQ((int64_t)8, calculator.GetSegmentSize(segmentData, schema));
    }

    {
        tearDown();
        setUp();
        // test build segment with one column
        auto segmentDir = GET_PARTITION_DIRECTORY()->MakeDirectory("segment_0_level_0");
        ASSERT_NE(segmentDir, nullptr);
        PrepareKVSegmentData(segmentDir, 1, false);
        SegmentData segmentData = CreateSegmentData(0);
        OnDiskSegmentSizeCalculator calculator;
        ASSERT_EQ((int64_t)2, calculator.GetSegmentSize(segmentData, schema));
    }

    {
        tearDown();
        setUp();
        // test merged segment
        auto segmentDir = GET_PARTITION_DIRECTORY()->MakeDirectory("segment_0_level_0");
        ASSERT_NE(segmentDir, nullptr);
        PrepareKVSegmentData(segmentDir, 8, true);
        SegmentData segmentData = CreateSegmentData(0);
        OnDiskSegmentSizeCalculator calculator;
        ASSERT_EQ((int64_t)2, calculator.GetSegmentSize(segmentData, schema));
    }
}

void OnDiskSegmentSizeCalculatorTest::TestGetKkvSegmentSize()
{
    string field = "pkey:string;skey:int32;value:uint32;";
    auto schema = SchemaMaker::MakeKKVSchema(field, "pkey", "skey", "value;skey;");
    {
        // test build segment
        auto segmentDir = GET_PARTITION_DIRECTORY()->MakeDirectory("segment_0_level_0");
        ASSERT_NE(segmentDir, nullptr);
        PrepareKKVSegmentData(segmentDir, 4, false);
        SegmentData segmentData = CreateSegmentData(0);
        OnDiskSegmentSizeCalculator calculator;
        ASSERT_EQ((int64_t)12, calculator.GetSegmentSize(segmentData, schema));
    }

    {
        tearDown();
        setUp();
        // test build segment with one column
        auto segmentDir = GET_PARTITION_DIRECTORY()->MakeDirectory("segment_0_level_0");
        ASSERT_NE(segmentDir, nullptr);
        PrepareKKVSegmentData(segmentDir, 1, false);
        SegmentData segmentData = CreateSegmentData(0);
        OnDiskSegmentSizeCalculator calculator;
        ASSERT_EQ((int64_t)3, calculator.GetSegmentSize(segmentData, schema));
    }

    {
        tearDown();
        setUp();
        // test merged segment
        auto segmentDir = GET_PARTITION_DIRECTORY()->MakeDirectory("segment_0_level_0");
        ASSERT_NE(segmentDir, nullptr);
        PrepareKKVSegmentData(segmentDir, 8, true);
        SegmentData segmentData = CreateSegmentData(0);
        OnDiskSegmentSizeCalculator calculator;
        ASSERT_EQ((int64_t)3, calculator.GetSegmentSize(segmentData, schema));
    }
}

void OnDiskSegmentSizeCalculatorTest::TestCustomizedTable()
{
    OnDiskSegmentSizeCalculator::SizeInfoMap sizeInfoMap;
    auto segmentDir = GET_PARTITION_DIRECTORY()->MakeDirectory("segment_0_level_0");
    ASSERT_NE(segmentDir, nullptr);
    auto customDir = segmentDir->MakeDirectory(CUSTOM_DATA_DIR_NAME);
    ASSERT_NE(customDir, nullptr);
    OnDiskSegmentSizeCalculator calculator;

    auto Reset = [this, &segmentDir, &customDir]() {
        tearDown();
        setUp();
        segmentDir = GET_PARTITION_DIRECTORY()->MakeDirectory("segment_0_level_0");
        ASSERT_NE(segmentDir, nullptr);
        customDir = segmentDir->MakeDirectory(CUSTOM_DATA_DIR_NAME);
        ASSERT_NE(customDir, nullptr);

        mSchema->SetTableType(TABLE_TYPE_CUSTOMIZED);
        SegmentInfo segInfo;
        segInfo.Store(segmentDir);
    };

    {
        Reset();
        SegmentData segmentData = CreateSegmentData(0);
        ASSERT_EQ((int64_t)0, calculator.GetSegmentSize(segmentData, mSchema, true));
    }
    {
        Reset();
        segmentDir->RemoveDirectory(CUSTOM_DATA_DIR_NAME);
        SegmentData segmentData = CreateSegmentData(0);
        ASSERT_EQ((int64_t)0, calculator.GetSegmentSize(segmentData, mSchema, false));
    }
    {
        Reset();
        auto indexDir = customDir->MakeDirectory("index/pk/sk");
        ASSERT_NE(indexDir, nullptr);
        SegmentData segmentData = CreateSegmentData(0);
        ASSERT_EQ((int64_t)0, calculator.GetSegmentSize(segmentData, mSchema, true));
    }
    {
        Reset();
        auto indexDir = customDir->MakeDirectory("index/pk/sk");
        ASSERT_NE(indexDir, nullptr);
        MakeOneByteFile(indexDir, "data");
        SegmentData segmentData = CreateSegmentData(0);
        ASSERT_EQ((int64_t)1, calculator.GetSegmentSize(segmentData, mSchema, true));
        ASSERT_EQ((int64_t)1, calculator.GetSegmentSize(segmentData, mSchema, false));
    }
    {
        Reset();
        MakeOneByteFile(segmentDir, "data");
        MakeOneByteFile(customDir, "data");
        auto indexDir = customDir->MakeDirectory("index/pk/sk");
        ASSERT_NE(indexDir, nullptr);
        MakeOneByteFile(indexDir, "data");
        SegmentData segmentData = CreateSegmentData(0);
        ASSERT_EQ((int64_t)2, calculator.GetSegmentSize(segmentData, mSchema, true));
    }
    {
        Reset();
        MakeOneByteFile(segmentDir, "data");
        MakeOneByteFile(customDir, "data");
        MakeOneByteFile(customDir, "data2");
        MakeOneByteFile(customDir, "data3");
        SegmentData segmentData = CreateSegmentData(0);
        ASSERT_EQ((int64_t)3, calculator.GetSegmentSize(segmentData, mSchema, false));
        ASSERT_EQ((int64_t)3, calculator.GetSegmentSize(segmentData, mSchema, true));
    }
}
}} // namespace indexlib::index
