#include "indexlib/index/calculator/test/on_disk_segment_size_calculator_unittest.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/util/path_util.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, OnDiskSegmentSizeCalculatorTest);

OnDiskSegmentSizeCalculatorTest::OnDiskSegmentSizeCalculatorTest()
{
}

OnDiskSegmentSizeCalculatorTest::~OnDiskSegmentSizeCalculatorTest()
{
}

void OnDiskSegmentSizeCalculatorTest::CaseSetUp()
{
    string field = "title:text;cat:uint32:true;dog:uint32:true;mice:uint32:true;abs:uint32:true";
    string index = "pk:primarykey64:title;title:pack:title";
    string attr = "cat;dog;mice";
    string summary = "abs";
    mSchema = SchemaMaker::MakeSchema(field, index, attr, summary);
    mRootDir = GET_TEST_DATA_PATH();
}

void OnDiskSegmentSizeCalculatorTest::CaseTearDown()
{
}

void OnDiskSegmentSizeCalculatorTest::TestSimpleProcess()
{
    string segmentDir = FileSystemWrapper::JoinPath(mRootDir, "segment_0_level_0");
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
        //test build segment
        string segmentDir = FileSystemWrapper::JoinPath(mRootDir, "segment_0_level_0");
        PrepareSegmentData(segmentDir, false);
        SegmentData segmentData = CreateSegmentData(0);
        OnDiskSegmentSizeCalculator calculator;
        ASSERT_EQ((int64_t)16, calculator.GetSegmentSize(segmentData, mSchema));        
    }

    {
        //test merged segment
        TearDown();
        SetUp();
        string segmentDir = FileSystemWrapper::JoinPath(mRootDir, "segment_0_level_0");
        PrepareSegmentData(segmentDir, true);
        SegmentData segmentData = CreateSegmentData(0);
        OnDiskSegmentSizeCalculator calculator;
        //ASSERT_EQ((int64_t)17, calculator.GetSegmentSize(segmentData, mSchema));
        ASSERT_EQ((int64_t)15, calculator.GetSegmentSize(segmentData, mSchema));
    }
}

void OnDiskSegmentSizeCalculatorTest::TestCalculateWithSubSchema()
{
    string segmentDir = FileSystemWrapper::JoinPath(mRootDir, "segment_0_level_0");
    PrepareSegmentData(segmentDir);
    string subSegmentDir = FileSystemWrapper::JoinPath(segmentDir, SUB_SEGMENT_DIR_NAME);
    PrepareSegmentData(subSegmentDir);

    IndexPartitionSchemaPtr subSchema(mSchema->Clone());
    mSchema->SetSubIndexPartitionSchema(subSchema);

    FileSystemWrapper::DeleteFile(PathUtil::JoinPath(segmentDir, SEGMENT_FILE_LIST));
    FileSystemWrapper::DeleteFile(PathUtil::JoinPath(segmentDir, DEPLOY_INDEX_FILE_NAME));
    
    DeployIndexWrapper deployer(PathUtil::GetParentDirPath(segmentDir));
    deployer.DumpSegmentDeployIndex(PathUtil::GetFileName(segmentDir));

    SegmentData segmentData = CreateSegmentData(0);
    OnDiskSegmentSizeCalculator::SizeInfoMap sizeInfoMap;
    OnDiskSegmentSizeCalculator calculator;
    ASSERT_EQ((int64_t)32, calculator.CollectSegmentSizeInfo(segmentData, mSchema, sizeInfoMap));
    ASSERT_EQ(2, sizeInfoMap["sub_summary"]);
}

void OnDiskSegmentSizeCalculatorTest::TestCalculateWithPackAttributes()
{
    string segmentDir = FileSystemWrapper::JoinPath(mRootDir, "segment_0_level_0");
    PrepareSegmentData(segmentDir);
    
    IndexPartitionSchemaPtr schema  = SchemaAdapter::LoadSchema(
        string(TEST_DATA_PATH) + "pack_attribute/",
        "schema_for_segment_lock_size_calculator.json");

    OnDiskSegmentSizeCalculator calculator;

    DeployIndexWrapper deployer(PathUtil::GetParentDirPath(segmentDir));
    deployer.DumpSegmentDeployIndex(PathUtil::GetFileName(segmentDir));
    SegmentData segmentData = CreateSegmentData(0);
    ASSERT_EQ((int64_t)6, calculator.GetSegmentSize(segmentData, schema));

    string attrDir = FileSystemWrapper::JoinPath(segmentDir, ATTRIBUTE_DIR_NAME);
    string packDir = FileSystemWrapper::JoinPath(attrDir, "pack_attr");
    MakeOneByteFile(packDir, ATTRIBUTE_DATA_FILE_NAME);
    MakeOneByteFile(packDir, ATTRIBUTE_OFFSET_FILE_NAME);
    FileSystemWrapper::DeleteFile(PathUtil::JoinPath(segmentDir, SEGMENT_FILE_LIST));
    FileSystemWrapper::DeleteFile(PathUtil::JoinPath(segmentDir, DEPLOY_INDEX_FILE_NAME));
    deployer.DumpSegmentDeployIndex(PathUtil::GetFileName(segmentDir));
    SegmentData segmentData2 = CreateSegmentData(0);
    ASSERT_EQ((int64_t)8, calculator.GetSegmentSize(segmentData2, schema));
}

void OnDiskSegmentSizeCalculatorTest::TestCalculateWithPackageFile()
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
            "pk:string;long:uint32", "pk:primarykey64:pk", "long", "pk");
    {
        PartitionStateMachine psm;
        IndexPartitionOptions options;
        ASSERT_TRUE(psm.Init(schema, options, mRootDir));
        string docString = "cmd=add,pk=1,long=1,ts=1;";
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
        docString = "cmd=update_field,pk=1,long=2,ts=2;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, docString, "", ""));
    }

    Version version;
    VersionLoader::GetVersion(mRootDir, version, INVALID_VERSION);
    merger::SegmentDirectory segDir(mRootDir, version);
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

void OnDiskSegmentSizeCalculatorTest::PrepareSegmentData(const string& segmentDir,
                                                         bool mergedSegment)
{
    string indexDir = FileSystemWrapper::JoinPath(segmentDir, INDEX_DIR_NAME);
    string titleIndexDir = FileSystemWrapper::JoinPath(indexDir, "title");
    MakeOneByteFile(titleIndexDir, POSTING_FILE_NAME);
    MakeOneByteFile(titleIndexDir, DICTIONARY_FILE_NAME);
    MakeOneByteFile(titleIndexDir, BITMAP_DICTIONARY_FILE_NAME);
    MakeOneByteFile(titleIndexDir, BITMAP_POSTING_FILE_NAME);
    string pkIndexDir = FileSystemWrapper::JoinPath(indexDir, "pk");
    MakeOneByteFile(pkIndexDir, PRIMARY_KEY_DATA_FILE_NAME);
    string pkAttrDir = FileSystemWrapper::JoinPath(pkIndexDir, string(PK_ATTRIBUTE_DIR_NAME_PREFIX) + "_title");
    MakeOneByteFile(pkAttrDir, PRIMARY_KEY_DATA_FILE_NAME);
    
    string titleIndexSection = FileSystemWrapper::JoinPath(indexDir, "title_section");
    MakeOneByteFile(titleIndexSection, ATTRIBUTE_DATA_FILE_NAME);
    MakeOneByteFile(titleIndexSection, ATTRIBUTE_OFFSET_FILE_NAME);
    
    string attrDir = FileSystemWrapper::JoinPath(segmentDir, ATTRIBUTE_DIR_NAME);
    string catAttrDir = FileSystemWrapper::JoinPath(attrDir, "cat");
    MakeOneByteFile(catAttrDir, ATTRIBUTE_DATA_FILE_NAME);
    if (!mergedSegment)
    {
        MakeOneByteFile(catAttrDir, ATTRIBUTE_OFFSET_FILE_NAME);   
    }
    MakeOneByteFile(catAttrDir, string("0_1.") + ATTRIBUTE_PATCH_FILE_NAME);

    string dogAttrDir = FileSystemWrapper::JoinPath(attrDir, "dog");
    MakeOneByteFile(dogAttrDir, ATTRIBUTE_DATA_FILE_NAME);
    MakeOneByteFile(dogAttrDir, ATTRIBUTE_OFFSET_FILE_NAME);
    MakeOneByteFile(dogAttrDir, string("0_1.") + ATTRIBUTE_PATCH_FILE_NAME);

    string miceAttrDir = FileSystemWrapper::JoinPath(attrDir, "mice");
    MakeOneByteFile(miceAttrDir, ATTRIBUTE_DATA_FILE_NAME);
    MakeOneByteFile(miceAttrDir, ATTRIBUTE_OFFSET_FILE_NAME);
    MakeOneByteFile(miceAttrDir, string("0_1.") + ATTRIBUTE_PATCH_FILE_NAME);
    
    string summaryDir = FileSystemWrapper::JoinPath(segmentDir, SUMMARY_DIR_NAME);
    MakeOneByteFile(summaryDir, SUMMARY_DATA_FILE_NAME);
    MakeOneByteFile(summaryDir, SUMMARY_OFFSET_FILE_NAME);

    string summaryGroupDir = FileSystemWrapper::JoinPath(summaryDir, "group");
    MakeOneByteFile(summaryGroupDir, SUMMARY_DATA_FILE_NAME);
    MakeOneByteFile(summaryGroupDir, SUMMARY_OFFSET_FILE_NAME);

    DeployIndexWrapper deployer(PathUtil::GetParentDirPath(segmentDir));
    deployer.DumpSegmentDeployIndex(PathUtil::GetFileName(segmentDir));
    
    string segInfoPath = FileSystemWrapper::JoinPath(segmentDir, SEGMENT_INFO_FILE_NAME);
    SegmentInfo segInfo;
    segInfo.mergedSegment = mergedSegment;
    segInfo.Store(segInfoPath);
}

void OnDiskSegmentSizeCalculatorTest::PrepareKVSegmentData(const string& segmentDir,
        size_t columnCount,
        bool mergedSegment)
{
    if (!mergedSegment && columnCount > 1)
    {
        for (size_t i = 0; i < columnCount; ++i)
        {
            string columnDir = FileSystemWrapper::JoinPath(segmentDir,
                    "column_" + std::to_string(i));
            PrepareKVSegmentData(columnDir, 1, mergedSegment);
        }
    }
    else
    {
        string indexDir = FileSystemWrapper::JoinPath(segmentDir, INDEX_DIR_NAME);
        string kvDir = FileSystemWrapper::JoinPath(indexDir, "key");
        MakeOneByteFile(kvDir, KV_KEY_FILE_NAME);
        MakeOneByteFile(kvDir, KV_VALUE_FILE_NAME);

        DeployIndexWrapper deployer(PathUtil::GetParentDirPath(segmentDir));
        deployer.DumpSegmentDeployIndex(PathUtil::GetFileName(segmentDir));
    }

    string segInfoPath = FileSystemWrapper::JoinPath(segmentDir, SEGMENT_INFO_FILE_NAME);
    SegmentInfo segInfo;
    segInfo.shardingColumnCount = columnCount;
    segInfo.mergedSegment = mergedSegment;
    if (mergedSegment) {
        segInfo.shardingColumnId = 0;
    } else {
        segInfo.shardingColumnId = SegmentInfo::INVALID_SHARDING_ID;
    }
    segInfo.Store(segInfoPath);
}

void OnDiskSegmentSizeCalculatorTest::PrepareKKVSegmentData(const string& segmentDir,
        size_t columnCount,
        bool mergedSegment)
{
    if (!mergedSegment && columnCount > 1)
    {
        for (size_t i = 0; i < columnCount; ++i)
        {
            string columnDir = FileSystemWrapper::JoinPath(segmentDir,
                    "column_" + std::to_string(i));
            PrepareKKVSegmentData(columnDir, 1, mergedSegment);
        }
    }
    else
    {
        string indexDir = FileSystemWrapper::JoinPath(segmentDir, INDEX_DIR_NAME);
        string kkvDir = FileSystemWrapper::JoinPath(indexDir, "pkey");
        MakeOneByteFile(kkvDir, PREFIX_KEY_FILE_NAME);
        MakeOneByteFile(kkvDir, SUFFIX_KEY_FILE_NAME);
        MakeOneByteFile(kkvDir, KKV_VALUE_FILE_NAME);

        DeployIndexWrapper deployer(PathUtil::GetParentDirPath(segmentDir));
        deployer.DumpSegmentDeployIndex(PathUtil::GetFileName(segmentDir));
    }

    string segInfoPath = FileSystemWrapper::JoinPath(segmentDir, SEGMENT_INFO_FILE_NAME);
    SegmentInfo segInfo;
    segInfo.shardingColumnCount = columnCount;
    segInfo.mergedSegment = mergedSegment;
    if (mergedSegment) {
        segInfo.shardingColumnId = 0;
    } else {
        segInfo.shardingColumnId = SegmentInfo::INVALID_SHARDING_ID;
    }
    segInfo.Store(segInfoPath);
}

void OnDiskSegmentSizeCalculatorTest::MakeOneByteFile(
        const string& dir, const string& fileName)
{
    string filePath = FileSystemWrapper::JoinPath(dir, fileName);
    FileSystemWrapper::AtomicStore(filePath, "0");
}

SegmentData OnDiskSegmentSizeCalculatorTest::CreateSegmentData(
        segmentid_t segmentId)
{
    Version version(0);
    version.AddSegment(segmentId);
    mSegDir.reset(new merger::SegmentDirectory(mRootDir, version));
    mSegDir->Init(mSchema->GetSubIndexPartitionSchema());
    return mSegDir->GetPartitionData()->GetSegmentData(0);
}

void OnDiskSegmentSizeCalculatorTest::TestGetKvSegmentSize()
{
    string field = "key:int32;value:uint64;";
    auto schema = SchemaMaker::MakeKVSchema(field, "key", "value");
    {
        //test build segment
        string segmentDir = FileSystemWrapper::JoinPath(mRootDir, "segment_0_level_0");
        PrepareKVSegmentData(segmentDir, 4, false);
        SegmentData segmentData = CreateSegmentData(0);
        OnDiskSegmentSizeCalculator calculator;
        ASSERT_EQ((int64_t)8, calculator.GetSegmentSize(segmentData, schema));
    }

    {
        TearDown();
        SetUp();
        //test build segment with one column
        string segmentDir = FileSystemWrapper::JoinPath(mRootDir, "segment_0_level_0");
        PrepareKVSegmentData(segmentDir, 1, false);
        SegmentData segmentData = CreateSegmentData(0);
        OnDiskSegmentSizeCalculator calculator;
        ASSERT_EQ((int64_t)2, calculator.GetSegmentSize(segmentData, schema));
    }

    {
        TearDown();
        SetUp();
        //test merged segment
        string segmentDir = FileSystemWrapper::JoinPath(mRootDir, "segment_0_level_0");
        PrepareKVSegmentData(segmentDir, 8, true);
        SegmentData segmentData = CreateSegmentData(0);
        OnDiskSegmentSizeCalculator calculator;
        ASSERT_EQ((int64_t)2, calculator.GetSegmentSize(segmentData, schema));
    }
}

void OnDiskSegmentSizeCalculatorTest::TestGetKkvSegmentSize()
{
    string field = "pkey:string;skey:int32;value:uint32;";
    auto schema = SchemaMaker::MakeKKVSchema(
            field, "pkey", "skey", "value;skey;");
    {
        //test build segment
        string segmentDir = FileSystemWrapper::JoinPath(mRootDir, "segment_0_level_0");
        PrepareKKVSegmentData(segmentDir, 4, false);
        SegmentData segmentData = CreateSegmentData(0);
        OnDiskSegmentSizeCalculator calculator;
        ASSERT_EQ((int64_t)12, calculator.GetSegmentSize(segmentData, schema));
    }

    {
        TearDown();
        SetUp();
        //test build segment with one column
        string segmentDir = FileSystemWrapper::JoinPath(mRootDir, "segment_0_level_0");
        PrepareKKVSegmentData(segmentDir, 1, false);
        SegmentData segmentData = CreateSegmentData(0);
        OnDiskSegmentSizeCalculator calculator;
        ASSERT_EQ((int64_t)3, calculator.GetSegmentSize(segmentData, schema));
    }

    {
        TearDown();
        SetUp();
        //test merged segment
        string segmentDir = FileSystemWrapper::JoinPath(mRootDir, "segment_0_level_0");
        PrepareKKVSegmentData(segmentDir, 8, true);
        SegmentData segmentData = CreateSegmentData(0);
        OnDiskSegmentSizeCalculator calculator;
        ASSERT_EQ((int64_t)3, calculator.GetSegmentSize(segmentData, schema));
    }
}

void OnDiskSegmentSizeCalculatorTest::TestCustomizedTable()
{
    OnDiskSegmentSizeCalculator::SizeInfoMap sizeInfoMap;
    string segmentDir = FileSystemWrapper::JoinPath(mRootDir, "segment_0_level_0");
    string customDir = FileSystemWrapper::JoinPath(segmentDir, CUSTOM_DATA_DIR_NAME);
    OnDiskSegmentSizeCalculator calculator;
    
    auto Reset = [this, segmentDir, customDir] (){
        TearDown();
        SetUp();
        mSchema->SetTableType(TABLE_TYPE_CUSTOMIZED);
        string segInfoPath = FileSystemWrapper::JoinPath(segmentDir, SEGMENT_INFO_FILE_NAME);
        SegmentInfo segInfo;
        segInfo.Store(segInfoPath);
        FileSystemWrapper::MkDir(customDir, true);
    };
    
    {
        Reset();
        SegmentData segmentData = CreateSegmentData(0);
        ASSERT_EQ((int64_t)0, calculator.GetSegmentSize(segmentData, mSchema, true));
    }
    {
        Reset();
        FileSystemWrapper::DeleteDir(customDir);
        SegmentData segmentData = CreateSegmentData(0);
        ASSERT_EQ((int64_t)0, calculator.GetSegmentSize(segmentData, mSchema, false));
    }
    {
        Reset();
        string indexDir = FileSystemWrapper::JoinPath(customDir, "index/pk/sk");
        FileSystemWrapper::MkDir(indexDir, true);
        SegmentData segmentData = CreateSegmentData(0);
        ASSERT_EQ((int64_t)0, calculator.GetSegmentSize(segmentData, mSchema, true));
    }
    {
        Reset();
        string indexDir = FileSystemWrapper::JoinPath(customDir, "index/pk/sk");
        FileSystemWrapper::MkDir(indexDir, true);
        MakeOneByteFile(indexDir, "data");
        SegmentData segmentData = CreateSegmentData(0);
        ASSERT_EQ((int64_t)1, calculator.GetSegmentSize(segmentData, mSchema, true));
        ASSERT_EQ((int64_t)1, calculator.GetSegmentSize(segmentData, mSchema, false));
    }
    {
        Reset();
        MakeOneByteFile(segmentDir, "data");
        MakeOneByteFile(customDir, "data");
        string indexDir = FileSystemWrapper::JoinPath(customDir, "index/pk/sk");
        FileSystemWrapper::MkDir(indexDir, true);
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

IE_NAMESPACE_END(index);

