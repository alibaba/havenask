#include "indexlib/index/calculator/test/segment_lock_size_calculator_unittest.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/util/path_util.h"
#include "indexlib/file_system/in_mem_directory.h"
#include "indexlib/file_system/local_directory.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/common/file_system_factory.h"
#include "indexlib/file_system/test/load_config_list_creator.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/impl/index_partition_schema_impl.h"
#include "indexlib/util/env_util.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, SegmentLockSizeCalculatorTest);

SegmentLockSizeCalculatorTest::SegmentLockSizeCalculatorTest()
{
}

SegmentLockSizeCalculatorTest::~SegmentLockSizeCalculatorTest()
{
}

void SegmentLockSizeCalculatorTest::CaseSetUp()
{
    const IndexlibFileSystemPtr& fs = GET_FILE_SYSTEM();
    string fsRoot = fs->GetRootPath();
    fs->MountInMemStorage(PathUtil::JoinPath(fsRoot, "test"));
    mRootDir.reset(new InMemDirectory(PathUtil::JoinPath(fsRoot, "test"), fs));

    string localDir = PathUtil::JoinPath(fsRoot, "local");
    fs->MakeDirectory(localDir);
    mLocalDirectory.reset(new LocalDirectory(localDir, fs));

    string field = "title:string;cat:uint32:true;dog:uint32;mice:uint32;abs:uint32";
    string index = "pk:primarykey64:title;title:string:title";
    string attr = "cat;dog;mice;";
    string summary = "abs";
    mSchema = SchemaMaker::MakeSchema(field, index, attr, summary);
}

void SegmentLockSizeCalculatorTest::CaseTearDown()
{
}

void SegmentLockSizeCalculatorTest::TestSimpleProcess()
{
    DirectoryPtr indexDir = mRootDir->MakeDirectory("index");
    DirectoryPtr indexNameDir = indexDir->MakeDirectory("title");
    indexNameDir->Store("dictionary", "0123");

    SegmentLockSizeCalculator calculator(mSchema, plugin::PluginManagerPtr());
    ASSERT_EQ((size_t)4, calculator.CalculateSize(mRootDir));
}

void SegmentLockSizeCalculatorTest::TestCalculateSize()
{
    PrepareData(mRootDir);
    // no section attribute
    {
        SegmentLockSizeCalculator calculator(mSchema, plugin::PluginManagerPtr());
        ASSERT_EQ((size_t)265, calculator.CalculateSize(mRootDir));
    }

    // test has section
    {
        string field = "title:text;cat:uint32:true;abs:uint32";
        string index = "pk:primarykey64:title;title:pack:title";
        string attr = "cat";
        string summary = "abs";
        IndexPartitionSchemaPtr schema =
            SchemaMaker::MakeSchema(field, index, attr, summary);
        SegmentLockSizeCalculator calculator(schema, plugin::PluginManagerPtr());
        ASSERT_EQ((size_t)265, calculator.CalculateSize(mRootDir));
    }

    // test pack attributes
    {
        DirectoryPtr attrDir = mRootDir->GetDirectory(ATTRIBUTE_DIR_NAME, true);
        DirectoryPtr packDir = attrDir->MakeDirectory("pack_attr");

        IndexPartitionSchemaPtr schema  = SchemaAdapter::LoadSchema(
            string(TEST_DATA_PATH) + "pack_attribute/",
            "schema_for_segment_lock_size_calculator.json");

        // this schema has no inverted index.
        SegmentLockSizeCalculator calculator(schema, plugin::PluginManagerPtr());
        ASSERT_EQ((size_t)4, calculator.CalculateSize(mRootDir));

        MakeOneByteFile(packDir, ATTRIBUTE_DATA_FILE_NAME);
        MakeOneByteFile(packDir, ATTRIBUTE_OFFSET_FILE_NAME);

        ASSERT_EQ((size_t)6, calculator.CalculateSize(mRootDir));
    }
}

void SegmentLockSizeCalculatorTest::TestCalculateRangeIndexSize()
{
    util::EnvUtil::SetEnv("RS_BLOCK_CACHE", "cache_size=1");
    PrepareData(mRootDir);
    string field = "title:text;cat:uint32:true;abs:uint32;ts:int64";
    string index = "pk:primarykey64:title;range:range:ts;";
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(field, index, "", "");
    SegmentLockSizeCalculator calculator(schema, plugin::PluginManagerPtr());
    // test range mmap load
    ASSERT_EQ((size_t)514, calculator.CalculateSize(mRootDir));
    // test range cache load
    string loadConfigStr = R"(
    {
        "load_config":
        [
            {
                "file_patterns" : ["/index/.*/dictionary"],
                "load_strategy" : "cache",
                "load_strategy_param": {
                    "direct_io":true
                }
            },
            {
                "file_patterns" : ["/index/.*/posting"],
                "load_strategy" : "mmap",
                "load_strategy_param": {
                    "lock":true
                }
            },
            {
                "file_patterns" : [".*"],
                "load_strategy" : "mmap",
                "load_strategy_param" : {
                    "lock" : false
                }
            }
        ]
    })";

    config::LoadConfigList loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(loadConfigStr);
    RESET_FILE_SYSTEM(loadConfigList);
    const IndexlibFileSystemPtr& fs = GET_FILE_SYSTEM();
    string fsRoot = fs->GetRootPath();
    string localDir = PathUtil::JoinPath(fsRoot, "local");
    mLocalDirectory.reset(new LocalDirectory(localDir, fs));
    PrepareData(mLocalDirectory);
    ASSERT_EQ((size_t)514, calculator.CalculateSize(mLocalDirectory));
    util::EnvUtil::UnsetEnv("RS_BLOCK_CACHE");
}

void SegmentLockSizeCalculatorTest::TestCalculateSummarySize()
{
    string field = "title:text;cat:uint32:true;abs:uint32";
    string index = "pk:primarykey64:title;title:pack:title";
    PrepareData(mRootDir);
    // test summary in attribute
    {
        string attr = "cat";
        string summary = "cat";
        IndexPartitionSchemaPtr schema =
            SchemaMaker::MakeSchema(field, index, attr, summary);
        SegmentLockSizeCalculator calculator(schema, plugin::PluginManagerPtr());
        ASSERT_EQ((size_t)263, calculator.CalculateSize(mRootDir));
    }

    // test default summary in attribute, but group not.
    {
        string attr = "cat";
        string summary = "cat";
        IndexPartitionSchemaPtr schema =
            SchemaMaker::MakeSchema(field, index, attr, summary);
        RegionSchemaPtr regionSchema = schema->GetRegionSchema(DEFAULT_REGIONID);
        summarygroupid_t groupId = regionSchema->CreateSummaryGroup("group");
        regionSchema->AddSummaryConfig("abs", groupId);
        regionSchema->SetNeedStoreSummary();
        SegmentLockSizeCalculator calculator(schema, plugin::PluginManagerPtr());
        ASSERT_EQ((size_t)265, calculator.CalculateSize(mRootDir));
    }
    // test default & group all not in attribute
    {
        string attr = "cat";
        string summary = "title";
        IndexPartitionSchemaPtr schema =
            SchemaMaker::MakeSchema(field, index, attr, summary);
        SegmentLockSizeCalculator calculator(schema, plugin::PluginManagerPtr());
        ASSERT_EQ((size_t)265, calculator.CalculateSize(mRootDir));

        RegionSchemaPtr regionSchema = schema->GetRegionSchema(DEFAULT_REGIONID);
        summarygroupid_t groupId = regionSchema->CreateSummaryGroup("group");
        regionSchema->AddSummaryConfig("abs", groupId);
        regionSchema->SetNeedStoreSummary();
        ASSERT_EQ((size_t)267, calculator.CalculateSize(mRootDir));
    }
    // test default summary not in attribute, but group in attribute
    {
        string attr = "cat";
        string summary = "abs";
        IndexPartitionSchemaPtr schema =
            SchemaMaker::MakeSchema(field, index, attr, summary);
        SegmentLockSizeCalculator calculator(schema, plugin::PluginManagerPtr());
        ASSERT_EQ((size_t)265, calculator.CalculateSize(mRootDir));

        RegionSchemaPtr regionSchema = schema->GetRegionSchema(DEFAULT_REGIONID);
        summarygroupid_t groupId = regionSchema->CreateSummaryGroup("group");
        regionSchema->AddSummaryConfig("cat", groupId);
        regionSchema->SetNeedStoreSummary();
        ASSERT_EQ((size_t)265, calculator.CalculateSize(mRootDir));
    }
}

void SegmentLockSizeCalculatorTest::InnerTestCalculateSizeWithLoadConfig(
        const string& loadConfigStr, size_t expectSize)
{
    TearDown();
    SetUp();
    PrepareData(mLocalDirectory);
    IndexPartitionOptions options;
    OnlineConfig& onlineConfig = options.GetOnlineConfig();
    LoadConfigList& loadConfigList = onlineConfig.loadConfigList;
    loadConfigList.Clear();
    LoadConfig loadConfig;
    FromJsonString(loadConfig, loadConfigStr);
    loadConfigList.PushBack(loadConfig);
    IndexlibFileSystemPtr fileSystem =
        common::FileSystemFactory::Create(mLocalDirectory->GetPath(), "", options, NULL);
    DirectoryPtr segDirectory(new LocalDirectory(mLocalDirectory->GetPath(),
                    fileSystem));
    SegmentLockSizeCalculator calculator(mSchema, plugin::PluginManagerPtr());
    ASSERT_EQ((size_t)expectSize, calculator.CalculateSize(segDirectory)) << loadConfigStr;
}

void SegmentLockSizeCalculatorTest::TestCalculateSizeWithDiffType()
{
    {
        //test cache load config
        string jsonStr = "\
        {                                                        \
            \"file_patterns\" : [\".*\"],                        \
            \"load_strategy\" : \"cache\",                       \
            \"load_strategy_param\" : {                          \
            \"cache_size\" : 512                                 \
            }                                                    \
        }";
        //dictionary(1) + bitmap(2) + summary offset(1)
        InnerTestCalculateSizeWithLoadConfig(jsonStr, 4);
    }

    {
        //test mmap no lock
        string jsonStr = "\
        {                                                        \
            \"file_patterns\" : [\".*\"],                        \
            \"load_strategy\" : \"mmap\",                        \
            \"load_strategy_param\" : {                          \
            \"lock\" : false                                     \
            }                                                    \
        }";
        InnerTestCalculateSizeWithLoadConfig(jsonStr, 0);
    }

    {
        //test mmap lock
        string jsonStr = "\
        {                                                        \
            \"file_patterns\" : [\".*\"],                        \
            \"load_strategy\" : \"mmap\",                        \
            \"load_strategy_param\" : {                          \
            \"lock\" : true                                      \
            }                                                    \
        }";
        InnerTestCalculateSizeWithLoadConfig(jsonStr, 265);
    }
}

void SegmentLockSizeCalculatorTest::TestCalculateSizeWithMultiShardingIndex()
{
    PrepareData(mRootDir);
    string field = "title:string;cat:uint32:true;dog:uint32;mice:uint32;abs:uint32";
    string index = "pk:primarykey64:title;title:string:title:false:3";
    string attr = "cat;dog;mice;";
    string summary = "abs";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attr, summary);

    DirectoryPtr indexDir = mRootDir->GetDirectory(INDEX_DIR_NAME, true);
    DirectoryPtr sIndexDir0 = indexDir->MakeDirectory("title_@_0");
    MakeOneByteFile(sIndexDir0, POSTING_FILE_NAME);
    MakeOneByteFile(sIndexDir0, DICTIONARY_FILE_NAME);
    MakeOneByteFile(sIndexDir0, BITMAP_DICTIONARY_FILE_NAME);
    MakeOneByteFile(sIndexDir0, BITMAP_POSTING_FILE_NAME);

    DirectoryPtr sIndexDir1 = indexDir->MakeDirectory("title_@_1");
    MakeOneByteFile(sIndexDir1, POSTING_FILE_NAME);
    MakeOneByteFile(sIndexDir1, DICTIONARY_FILE_NAME);
    MakeOneByteFile(sIndexDir1, BITMAP_DICTIONARY_FILE_NAME);
    MakeOneByteFile(sIndexDir1, BITMAP_POSTING_FILE_NAME);

    DirectoryPtr sIndexDir2 = indexDir->MakeDirectory("title_@_2");
    MakeOneByteFile(sIndexDir2, POSTING_FILE_NAME);
    MakeOneByteFile(sIndexDir2, DICTIONARY_FILE_NAME);

    DirectoryPtr nonExistIndexDir = indexDir->MakeDirectory("title_@_3");
    MakeOneByteFile(nonExistIndexDir, POSTING_FILE_NAME);
    MakeOneByteFile(nonExistIndexDir, DICTIONARY_FILE_NAME);

    SegmentLockSizeCalculator calculator(schema, plugin::PluginManagerPtr());
    ASSERT_EQ((size_t)275, calculator.CalculateSize(mRootDir));
}

void SegmentLockSizeCalculatorTest::MakeMultiByteFile(
        const DirectoryPtr& dir, const string& fileName, size_t count)
{
    string data(count, '0');
    dir->Store(fileName, data);
}

void SegmentLockSizeCalculatorTest::MakeOneByteFile(
        const DirectoryPtr& dir, const string& fileName)
{
    dir->Store(fileName, "0");
}

void SegmentLockSizeCalculatorTest::PrepareData(const DirectoryPtr& dir)
{
    DirectoryPtr indexDir = dir->MakeDirectory(INDEX_DIR_NAME);

    DirectoryPtr titleIndexDir = indexDir->MakeDirectory("title");
    MakeOneByteFile(titleIndexDir, POSTING_FILE_NAME);
    MakeMultiByteFile(titleIndexDir, DICTIONARY_FILE_NAME, 256);
    MakeOneByteFile(titleIndexDir, BITMAP_DICTIONARY_FILE_NAME);
    MakeOneByteFile(titleIndexDir, BITMAP_POSTING_FILE_NAME);

    DirectoryPtr rangeIndexDir = indexDir->MakeDirectory("range");
    DirectoryPtr lowIndexDir = rangeIndexDir->MakeDirectory("range_@_bottom");
    DirectoryPtr highIndexDir = rangeIndexDir->MakeDirectory("range_@_high");
    MakeOneByteFile(lowIndexDir, POSTING_FILE_NAME);
    MakeMultiByteFile(lowIndexDir, DICTIONARY_FILE_NAME, 256);
    MakeOneByteFile(highIndexDir, POSTING_FILE_NAME);
    MakeMultiByteFile(highIndexDir, DICTIONARY_FILE_NAME, 256);
    //pk size not calculate
    DirectoryPtr pkIndexDir = indexDir->MakeDirectory("pk");
    MakeOneByteFile(pkIndexDir, PRIMARY_KEY_DATA_FILE_NAME);
    DirectoryPtr pkAttrDir = pkIndexDir->MakeDirectory(string(PK_ATTRIBUTE_DIR_NAME_PREFIX) +
    "_title");
    MakeOneByteFile(pkAttrDir, PRIMARY_KEY_DATA_FILE_NAME);

    DirectoryPtr titleIndexSection = indexDir->MakeDirectory("title_section");
    MakeOneByteFile(titleIndexSection, ATTRIBUTE_DATA_FILE_NAME);
    MakeOneByteFile(titleIndexSection, ATTRIBUTE_OFFSET_FILE_NAME);

    DirectoryPtr attrDir = dir->MakeDirectory(ATTRIBUTE_DIR_NAME);
    DirectoryPtr catAttrDir = attrDir->MakeDirectory("cat");
    MakeOneByteFile(catAttrDir, ATTRIBUTE_DATA_FILE_NAME);
    MakeOneByteFile(catAttrDir, ATTRIBUTE_OFFSET_FILE_NAME);

    DirectoryPtr dogAttrDir = attrDir->MakeDirectory("dog");
    MakeOneByteFile(dogAttrDir, ATTRIBUTE_DATA_FILE_NAME);

    DirectoryPtr miceAttrDir = attrDir->MakeDirectory("mice");
    MakeOneByteFile(miceAttrDir, ATTRIBUTE_DATA_FILE_NAME);


    // segment calculator not calculate patch size
    MakeOneByteFile(catAttrDir, string("0_1.") + ATTRIBUTE_PATCH_FILE_NAME);

    DirectoryPtr summaryDir = dir->MakeDirectory(SUMMARY_DIR_NAME);
    MakeOneByteFile(summaryDir, SUMMARY_DATA_FILE_NAME);
    MakeOneByteFile(summaryDir, SUMMARY_OFFSET_FILE_NAME);

    DirectoryPtr summaryGroupDir = summaryDir->MakeDirectory("group");
    MakeOneByteFile(summaryGroupDir, SUMMARY_DATA_FILE_NAME);
    MakeOneByteFile(summaryGroupDir, SUMMARY_OFFSET_FILE_NAME);
}

void SegmentLockSizeCalculatorTest::TestDisable()
{
    // TODO test pack attribute
    PrepareData(mRootDir);
    // disable index & attribute
    string field = "title:text;cat:uint32:true;abs:uint32;dog:uint32";
    string index = "pk:primarykey64:title;title:pack:title";
    string attr = "cat";
    string summary = "abs";
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(field, index, attr, summary);
    SchemaModifyOperationPtr modifyOp(new SchemaModifyOperation);
    {
        //263bytes
        string jsonStr = R"({
 	"indexs" : ["title"],
        "attributes" : ["cat"]
        })";
        json::JsonMap jsonMap;
        FromJsonString(jsonMap, jsonStr);
        autil::legacy::Any any = autil::legacy::ToJson(&jsonMap);
        modifyOp->LoadDeleteOperation(any, *(schema->mImpl));
    }
    {
        // add one attribute
        string jsonStr = R"({
        "attributes" : ["dog"]
        })";
        json::JsonMap jsonMap;
        FromJsonString(jsonMap, jsonStr);
        autil::legacy::Any any = autil::legacy::ToJson(&jsonMap);
        modifyOp->LoadAddOperation(any, *(schema->mImpl));
    }
    SegmentLockSizeCalculator calculator(schema, plugin::PluginManagerPtr());
    schema->mImpl->AddSchemaModifyOperation(modifyOp);
    ASSERT_EQ((size_t)3, calculator.CalculateSize(mRootDir));

    schema->MarkOngoingModifyOperation(1);
    ASSERT_EQ((size_t)2, calculator.CalculateSize(mRootDir));


}

IE_NAMESPACE_END(index);
