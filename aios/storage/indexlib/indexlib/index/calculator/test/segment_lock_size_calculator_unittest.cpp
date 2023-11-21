#include "indexlib/index/calculator/test/segment_lock_size_calculator_unittest.h"

#include "autil/EnvUtil.h"
#include "indexlib/common/file_system_factory.h"
#include "indexlib/config/impl/index_partition_schema_impl.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/file_system/DirectoryOption.h"
#include "indexlib/file_system/LocalDirectory.h"
#include "indexlib/file_system/MemDirectory.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/index/normal/source/source_define.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/index_define.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::test;
using namespace indexlib::file_system;
using namespace indexlib::util;
using namespace indexlib::index_base;
using namespace indexlib::common;
namespace indexlib { namespace index {
IE_LOG_SETUP(index, SegmentLockSizeCalculatorTest);

SegmentLockSizeCalculatorTest::SegmentLockSizeCalculatorTest() {}

SegmentLockSizeCalculatorTest::~SegmentLockSizeCalculatorTest() {}

void SegmentLockSizeCalculatorTest::CaseSetUp()
{
    const IFileSystemPtr& fs = GET_FILE_SYSTEM();
    mRootDir = IDirectory::ToLegacyDirectory(make_shared<MemDirectory>("test", fs));

    string localDir = "local";
    ASSERT_EQ(FSEC_OK, fs->MakeDirectory(localDir, DirectoryOption()));
    mLocalDirectory = IDirectory::ToLegacyDirectory(make_shared<LocalDirectory>(localDir, fs));

    string field = "title:string;cat:uint32:true;dog:uint32;mice:uint32;abs:uint32";
    string index = "pk:primarykey64:title;title:string:title";
    string attr = "cat;dog;mice;";
    string summary = "abs";
    mSchema = SchemaMaker::MakeSchema(field, index, attr, summary);

    mCacheLoadConfigForRange = R"(
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

    mMMapLockLoadConfigForRange = R"(
    {
        "load_config":
        [
            {
                "file_patterns" : ["/index/.*/dictionary"],
                "load_strategy" : "mmap",
                "load_strategy_param": {
                    "lock":true
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

    mMMapNoLockLoadConfigForRange = R"(
    {
        "load_config":
        [
            {
                "file_patterns" : ["/index/.*/dictionary"],
                "load_strategy" : "mmap",
                "load_strategy_param": {
                    "lock":false
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
}

void SegmentLockSizeCalculatorTest::CaseTearDown() {}

void SegmentLockSizeCalculatorTest::TestSimpleProcess()
{
    DirectoryPtr indexDir = mRootDir->MakeDirectory("index");
    DirectoryPtr indexNameDir = indexDir->MakeDirectory("title");
    indexNameDir->Store("dictionary", "0123");

    SegmentLockSizeCalculator calculator(mSchema, plugin::PluginManagerPtr());
    ASSERT_EQ((size_t)4, calculator.CalculateSize(mRootDir));
}

void SegmentLockSizeCalculatorTest::TestSimpleProcessWithSubSegment()
{
    string field = "sub_title:string;sub_cat:uint32:true;sub_dog:uint32;sub_mice:uint32;sub_abs:uint32";
    string index = "sub_pk:primarykey64:sub_title;sub_title:string:sub_title";
    string attr = "sub_cat;sub_dog;sub_mice;";
    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(field, index, attr, "");
    mSchema->SetSubIndexPartitionSchema(subSchema);

    for (segmentid_t segmentId : {0, 1}) {
        DirectoryPtr segmentDir = mRootDir->MakeDirectory("segment_" + std::to_string(segmentId) + "_level_0");
        segmentDir->MakeDirectory("index/title")->Store("dictionary", "0123");
        segmentDir->MakeDirectory("sub_segment/index/sub_title")->Store("dictionary", "456");
    }

    SegmentLockSizeCalculator calculator1(mSchema, plugin::PluginManagerPtr(), false);
    SegmentLockSizeCalculator calculator2(mSchema, plugin::PluginManagerPtr(), true);
    SegmentLockSizeCalculator subCalculator1(mSchema->GetSubIndexPartitionSchema(), plugin::PluginManagerPtr(), false);
    SegmentLockSizeCalculator subCalculator2(mSchema->GetSubIndexPartitionSchema(), plugin::PluginManagerPtr(), true);
    for (segmentid_t segmentId : {0, 1}) {
        DirectoryPtr segmentDir = mRootDir->GetDirectory("segment_" + std::to_string(segmentId) + "_level_0", true);
        EXPECT_EQ((size_t)4, calculator1.CalculateSize(segmentDir)) << segmentDir->GetLogicalPath();
        EXPECT_EQ((size_t)4, calculator2.CalculateSize(segmentDir)) << segmentDir->GetLogicalPath();

        DirectoryPtr subSegmentDir = segmentDir->GetDirectory("sub_segment", true);
        EXPECT_EQ((size_t)3, subCalculator1.CalculateSize(subSegmentDir)) << subSegmentDir->GetLogicalPath();
        EXPECT_EQ((size_t)3, subCalculator2.CalculateSize(subSegmentDir)) << subSegmentDir->GetLogicalPath();
    }
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
        IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attr, summary);
        SegmentLockSizeCalculator calculator(schema, plugin::PluginManagerPtr());
        ASSERT_EQ((size_t)265, calculator.CalculateSize(mRootDir));
    }

    // test pack attributes
    {
        DirectoryPtr attrDir = mRootDir->GetDirectory(ATTRIBUTE_DIR_NAME, true);
        DirectoryPtr packDir = attrDir->MakeDirectory("pack_attr");
        IndexPartitionSchemaPtr schema = SchemaAdapter::TEST_LoadSchema(
            GET_PRIVATE_TEST_DATA_PATH() + "pack_attribute/schema_for_segment_lock_size_calculator.json");

        // this schema has no inverted index.
        SegmentLockSizeCalculator calculator(schema, plugin::PluginManagerPtr());
        ASSERT_EQ((size_t)4, calculator.CalculateSize(mRootDir));

        MakeOneByteFile(packDir, ATTRIBUTE_DATA_FILE_NAME);
        MakeOneByteFile(packDir, ATTRIBUTE_OFFSET_FILE_NAME);

        ASSERT_EQ((size_t)6, calculator.CalculateSize(mRootDir));
    }
}

void SegmentLockSizeCalculatorTest::InnerTestCalculateRangeIndexSize(const string& loadConfigStr, size_t expectSize)
{
    autil::EnvUtil::setEnv("RS_BLOCK_CACHE", "cache_size=1");
    PrepareData(mRootDir);
    string field = "title:text;cat:uint32:true;abs:uint32;ts:int64";
    string index = "pk:primarykey64:title;range:range:ts;";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "", "");
    SegmentLockSizeCalculator calculator(schema, plugin::PluginManagerPtr());
    ASSERT_EQ((size_t)514, calculator.CalculateSize(mRootDir)); // 256 * 2 + 2

    config::LoadConfigList loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(loadConfigStr);
    RESET_FILE_SYSTEM(loadConfigList);
    const IFileSystemPtr& fs = GET_FILE_SYSTEM();
    string localDir = "local";
    mLocalDirectory = IDirectory::ToLegacyDirectory(make_shared<LocalDirectory>(localDir, fs));
    PrepareData(mLocalDirectory);
    ASSERT_EQ((size_t)expectSize, calculator.CalculateSize(mLocalDirectory));
    autil::EnvUtil::unsetEnv("RS_BLOCK_CACHE");
}

void SegmentLockSizeCalculatorTest::TestCalculateRangeIndexSizeBlockCache()
{
    InnerTestCalculateRangeIndexSize(mCacheLoadConfigForRange, 4); // 512 / 256 + 2 = 4
}

void SegmentLockSizeCalculatorTest::TestCalculateRangeIndexSizeBlockCache_2()
{
    autil::EnvUtil::setEnv("INDEXLIB_DISABLE_SLICE_MEM_LOCK", "true");
    InnerTestCalculateRangeIndexSize(mCacheLoadConfigForRange, 4); // 512 / 256 + 2 = 4
    autil::EnvUtil::unsetEnv("INDEXLIB_DISABLE_SLICE_MEM_LOCK");
}

void SegmentLockSizeCalculatorTest::TestCalculateRangeIndexSizeMMapNoLock()
{
    InnerTestCalculateRangeIndexSize(mMMapNoLockLoadConfigForRange, 4); // use slice file
}

void SegmentLockSizeCalculatorTest::TestCalculateRangeIndexSizeMMapNoLock_2()
{
    autil::EnvUtil::setEnv("INDEXLIB_DISABLE_SLICE_MEM_LOCK", "true");
    InnerTestCalculateRangeIndexSize(mMMapNoLockLoadConfigForRange, 2); // disable use slice file
    autil::EnvUtil::unsetEnv("INDEXLIB_DISABLE_SLICE_MEM_LOCK");
}

void SegmentLockSizeCalculatorTest::TestCalculateRangeIndexSizeMMapLock()
{
    InnerTestCalculateRangeIndexSize(mMMapLockLoadConfigForRange, 514);
}

void SegmentLockSizeCalculatorTest::TestCalculateRangeIndexSizeMMapLock_2()
{
    autil::EnvUtil::setEnv("INDEXLIB_DISABLE_SLICE_MEM_LOCK", "true");
    InnerTestCalculateRangeIndexSize(mMMapLockLoadConfigForRange, 514);
    autil::EnvUtil::unsetEnv("INDEXLIB_DISABLE_SLICE_MEM_LOCK");
}

void SegmentLockSizeCalculatorTest::TestCalculateSourceSize()
{
    string field = "title:text;cat:uint32:true;abs:uint32";
    string index = "pk:primarykey64:title;title:pack:title";
    PrepareData(mRootDir);

    string attr = "cat";
    string summary = "cat";
    string source = "title:cat#abs";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attr, summary, "", source);
    auto sourceSchema = schema->GetSourceSchema();
    {
        SegmentLockSizeCalculator calculator(schema, plugin::PluginManagerPtr());
        ASSERT_EQ((size_t)269, calculator.CalculateSize(mRootDir));
    }

    {
        // disable group 0
        sourceSchema->DisableFieldGroup(0);
        SegmentLockSizeCalculator calculator(schema, plugin::PluginManagerPtr());
        ASSERT_EQ((size_t)267, calculator.CalculateSize(mRootDir));
    }

    {
        // disable source
        sourceSchema->DisableAllFields();
        SegmentLockSizeCalculator calculator(schema, plugin::PluginManagerPtr());
        ASSERT_EQ((size_t)263, calculator.CalculateSize(mRootDir));
    }
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
        IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attr, summary);
        SegmentLockSizeCalculator calculator(schema, plugin::PluginManagerPtr());
        ASSERT_EQ((size_t)263, calculator.CalculateSize(mRootDir));
    }

    // test default summary in attribute, but group not.
    {
        string attr = "cat";
        string summary = "cat";
        IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attr, summary);
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
        IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attr, summary);
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
        IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attr, summary);
        SegmentLockSizeCalculator calculator(schema, plugin::PluginManagerPtr());
        ASSERT_EQ((size_t)265, calculator.CalculateSize(mRootDir));

        RegionSchemaPtr regionSchema = schema->GetRegionSchema(DEFAULT_REGIONID);
        summarygroupid_t groupId = regionSchema->CreateSummaryGroup("group");
        regionSchema->AddSummaryConfig("cat", groupId);
        regionSchema->SetNeedStoreSummary();
        ASSERT_EQ((size_t)265, calculator.CalculateSize(mRootDir));
    }
}

void SegmentLockSizeCalculatorTest::InnerTestCalculateSizeWithLoadConfig(const string& loadConfigStr, size_t expectSize)
{
    tearDown();
    setUp();
    PrepareData(mLocalDirectory);
    IndexPartitionOptions options;
    OnlineConfig& onlineConfig = options.GetOnlineConfig();
    LoadConfigList& loadConfigList = onlineConfig.loadConfigList;
    loadConfigList.Clear();
    LoadConfig loadConfig;
    FromJsonString(loadConfig, loadConfigStr);
    loadConfigList.PushBack(loadConfig);
    std::string workDir = GET_TEMP_DATA_PATH() + mLocalDirectory->GetLogicalPath();
    auto fsOptions = FileSystemFactory::CreateFileSystemOptions(
        workDir, options, util::MemoryQuotaControllerCreator::CreatePartitionMemoryController(),
        file_system::FileBlockCacheContainerPtr(), "");
    IFileSystemPtr fileSystem =
        FileSystemCreator::Create("TestCalculateSizeWithDiffType", workDir, fsOptions).GetOrThrow();
    // TODO;
    ASSERT_EQ(FSEC_OK, fileSystem->MountDir(workDir, "", "local", FSMT_READ_WRITE, true));
    DirectoryPtr segDirectory =
        IDirectory::ToLegacyDirectory(make_shared<LocalDirectory>(mLocalDirectory->GetLogicalPath(), fileSystem));
    SegmentLockSizeCalculator calculator(mSchema, plugin::PluginManagerPtr());
    ASSERT_EQ((size_t)expectSize, calculator.CalculateSize(segDirectory)) << loadConfigStr;
}

void SegmentLockSizeCalculatorTest::TestCalculateSizeWithDiffType()
{
    {
        // test cache load config
        string jsonStr = "\
        {                                                        \
            \"file_patterns\" : [\".*\"],                        \
            \"load_strategy\" : \"cache\",                       \
            \"load_strategy_param\" : {                          \
            \"cache_size\" : 512                                 \
            }                                                    \
        }";
        // dictionary(1) + bitmap(2)
        InnerTestCalculateSizeWithLoadConfig(jsonStr, 3);
    }
    {
        // test mmap no lock
        string jsonStr = "\
        {                                                        \
            \"file_patterns\" : [\".*\"],                        \
            \"load_strategy\" : \"mmap\",                        \
            \"load_strategy_param\" : {                          \
            \"lock\" : false                                     \
            }                                                    \
        }";
        // dictionary(1)
        InnerTestCalculateSizeWithLoadConfig(jsonStr, 1);
    }
    {
        // test mmap lock
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

void SegmentLockSizeCalculatorTest::TestCalculateChangeSize()
{
    string loadConfigStr = R"(
    {
        "load_config" :
        [
            {
                "file_patterns" : ["/index/"],
                "load_strategy" : "mmap",
                "lifecycle": "HOT",
                "load_strategy_param" : {
                     "lock" : true
                }
            },
            {
                "file_patterns" : ["/index/"],
                "load_strategy" : "cache",
                "lifecycle": "COLD",
                "load_strategy_param" : {
                     "global_cache" : false
                }
            },
            {
                "file_patterns" : ["/attribute/"],
                "load_strategy" : "mmap",
                "lifecycle": "HOT",
                "load_strategy_param" : {
                     "lock" : true
                }
            },
            {
                "file_patterns" : ["/attribute/"],
                "load_strategy" : "mmap",
                "lifecycle": "WARM",
                "load_strategy_param" : {
                     "lock" : true
                }
            },
            {
                "file_patterns" : ["/index/"],
                "load_strategy" : "cache",
                "lifecycle": "WARM",
                "load_strategy_param" : {
                     "global_cache" : false
                }
            },
            {
                "file_patterns" : ["/attribtue/"],
                "load_strategy" : "cache",
                "lifecycle": "COLD",
                "load_strategy_param" : {
                     "global_cache" : false
                }
            }
        ]
    })";

    IndexPartitionOptions options;
    OnlineConfig& onlineConfig = options.GetOnlineConfig();
    LoadConfigList& loadConfigList = onlineConfig.loadConfigList;
    loadConfigList.Clear();
    loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(loadConfigStr);
    std::string workDir = GET_TEMP_DATA_PATH() + mLocalDirectory->GetLogicalPath();
    auto fsOptions = FileSystemFactory::CreateFileSystemOptions(
        workDir, options, util::MemoryQuotaControllerCreator::CreatePartitionMemoryController(),
        file_system::FileBlockCacheContainerPtr(), "");
    IFileSystemPtr fileSystem = FileSystemCreator::Create("TestCalculateSizeChange", workDir, fsOptions).GetOrThrow();
    ASSERT_EQ(FSEC_OK, fileSystem->MountDir(workDir, "", "local", FSMT_READ_WRITE, true));
    DirectoryPtr segDirectory =
        IDirectory::ToLegacyDirectory(make_shared<LocalDirectory>(mLocalDirectory->GetLogicalPath(), fileSystem));
    PrepareData(segDirectory);
    SegmentLockSizeCalculator calculator(mSchema, plugin::PluginManagerPtr());
    string hot = "HOT", warm = "WARM", cold = "COLD";
    ASSERT_EQ((size_t)1, calculator.CalculateDiffSize(segDirectory, warm, hot));
    ASSERT_EQ((size_t)4, calculator.CalculateDiffSize(segDirectory, cold, warm));
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

void SegmentLockSizeCalculatorTest::TestCalculateSizeForKVTable()
{
    string jsonStr = R"(
    {
        "load_config" :
        [{ "file_patterns" : ["_KV_KEY_"], "load_strategy" : "mmap", "load_strategy_param":{"lock":true} },
         { "file_patterns" : ["_KV_VALUE_"], "load_strategy" : "mmap", "load_strategy_param":{"lock":true} }]
    })";

    InnerTestCalculateSizeForKVTable(jsonStr, 2);

    jsonStr = R"(
    {
        "load_config" :
        [{ "file_patterns" : ["_KV_KEY_"], "load_strategy" : "mmap", "load_strategy_param":{"lock":true} },
         { "file_patterns" : ["_KV_VALUE_"], "load_strategy" : "cache",
               "load_strategy_param" : { "global_cache" : false, "cache_decompress_file" : true }}]
    })";

    InnerTestCalculateSizeForKVTable(jsonStr, 1);
}

void SegmentLockSizeCalculatorTest::TestCalculateSizeForKKVTable()
{
    string jsonStr = R"(
    {
        "load_config" :
        [{ "file_patterns" : ["_KKV_PKEY_"], "load_strategy" : "mmap", "load_strategy_param":{"lock":true} },
         { "file_patterns" : ["_KKV_SKEY_"], "load_strategy" : "mmap", "load_strategy_param":{"lock":true} },
         { "file_patterns" : ["_KKV_VALUE_"], "load_strategy" : "mmap"}]
    })";

    InnerTestCalculateSizeForKKVTable(jsonStr, 2);

    jsonStr = R"(
    {
        "load_config" :
        [{ "file_patterns" : ["_KKV_PKEY_"], "load_strategy" : "mmap", "load_strategy_param":{"lock":true} },
         { "file_patterns" : ["_KKV_SKEY_"], "load_strategy" : "cache",
               "load_strategy_param" : { "global_cache" : false, "cache_decompress_file" : true }},
         { "file_patterns" : ["_KKV_VALUE_"], "load_strategy" : "mmap"}]
    })";

    InnerTestCalculateSizeForKKVTable(jsonStr, 1);
}

void SegmentLockSizeCalculatorTest::MakeMultiByteFile(const DirectoryPtr& dir, const string& fileName, size_t count)
{
    string data(count, '0');
    dir->Store(fileName, data);
}

void SegmentLockSizeCalculatorTest::MakeOneByteFile(const DirectoryPtr& dir, const string& fileName)
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
    // pk size not calculate
    DirectoryPtr pkIndexDir = indexDir->MakeDirectory("pk");
    MakeOneByteFile(pkIndexDir, PRIMARY_KEY_DATA_FILE_NAME);
    DirectoryPtr pkAttrDir = pkIndexDir->MakeDirectory(string(PK_ATTRIBUTE_DIR_NAME_PREFIX) + "_title");
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
    MakeOneByteFile(catAttrDir, string("0_1.") + PATCH_FILE_NAME);

    DirectoryPtr summaryDir = dir->MakeDirectory(SUMMARY_DIR_NAME);
    MakeOneByteFile(summaryDir, SUMMARY_DATA_FILE_NAME);
    MakeOneByteFile(summaryDir, SUMMARY_OFFSET_FILE_NAME);

    DirectoryPtr summaryGroupDir = summaryDir->MakeDirectory("group");
    MakeOneByteFile(summaryGroupDir, SUMMARY_DATA_FILE_NAME);
    MakeOneByteFile(summaryGroupDir, SUMMARY_OFFSET_FILE_NAME);

    // source
    DirectoryPtr sourceDir = dir->MakeDirectory(SOURCE_DIR_NAME);
    DirectoryPtr metaDir = sourceDir->MakeDirectory(SOURCE_META_DIR);
    MakeOneByteFile(metaDir, SOURCE_DATA_FILE_NAME);
    MakeOneByteFile(metaDir, SOURCE_OFFSET_FILE_NAME);
    // 2 group
    DirectoryPtr groupDir = sourceDir->MakeDirectory(SourceDefine::GetDataDir(0));
    MakeOneByteFile(groupDir, SOURCE_DATA_FILE_NAME);
    MakeOneByteFile(groupDir, SOURCE_OFFSET_FILE_NAME);
    DirectoryPtr groupDir1 = sourceDir->MakeDirectory(SourceDefine::GetDataDir(1));
    MakeOneByteFile(groupDir1, SOURCE_DATA_FILE_NAME);
    MakeOneByteFile(groupDir1, SOURCE_OFFSET_FILE_NAME);
}

void SegmentLockSizeCalculatorTest::InnerTestCalculateSizeForKVTable(const string& loadConfigStr, size_t expectSize)
{
    tearDown();
    setUp();

    string fields = "value1:int64;value2:int64;key:uint64";
    mSchema = SchemaMaker::MakeKVSchema(fields, "key", "value1;value2");

    DirectoryPtr segDir = mLocalDirectory->MakeDirectory("segment_0_level_0");
    DirectoryPtr indexDir = segDir->MakeDirectory(INDEX_DIR_NAME);
    DirectoryPtr sIndexDir0 = indexDir->MakeDirectory("key");
    MakeOneByteFile(sIndexDir0, KV_KEY_FILE_NAME);
    MakeOneByteFile(sIndexDir0, KV_VALUE_FILE_NAME);

    IndexPartitionOptions options;
    options.GetOnlineConfig().loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(loadConfigStr);
    std::string workDir = GET_TEMP_DATA_PATH() + mLocalDirectory->GetLogicalPath();
    auto fsOptions = FileSystemFactory::CreateFileSystemOptions(
        workDir, options, util::MemoryQuotaControllerCreator::CreatePartitionMemoryController(),
        file_system::FileBlockCacheContainerPtr(), "");
    IFileSystemPtr fileSystem =
        FileSystemCreator::Create("TestCalculateSizeForKVTable", workDir, fsOptions).GetOrThrow();
    ASSERT_EQ(FSEC_OK, fileSystem->MountDir(workDir, "segment_0_level_0", "segment_0_level_0", FSMT_READ_WRITE, true));
    DirectoryPtr rootDir = IDirectory::ToLegacyDirectory(make_shared<LocalDirectory>("", fileSystem));
    DirectoryPtr segDirectory = rootDir->GetDirectory("segment_0_level_0", false);

    SegmentLockSizeCalculator calculator(mSchema, plugin::PluginManagerPtr());
    ASSERT_EQ((size_t)expectSize, calculator.CalculateSize(segDirectory));
}

void SegmentLockSizeCalculatorTest::InnerTestCalculateSizeForKKVTable(const string& loadConfigStr, size_t expectSize)
{
    tearDown();
    setUp();

    string fields = "value1:int64;value2:int64;pkey:uint64;skey:uint64";
    mSchema = SchemaMaker::MakeKKVSchema(fields, "pkey", "skey", "value1;value2");

    DirectoryPtr segDir = mLocalDirectory->MakeDirectory("segment_0_level_0");
    DirectoryPtr indexDir = segDir->MakeDirectory(INDEX_DIR_NAME);
    DirectoryPtr sIndexDir0 = indexDir->MakeDirectory("pkey");
    MakeOneByteFile(sIndexDir0, PREFIX_KEY_FILE_NAME);
    MakeOneByteFile(sIndexDir0, SUFFIX_KEY_FILE_NAME);
    MakeOneByteFile(sIndexDir0, KKV_VALUE_FILE_NAME);

    IndexPartitionOptions options;
    options.GetOnlineConfig().loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(loadConfigStr);
    std::string workDir = GET_TEMP_DATA_PATH() + mLocalDirectory->GetLogicalPath();
    auto fsOptions = FileSystemFactory::CreateFileSystemOptions(
        workDir, options, util::MemoryQuotaControllerCreator::CreatePartitionMemoryController(),
        file_system::FileBlockCacheContainerPtr(), "");
    IFileSystemPtr fileSystem =
        FileSystemCreator::Create("TestCalculateSizeForKKVTable", workDir, fsOptions).GetOrThrow();
    ASSERT_EQ(FSEC_OK, fileSystem->MountDir(workDir, "segment_0_level_0", "segment_0_level_0", FSMT_READ_WRITE, true));
    DirectoryPtr rootDir = IDirectory::ToLegacyDirectory(make_shared<LocalDirectory>("", fileSystem));
    DirectoryPtr segDirectory = rootDir->GetDirectory("segment_0_level_0", false);
    SegmentLockSizeCalculator calculator(mSchema, plugin::PluginManagerPtr());
    ASSERT_EQ((size_t)expectSize, calculator.CalculateSize(segDirectory));
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
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attr, summary);
    SchemaModifyOperationPtr modifyOp(new SchemaModifyOperation);
    {
        // 263bytes
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
}} // namespace indexlib::index
