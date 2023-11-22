#include "indexlib/partition/remote_access/test/partition_resource_provider_intetest.h"

#include <cmath>
#include <type_traits>

#include "indexlib/config/field_type_traits.h"
#include "indexlib/config/module_info.h"
#include "indexlib/file_system/EntryTable.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/file/FileBuffer.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/index/calculator/on_disk_segment_size_calculator.h"
#include "indexlib/index/calculator/partition_size_calculator.h"
#include "indexlib/index/normal/attribute/accessor/attribute_data_iterator.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/patch/partition_patch_index_accessor.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_roll_back_util.h"
#include "indexlib/partition/offline_partition.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/remote_access/attribute_data_seeker.h"
#include "indexlib/partition/remote_access/attribute_patch_data_writer.h"
#include "indexlib/partition/remote_access/partition_resource_provider_factory.h"
#include "indexlib/test/directory_creator.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/unittest.h"
#include "indexlib/testlib/indexlib_partition_creator.h"
#include "indexlib/util/EpochIdUtil.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::config;
using namespace indexlib::test;
using namespace indexlib::testlib;
using namespace indexlib::util;
using namespace indexlib::file_system;
using namespace indexlib::file_system;
using namespace indexlib::index;
using namespace indexlib::plugin;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, PartitionResourceProviderInteTest);

INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(PartitionResourceProviderInteTestMode, PartitionResourceProviderInteTest,
                                     testing::Values(false, true));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionResourceProviderInteTestMode, TestPartitionSeeker);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionResourceProviderInteTestMode, TestPartitionPatcher);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionResourceProviderInteTestMode, TestReadAndMergeWithPartitionPatchIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionResourceProviderInteTestMode, TestAutoDeletePartitionPatchMeta);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionResourceProviderInteTestMode, TestValidateDeployIndexForPatchIndexData);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionResourceProviderInteTestMode, TestOnlineCleanOnDiskIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionResourceProviderInteTestMode, TestPatchDataLoadConfig);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionResourceProviderInteTestMode, TestPartitionSizeCalculatorForPatch);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionResourceProviderInteTestMode, TestOnDiskSegmentSizeCalculatorForPatch);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionResourceProviderInteTestMode, TestPatchMetaFileExist);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionResourceProviderInteTestMode, TestGetSegmentSizeByDeployIndexWrapper);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionResourceProviderInteTestMode, TestPartitionPatchIndexAccessor);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionResourceProviderInteTestMode, TestDeployIndexWrapperGetDeployFiles);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionResourceProviderInteTestMode, TestCreateRollBackVersion);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionResourceProviderInteTestMode, TestBuildIndexWithSchemaIdNotZero);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionResourceProviderInteTestMode, TestAsyncWriteAttribute);

PartitionResourceProviderInteTest::PartitionResourceProviderInteTest() {}

PartitionResourceProviderInteTest::~PartitionResourceProviderInteTest() {}

void PartitionResourceProviderInteTest::CaseSetUp()
{
    {
        // create index table
        string field = "name:string:false:false:uniq;price:uint32;"
                       "category:int32:true:true;tags:string:true;payload:float:true:true:fp16:4";
        string index = "pk:primarykey64:name";
        string attributes = "name;price;category;tags;payload";
        IndexPartitionSchemaPtr schema =
            IndexlibPartitionCreator::CreateSchema("index_table", field, index, attributes, "");
        IndexPartitionOptions options;
        options.GetBuildConfig(false).maxDocCount = 2;
        string docs = "cmd=add,name=doc1,price=10,category=1 2,payload=1 2 3 4,tags=test1 test,ts=0;"
                      "cmd=add,name=doc2,price=20,category=2 3,payload=2 3 4 5,tags=test2 test,ts=0;"
                      "cmd=add,name=doc3,price=30,category=3 4,payload=3 4 5 6,tags=test3 test,ts=0;"
                      "cmd=add,name=doc4,price=40,category=4 5,payload=4 5 6 7,tags=test4 test,ts=0;"
                      "cmd=update_field,name=doc1,price=11,ts=0;"
                      "cmd=update_field,name=doc2,category=2 2,ts=0;";
        IndexlibPartitionCreator::CreateIndexPartition(schema, GET_TEMP_DATA_PATH() + "/index_table", docs, options, "",
                                                       false);
    }

    {
        // create kv table
        string field = "name:string;price:uint32;category:int32:true;tags:string:true;"
                       "feature:float:true:false:block_fp:4";
        string attributes = "name;price;category;tags;feature";
        IndexPartitionSchemaPtr schema =
            IndexlibPartitionCreator::CreateKVSchema("kv_table", field, "name", attributes);

        IndexPartitionOptions options;
        string docs = "cmd=add,name=doc5,price=50,category=5 2,tags=test5 test,feature=1.1 1.2,ts=0;"
                      "cmd=add,name=doc6,price=60,category=6 3,tags=test6 test,feature=1.2 1.3 1.4,ts=0;"
                      "cmd=add,name=doc7,price=70,category=7 4,tags=test7 test,feature=1.3 1.4 -1.5 1.6,ts=0;"
                      "cmd=add,name=doc8,price=80,category=8 5,tags=test8 test,feature=1.4 -1.5 -1.6 1.7 1.8,ts=0;";
        IndexlibPartitionCreator::CreateIndexPartition(schema, GET_TEMP_DATA_PATH() + "/kv_table", docs, options);
    }

    // create new schema
    string field =
        "name:string;price:uint32;category:int32:true:true;tags:string:true;payload:float:true:true:fp16:4;"
        "new_value:uint32;field0:text;field1:text;time:uint64;coordinate:location;feature:float:true:true:int8#127:2;";
    string index = "";
    bool shardingIndex = GET_CASE_PARAM();
    if (shardingIndex) {
        index = "pk:primarykey64:name;numberIndex:number:new_value;"
                "packIndex:expack:field0,field1:false:3;priceIndex:range:price;"
                "timeIndex:date:time;spatial_index:spatial:coordinate";
    } else {
        index = "pk:primarykey64:name;numberIndex:number:new_value;"
                "packIndex:expack:field0,field1;priceIndex:range:price;"
                "timeIndex:date:time;spatial_index:spatial:coordinate";
    }
    string attributes = "name;price;category;tags;payload;new_value;feature";
    mNewSchema = IndexlibPartitionCreator::CreateSchema("index_table", field, index, attributes, "");
    mNewSchema->SetSchemaVersionId(1);
}

void PartitionResourceProviderInteTest::CaseTearDown() {}

void PartitionResourceProviderInteTest::TestBuildIndexWithSchemaIdNotZero()
{
    // create index table
    string field = "name:string:false:false:uniq;price:uint32;"
                   "category:int32:true:true;tags:string:true";
    string index = "pk:primarykey64:name";
    string attributes = "name;price;category;tags";
    IndexPartitionSchemaPtr schema =
        IndexlibPartitionCreator::CreateSchema("index_table", field, index, attributes, "");
    schema->SetSchemaVersionId(1);
    IndexPartitionOptions options;
    options.GetBuildConfig(false).maxDocCount = 2;
    string docs = "cmd=add,name=doc1,price=10,category=1 2,tags=test1 test,ts=0;"
                  "cmd=add,name=doc2,price=20,category=2 3,tags=test2 test,ts=0;"
                  "cmd=add,name=doc3,price=30,category=3 4,tags=test3 test,ts=0;"
                  "cmd=add,name=doc4,price=40,category=4 5,tags=test4 test,ts=0;"
                  "cmd=update_field,name=doc1,price=11,ts=0;"
                  "cmd=update_field,name=doc2,category=2 2,ts=0;";
    IndexlibPartitionCreator::CreateIndexPartition(schema, GET_TEMP_DATA_PATH() + "/schema_1_table", docs, options, "",
                                                   false);
    mNewSchema->SetSchemaVersionId(2);
    PreparePatchIndex("schema_1_table");
}

void PartitionResourceProviderInteTest::TestPartitionIterator()
{
    IndexPartitionOptions options;
    PartitionResourceProviderPtr resourceProvider = PartitionResourceProviderFactory::GetInstance()->CreateProvider(
        GET_TEMP_DATA_PATH() + "/index_table", options, "");

    ASSERT_EQ((versionid_t)0, resourceProvider->GetVersion().GetVersionId());

    PartitionIteratorPtr iterator = resourceProvider->CreatePartitionIterator();
    CheckIterator(iterator, 0, "price", "11;20");
    CheckIterator(iterator, 1, "price", "30;40");

    CheckIterator(iterator, 0, "name", "doc1;doc2");
    CheckIterator(iterator, 1, "name", "doc3;doc4");

    CheckIterator(iterator, 0, "category", "12;22");
    CheckIterator(iterator, 1, "category", "34;45");

    CheckIterator(iterator, 0, "tags", "test1test;test2test");
    CheckIterator(iterator, 1, "tags", "test3test;test4test");

    CheckIterator(iterator, 0, "payload", "1234;2345");
    CheckIterator(iterator, 1, "payload", "3456;4567");
}

void PartitionResourceProviderInteTest::TestPartitionSeeker()
{
    {
        IndexPartitionOptions options;
        options.GetOfflineConfig().readerConfig.loadIndex = false;
        PartitionResourceProviderPtr resourceProvider = PartitionResourceProviderFactory::GetInstance()->CreateProvider(
            GET_TEMP_DATA_PATH() + "/index_table", options, "");
        PartitionSeekerPtr seeker = resourceProvider->CreatePartitionSeeker();
        CheckSeeker(seeker, "doc1", "price=11");
    }
}

void PartitionResourceProviderInteTest::TestPartitionPatcher()
{
    // create index table
    string field = "name:string;price:uint32;category:int32:true:true;tags:string:true;"
                   "new_value1:uint32;new_value2:uint64:true;"
                   "score1:float:false:false:fp16;score2:float:false:false:int8#1;";
    string index = "pk:primarykey64:name;numberIndex:number:new_value1";
    string attributes = "name;price;category;tags;new_value1;new_value2;"
                        "score1;score2;";
    IndexPartitionSchemaPtr newSchema =
        IndexlibPartitionCreator::CreateSchema("index_table", field, index, attributes, "");
    newSchema->SetSchemaVersionId(1);

    string rootPath = GET_TEMP_DATA_PATH() + "/index_table";
    IndexPartitionOptions options;
    PartitionResourceProviderPtr resourceProvider =
        PartitionResourceProviderFactory::GetInstance()->CreateProvider(rootPath, options, "");

    string patchDir1 = GET_TEMP_DATA_PATH() + "/index_table/patch_index_1";
    string patchDir2 = GET_TEMP_DATA_PATH() + "/index_table/patch_index_2";
    FslibWrapper::MkDirE(patchDir1, true, true);
    FslibWrapper::MkDirE(patchDir2, true, true);
    file_system::FileSystemOptions fsOptions;
    fsOptions.isOffline = true;
    string branchName;
    auto patchDirectory1 = BranchFS::GetDirectoryFromDefaultBranch(patchDir1, fsOptions, nullptr, &branchName);
    auto patchDirectory2 = BranchFS::GetDirectoryFromDefaultBranch(patchDir2, fsOptions, nullptr, &branchName);
    {
        PartitionPatcherPtr patcher = resourceProvider->CreatePartitionPatcher(newSchema, patchDirectory1);
        AttributeConfigPtr attrConf = patcher->GetAlterAttributeConfig("score1");
        ASSERT_EQ(FieldType::ft_fp16, attrConf->GetFieldType());
        attrConf = patcher->GetAlterAttributeConfig("score2");
        ASSERT_EQ(FieldType::ft_fp8, attrConf->GetFieldType());
        AttributeDataPatcherPtr attr1Patcher = patcher->CreateSingleAttributePatcher("new_value1", 0);
        AttributeDataPatcherPtr attr2Patcher = patcher->CreateSingleAttributePatcher("new_value2", 1);
        AttributeDataPatcherPtr attr3Patcher = patcher->CreateSingleAttributePatcher("score1", 1);
        AttributeDataPatcherPtr attr4Patcher = patcher->CreateSingleAttributePatcher("score2", 1);

        IndexDataPatcherPtr indexPatcher = patcher->CreateSingleIndexPatcher("numberIndex", 0);
        attr1Patcher->Close();
        attr2Patcher->Close();
        attr3Patcher->Close();
        attr4Patcher->Close();
        indexPatcher->Close();
        ASSERT_TRUE(FslibWrapper::IsExist(patchDir1 + "/segment_0_level_0/attribute/new_value1").GetOrThrow());
        ASSERT_TRUE(FslibWrapper::IsExist(patchDir1 + "/segment_0_level_0/attribute/new_value1/data").GetOrThrow());
        ASSERT_TRUE(FslibWrapper::IsExist(patchDir1 + "/segment_1_level_0/attribute/new_value2").GetOrThrow());
        ASSERT_TRUE(FslibWrapper::IsExist(patchDir1 + "/segment_1_level_0/attribute/new_value2/data").GetOrThrow());
        ASSERT_TRUE(FslibWrapper::IsExist(patchDir1 + "/segment_1_level_0/attribute/new_value2/offset").GetOrThrow());
        ASSERT_TRUE(FslibWrapper::IsExist(patchDir1 + "/segment_1_level_0/attribute/score1").GetOrThrow());
        ASSERT_TRUE(FslibWrapper::IsExist(patchDir1 + "/segment_1_level_0/attribute/score1/data").GetOrThrow());
        ASSERT_TRUE(FslibWrapper::IsExist(patchDir1 + "/segment_1_level_0/attribute/score2").GetOrThrow());
        ASSERT_TRUE(FslibWrapper::IsExist(patchDir1 + "/segment_1_level_0/attribute/score2/data").GetOrThrow());
        ASSERT_TRUE(FslibWrapper::IsExist(patchDir1 + "/segment_0_level_0/index/numberIndex").GetOrThrow());
    }

    resourceProvider->StoreVersion(newSchema, 1);
    RESET_FILE_SYSTEM();
    auto rootDir = GET_PARTITION_DIRECTORY()->GetDirectory("index_table", false);
    ASSERT_NE(rootDir, nullptr);
    CheckPatchMeta(rootDir, 1, "1:0,1");
    newSchema->SetSchemaVersionId(2);
    {
        // new attribute use equal & uniq compress
        auto attrConfig = newSchema->GetAttributeSchema()->GetAttributeConfig("new_value1");
        ASSERT_TRUE(attrConfig);
        ASSERT_TRUE(attrConfig->SetCompressType("equal").IsOK());
        attrConfig = newSchema->GetAttributeSchema()->GetAttributeConfig("new_value2");
        ASSERT_TRUE(attrConfig);
        ASSERT_TRUE(attrConfig->SetCompressType("equal|uniq").IsOK());

        PartitionPatcherPtr patcher = resourceProvider->CreatePartitionPatcher(newSchema, patchDirectory2);
        AttributeDataPatcherPtr attr1Patcher = patcher->CreateSingleAttributePatcher("new_value1", 0);
        AttributeDataPatcherPtr attr2Patcher = patcher->CreateSingleAttributePatcher("new_value2", 1);
        IndexDataPatcherPtr indexPatcher = patcher->CreateSingleIndexPatcher("numberIndex", 0);
        attr1Patcher->Close();
        attr2Patcher->Close();
        indexPatcher->Close();
        ASSERT_TRUE(FslibWrapper::IsExist(patchDir2 + "/segment_0_level_0/attribute/new_value1").GetOrThrow());
        ASSERT_TRUE(FslibWrapper::IsExist(patchDir2 + "/segment_0_level_0/attribute/new_value1/data").GetOrThrow());
        ASSERT_TRUE(FslibWrapper::IsExist(patchDir2 + "/segment_1_level_0/attribute/new_value2").GetOrThrow());
        ASSERT_TRUE(FslibWrapper::IsExist(patchDir2 + "/segment_1_level_0/attribute/new_value2/data").GetOrThrow());
        ASSERT_TRUE(FslibWrapper::IsExist(patchDir2 + "/segment_1_level_0/attribute/new_value2/offset").GetOrThrow());
        ASSERT_TRUE(FslibWrapper::IsExist(patchDir2 + "/segment_0_level_0/index/numberIndex").GetOrThrow());
    }
    resourceProvider->StoreVersion(newSchema, 2);
    RESET_FILE_SYSTEM();
    rootDir = GET_PARTITION_DIRECTORY()->GetDirectory("index_table", false);
    CheckPatchMeta(rootDir, 2, "2:0,1;1:1");
    ASSERT_NE(FslibWrapper::GetFileLength(patchDir1 + "/segment_0_level_0/attribute/new_value1/data").GetOrThrow(),
              FslibWrapper::GetFileLength(patchDir2 + "/segment_0_level_0/attribute/new_value1/data").GetOrThrow());

    ASSERT_NE(FslibWrapper::GetFileLength(patchDir1 + "/segment_1_level_0/attribute/new_value2/data").GetOrThrow(),
              FslibWrapper::GetFileLength(patchDir2 + "/segment_1_level_0/attribute/new_value2/data").GetOrThrow());

    ASSERT_NE(FslibWrapper::GetFileLength(patchDir1 + "/segment_1_level_0/attribute/new_value2/offset").GetOrThrow(),
              FslibWrapper::GetFileLength(patchDir2 + "/segment_1_level_0/attribute/new_value2/offset").GetOrThrow());
}

void PartitionResourceProviderInteTest::CheckIterator(const PartitionIteratorPtr& iterator, segmentid_t segmentId,
                                                      const string& attrName, const string& attrValues)
{
    ASSERT_TRUE(iterator);
    AttributeDataIteratorPtr attrIter = iterator->CreateSingleAttributeIterator(attrName, segmentId);
    ASSERT_TRUE(attrIter);

    vector<string> valueStrs;
    StringUtil::fromString(attrValues, valueStrs, ";");

    size_t count = 0;
    while (attrIter->IsValid()) {
        ASSERT_EQ(valueStrs[count], attrIter->GetValueStr());
        count++;
        attrIter->MoveToNext();
    }
    ASSERT_EQ(count, valueStrs.size());
}

void PartitionResourceProviderInteTest::CheckSeeker(const PartitionSeekerPtr& seeker, const string& key,
                                                    const string& fieldValues)
{
    ASSERT_TRUE(seeker);

    vector<vector<string>> fieldInfoVec;
    StringUtil::fromString(fieldValues, fieldInfoVec, "=", ";");

    Pool pool;
    for (size_t i = 0; i < fieldInfoVec.size(); i++) {
        assert(fieldInfoVec[i].size() == 2);
        const string& fieldName = fieldInfoVec[i][0];
        const string& fieldValue = fieldInfoVec[i][1];
        AttributeDataSeeker* attrSeeker = seeker->GetSingleAttributeSeeker(fieldName);

        AttributeConfigPtr attrConf = attrSeeker->GetAttrConfig();
        CompressTypeOption compressType = attrConf->GetCompressType();
        FieldType ft = attrConf->GetFieldType();
        bool isMultiValue = attrConf->IsMultiValue();
        bool isFixedLen = attrConf->IsLengthFixed();

        autil::StringView keyStr(key);
        switch (ft) {
#define MACRO(field_type)                                                                                              \
    case field_type: {                                                                                                 \
        if (!isMultiValue) {                                                                                           \
            typedef typename FieldTypeTraits<field_type>::AttrItemType T;                                              \
            T value;                                                                                                   \
            AttributeDataSeekerTyped<T>* typedSeeker = dynamic_cast<AttributeDataSeekerTyped<T>*>(attrSeeker);         \
            assert(typedSeeker);                                                                                       \
            ASSERT_TRUE(typedSeeker->SeekByString(keyStr, value));                                                     \
            ASSERT_EQ(StringUtil::fromString<T>(fieldValue), value);                                                   \
        } else if (!isFixedLen || !seeker->EnableCountedMultiValue()) {                                                \
            typedef typename FieldTypeTraits<field_type>::AttrItemType T;                                              \
            typedef typename autil::MultiValueType<T> MT;                                                              \
            MT value;                                                                                                  \
            AttributeDataSeekerTyped<MT>* typedSeeker = dynamic_cast<AttributeDataSeekerTyped<MT>*>(attrSeeker);       \
            assert(typedSeeker);                                                                                       \
            ASSERT_TRUE(typedSeeker->SeekByString(keyStr, value));                                                     \
            vector<T> values;                                                                                          \
            StringUtil::fromString(fieldValue, values, ",");                                                           \
            ASSERT_EQ(value.size(), (uint32_t)values.size());                                                          \
            for (size_t i = 0; i < values.size(); i++) {                                                               \
                using T2 = std::conditional<std::is_unsigned<T>::value, int64_t, T>::type;                             \
                if (compressType.HasBlockFpEncodeCompress()) {                                                         \
                    ASSERT_TRUE(std::abs((T2)(values[i]) - (T2)(value[i])) <= 0.0001);                                 \
                } else if (compressType.HasFp16EncodeCompress()) {                                                     \
                    ASSERT_TRUE(std::abs((T2)(values[i]) - (T2)(value[i])) <= 0.01);                                   \
                } else {                                                                                               \
                    ASSERT_EQ(values[i], value[i]);                                                                    \
                }                                                                                                      \
            }                                                                                                          \
        } else {                                                                                                       \
            typedef typename FieldTypeTraits<field_type>::AttrItemType T;                                              \
            typedef typename autil::MultiValueType<T> MT;                                                              \
            MT value;                                                                                                  \
            AttributeDataSeekerTyped<MT>* typedSeeker = dynamic_cast<AttributeDataSeekerTyped<MT>*>(attrSeeker);       \
            assert(typedSeeker);                                                                                       \
            ASSERT_TRUE(typedSeeker->SeekByString(keyStr, value));                                                     \
            vector<T> values;                                                                                          \
            StringUtil::fromString(fieldValue, values, ",");                                                           \
            ASSERT_EQ(value.size(), (uint32_t)values.size());                                                          \
            for (size_t i = 0; i < values.size(); i++) {                                                               \
                using T2 = std::conditional<std::is_unsigned<T>::value, int64_t, T>::type;                             \
                if (compressType.HasBlockFpEncodeCompress()) {                                                         \
                    ASSERT_TRUE(std::abs((T2)(values[i]) - (T2)(value[i])) <= 0.0001);                                 \
                } else if (compressType.HasFp16EncodeCompress()) {                                                     \
                    ASSERT_TRUE(std::abs((T2)(values[i]) - (T2)(value[i])) <= 0.01);                                   \
                } else {                                                                                               \
                    ASSERT_EQ(values[i], value[i]);                                                                    \
                }                                                                                                      \
            }                                                                                                          \
        }                                                                                                              \
        break;                                                                                                         \
    }

            NUMBER_FIELD_MACRO_HELPER(MACRO);

        case ft_string:
            if (!isMultiValue) {
                MultiChar value;
                AttributeDataSeekerTyped<MultiChar>* typedSeeker =
                    dynamic_cast<AttributeDataSeekerTyped<MultiChar>*>(attrSeeker);
                assert(typedSeeker);
                ASSERT_TRUE(typedSeeker->SeekByString(keyStr, value));
                ASSERT_EQ(fieldValue, string(value.data(), value.size()));
            } else if (!isFixedLen || !seeker->EnableCountedMultiValue()) {
                MultiString value;
                AttributeDataSeekerTyped<MultiString>* typedSeeker =
                    dynamic_cast<AttributeDataSeekerTyped<MultiString>*>(attrSeeker);
                assert(typedSeeker);
                ASSERT_TRUE(typedSeeker->SeekByString(keyStr, value));
                vector<string> values;
                StringUtil::fromString(fieldValue, values, ",");
                ASSERT_EQ(value.size(), (uint32_t)values.size());
                for (size_t i = 0; i < values.size(); i++) {
                    string valueStr(value[i].data(), value[i].size());
                    ASSERT_EQ(valueStr, values[i]);
                }
            } else {
                MultiString value;
                AttributeDataSeekerTyped<MultiString>* typedSeeker =
                    dynamic_cast<AttributeDataSeekerTyped<MultiString>*>(attrSeeker);
                assert(typedSeeker);
                ASSERT_TRUE(typedSeeker->SeekByString(keyStr, value));
                vector<string> values;
                StringUtil::fromString(fieldValue, values, ",");
                ASSERT_EQ(value.size(), (uint32_t)values.size());
                for (size_t i = 0; i < values.size(); i++) {
                    string valueStr(value[i].data(), value[i].size());
                    ASSERT_EQ(valueStr, values[i]);
                }
            }
            break;
        default:
            assert(false);
        }
    }
}

void PartitionResourceProviderInteTest::TestReadAndMergeWithPartitionPatchIndex()
{
    PreparePatchIndex();

    string rootDir = GET_TEMP_DATA_PATH() + "/index_table";
    IndexPartitionOptions options;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mNewSchema, options, rootDir));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:doc1", "docid=0,new_value=0,feature=0 0"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:doc2", "docid=1,new_value=1,feature=1 0"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:doc3", "docid=2,new_value=2,feature=2 0"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:doc4", "docid=3,new_value=3,feature=3 0"));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "numberIndex:0", "docid=0,new_value=0,feature=0 0"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "numberIndex:1", "docid=1,new_value=1,feature=1 0"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "numberIndex:2", "docid=2,new_value=2,feature=2 0"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "numberIndex:3", "docid=3,new_value=3,feature=3 0"));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "packIndex:0", "docid=0"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "packIndex:1", "docid=0;docid=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "packIndex:2", "docid=1;docid=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "packIndex:3", "docid=2;docid=3"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "packIndex:4", "docid=3"));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "priceIndex:(0, 200]", "docid=1;docid=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "timeIndex:[1516086000000, 1516088000000]", "docid=0;docid=1;docid=2"));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "spatial_index:point(116.3906 39.92324)", "docid=0;docid=1;docid=2;docid=3"));

    string docs = "cmd=add,name=doc5,price=50,category=5 2,new_value=5,feature=5 5,field0=5,field1=6,tags=test5 "
                  "test,coordinate=116.3906 39.92324,time=1516086500000,ts=10;"
                  "cmd=add,name=doc6,price=60,category=6 3,new_value=6,feature=6 6,field0=6,field1=7,tags=test6 "
                  "test,coordinate=116.3906 39.92324,time=1516088500000,ts=10;"
                  "cmd=update_field,name=doc1,new_value=7,ts=10;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, docs, "pk:doc1", "docid=0,new_value=7"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:doc5", "docid=4,new_value=5,feature=5 5"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:doc6", "docid=5,new_value=6,feature=6 6"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "numberIndex:5", "docid=4,new_value=5,feature=5 5"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "numberIndex:6", "docid=5,new_value=6,feature=6 6"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "packIndex:5", "docid=4"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "packIndex:6", "docid=4;docid=5"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "packIndex:7", "docid=5"));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "priceIndex:(0, 200]", "docid=1;docid=2;docid=4;docid=5"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "timeIndex:[1516086000000, 1516088000000]", "docid=0;docid=1;docid=2;docid=4"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "spatial_index:point(116.3906 39.92324)",
                             "docid=0;docid=1;docid=2;docid=3;docid=4;docid=5"));

    RESET_FILE_SYSTEM();
    auto rootDirectory = GET_PARTITION_DIRECTORY()->GetDirectory("index_table", false);
    ASSERT_NE(rootDirectory, nullptr);
    CheckPatchMeta(rootDirectory, 2, "1:0,1");

    // optimize merge
    ASSERT_TRUE(psm.Transfer(BUILD_INC, "", "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:doc1", "docid=0,new_value=7,feature=0 0"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:doc2", "docid=1,new_value=1,feature=1 0"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:doc3", "docid=2,new_value=2,feature=2 0"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:doc4", "docid=3,new_value=3,feature=3 0"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:doc5", "docid=4,new_value=5,feature=5 5"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:doc6", "docid=5,new_value=6,feature=6 6"));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "numberIndex:0", "docid=0,new_value=7"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "numberIndex:1", "docid=1,new_value=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "numberIndex:2", "docid=2,new_value=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "numberIndex:3", "docid=3,new_value=3"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "numberIndex:5", "docid=4,new_value=5"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "numberIndex:6", "docid=5,new_value=6"));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "packIndex:0", "docid=0"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "packIndex:1", "docid=0;docid=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "packIndex:2", "docid=1;docid=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "packIndex:3", "docid=2;docid=3"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "packIndex:4", "docid=3"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "packIndex:5", "docid=4"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "packIndex:6", "docid=4;docid=5"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "packIndex:7", "docid=5"));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "priceIndex:(0, 200]", "docid=1;docid=2;docid=4;docid=5"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "timeIndex:[1516086000000, 1516088000000]", "docid=0;docid=1;docid=2;docid=4"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "spatial_index:point(116.3906 39.92324)",
                             "docid=0;docid=1;docid=2;docid=3;docid=4;docid=5"));
    psm.GetIndexPartition()->Close();
    RESET_FILE_SYSTEM();
    rootDirectory = GET_PARTITION_DIRECTORY()->GetDirectory("index_table", false);
    CheckPatchMeta(rootDirectory, 3, "");
}

void PartitionResourceProviderInteTest::TestAutoDeletePartitionPatchMeta()
{
    // version 1: segment 0, 1, 2
    PreparePatchIndex();
    string rootDir = GET_TEMP_DATA_PATH() + "/index_table/";
    ASSERT_TRUE(FslibWrapper::IsExist(rootDir + "patch_index_1").GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(rootDir + "patch_index_1/segment_0_level_0").GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(rootDir + "patch_index_1/segment_1_level_0").GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(rootDir + "schema.json").GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(rootDir + "schema.json.1").GetOrThrow());

    IndexPartitionOptions options;
    options.GetBuildConfig(true).keepVersionCount = 1;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mNewSchema, options, rootDir));
    string docs = "cmd=add,name=doc5,price=50,category=5 2,new_value=5,tags=test5 test,ts=10;"
                  "cmd=add,name=doc6,price=60,category=6 3,new_value=6,tags=test6 test,ts=10;"
                  "cmd=update_field,name=doc1,new_value=7,ts=10;";

    // version 2: segment 3,4
    ASSERT_TRUE(psm.Transfer(PE_BUILD_INC, docs, "", ""));
    {
        // only online partiton writer will clean version
        auto indexBuilder = psm.CreateIndexBuilder(PE_BUILD_INC);
        auto indexPartitionWriter = indexBuilder->mIndexPartitionWriter;
        indexPartitionWriter->mOptions.SetIsOnline(true);
        indexPartitionWriter->Close();
    }
    auto rootDirectory = GET_CHECK_DIRECTORY()->GetDirectory("index_table", false);
    ASSERT_NE(rootDirectory, nullptr);
    CheckPatchMeta(rootDirectory, 2, "1:0,1");

    // ASSERT_FALSE(FslibWrapper::IsExist(rootDir + "schema.json").GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(rootDir + "schema.json.1").GetOrThrow());
    // ASSERT_FALSE(FslibWrapper::IsExist(rootDir + "version.1").GetOrThrow());
    // ASSERT_FALSE(FslibWrapper::IsExist(rootDir + "deploy_meta.1").GetOrThrow());
    // ASSERT_FALSE(FslibWrapper::IsExist(rootDir + "patch_meta.1").GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(rootDir + "version.2").GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(rootDir + "deploy_meta.2").GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(rootDir + "patch_meta.2").GetOrThrow());

    MergeConfig specificMerge;
    specificMerge.mergeStrategyStr = "specific_segments";
    specificMerge.mergeStrategyParameter.SetLegacyString("merge_segments=1,2,3");
    psm.SetMergeConfig(specificMerge);

    // version 3: segment 0, 4, 5, remove segment 1 in root & patch_index
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, "", "", ""));
    {
        // only online partiton writer will clean version
        auto indexBuilder = psm.CreateIndexBuilder(PE_BUILD_INC);
        auto indexPartitionWriter = indexBuilder->mIndexPartitionWriter;
        indexPartitionWriter->mOptions.SetIsOnline(true);
        indexPartitionWriter->Close();
    }
    ASSERT_TRUE(FslibWrapper::IsExist(rootDir + "version.3").GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(rootDir + "deploy_meta.3").GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(rootDir + "patch_meta.3").GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(rootDir + "patch_index_1/segment_0_level_0").GetOrThrow());
    // ASSERT_FALSE(FslibWrapper::IsExist(rootDir + "patch_index_1/segment_1_level_0").GetOrThrow());
    rootDirectory = GET_CHECK_DIRECTORY()->GetDirectory("index_table", false);
    ASSERT_NE(rootDirectory, nullptr);
    CheckPatchMeta(rootDirectory, 3, "1:0");

    // version 4: segment 6, remove patch_index_1
    MergeConfig optMergeConf;
    psm.SetMergeConfig(optMergeConf);
    string emptyEntryTableStr = R"(
    {
        "files":{},
        "dirs":[],
        "package_files":{}
    })";
    std::string emptyEntryTable = PathUtil::JoinPath(rootDir, EntryTable::GetEntryTableFileName(2));
    auto ec = FslibWrapper::AtomicStore(emptyEntryTable, emptyEntryTableStr.data(), emptyEntryTableStr.size(), true,
                                        FenceContext::NoFence())
                  .Code();
    ASSERT_EQ(ec, file_system::ErrorCode::FSEC_OK);

    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, "", "", ""));
    // meaningless, differ from online env. For no logicalFS in patch_index_*
    // ASSERT_FALSE(FslibWrapper::IsExist(rootDir + "patch_index_1").GetOrThrow());
    rootDirectory = GET_CHECK_DIRECTORY()->GetDirectory("index_table", false);
    ASSERT_NE(rootDirectory, nullptr);
    CheckPatchMeta(rootDirectory, 4, "");
}

void PartitionResourceProviderInteTest::TestValidateDeployIndexForPatchIndexData()
{
    PreparePatchIndex();

    string rootDir = GET_TEMP_DATA_PATH() + "/index_table";
    file_system::DeployIndexMeta dpMeta;
    ASSERT_TRUE(dpMeta.Load(rootDir + "/deploy_meta.1").OK());

    vector<string> patchIndexPathVec;
    for (const auto& meta : dpMeta.deployFileMetas) {
        if (meta.filePath.find(PATCH_INDEX_DIR_PREFIX) != string::npos && meta.fileLength != (int64_t)-1) {
            patchIndexPathVec.push_back(meta.filePath);
        }
    }
    ASSERT_TRUE(!patchIndexPathVec.empty());

    {
        // normal read
        IndexPartitionOptions options;
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(mNewSchema, options, rootDir));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:doc1", "docid=0,new_value=0"));
    }

    {
        // open fail
        string toRemoveFilePath = util::PathUtil::JoinPath(rootDir, patchIndexPathVec[0]);
        FslibWrapper::DeleteFileE(toRemoveFilePath, DeleteOption::NoFence(true));
        IndexPartitionOptions options;
        PartitionStateMachine psm;
        psm.Init(mNewSchema, options, rootDir);
        ASSERT_FALSE(psm.Transfer(BUILD_INC_NO_MERGE, "", "pk:doc1", "docid=0,new_value=0,ts=100"));
    }
}

void PartitionResourceProviderInteTest::TestOnlineCleanOnDiskIndex()
{
    // version 1: segment 0, 1, 2
    PreparePatchIndex();
    string rootDir = GET_TEMP_DATA_PATH() + "/index_table/";
    ASSERT_TRUE(FslibWrapper::IsExist(rootDir + "schema.json").GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(rootDir + "schema.json.1").GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(rootDir + "patch_index_1").GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(rootDir + "patch_index_1/segment_0_level_0").GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(rootDir + "patch_index_1/segment_1_level_0").GetOrThrow());

    IndexPartitionOptions options;
    options.GetBuildConfig(false).keepVersionCount = 10;
    options.GetOnlineConfig().onlineKeepVersionCount = 1;

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mNewSchema, options, rootDir));
    string docs = "cmd=add,name=doc5,price=50,category=5 2,new_value=5,tags=test5 test,ts=10;"
                  "cmd=add,name=doc6,price=60,category=6 3,new_value=6,tags=test6 test,ts=10;"
                  "cmd=update_field,name=doc1,new_value=7,ts=10;";

    MergeConfig specificMerge;
    specificMerge.mergeStrategyStr = "specific_segments";
    specificMerge.mergeStrategyParameter.SetLegacyString("merge_segments=1,2,3");
    psm.SetMergeConfig(specificMerge);
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, docs, "", ""));

    // version 3: segment 0, 4, 5, remove segment 1 in root & patch_index
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, "", "", ""));
    ASSERT_TRUE(FslibWrapper::IsExist(rootDir + "version.3").GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(rootDir + "deploy_meta.3").GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(rootDir + "patch_meta.3").GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(rootDir + "patch_index_1/segment_0_level_0").GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(rootDir + "patch_index_1/segment_1_level_0").GetOrThrow());

    // create online partition
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:doc1", "docid=0,new_value=7"));

    while (FslibWrapper::IsExist(rootDir + "schema.json").GetOrThrow())
        ;
    ASSERT_FALSE(FslibWrapper::IsExist(rootDir + "schema.json").GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(rootDir + "schema.json.1").GetOrThrow());
    ASSERT_FALSE(FslibWrapper::IsExist(rootDir + "patch_index_1/segment_1_level_0").GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(rootDir + "patch_index_1/segment_0_level_0").GetOrThrow());
    ASSERT_FALSE(FslibWrapper::IsExist(rootDir + "deploy_meta.2").GetOrThrow());
    ASSERT_FALSE(FslibWrapper::IsExist(rootDir + "deploy_meta.1").GetOrThrow());

    // version 4: segment 6, online remove useless patch_index data
    MergeConfig optMerger;
    psm.SetMergeConfig(optMerger);
    ASSERT_TRUE(psm.Transfer(BUILD_INC, "", "pk:doc1", "docid=0,new_value=7"));

    while (FslibWrapper::IsExist(rootDir + "patch_index_1/segment_0_level_0").GetOrThrow())
        ;
    ASSERT_FALSE(FslibWrapper::IsExist(rootDir + "patch_index_1/segment_0_level_0").GetOrThrow());
    ASSERT_FALSE(FslibWrapper::IsExist(rootDir + "version.3").GetOrThrow());
    ASSERT_FALSE(FslibWrapper::IsExist(rootDir + "patch_meta.3").GetOrThrow());
    ASSERT_FALSE(FslibWrapper::IsExist(rootDir + "deploy_meta.3").GetOrThrow());
}

void PartitionResourceProviderInteTest::TestPatchDataLoadConfig()
{
    uint64_t v0LockSize = GetMMapLockSize();
    PreparePatchIndex();
    uint64_t v1LockSize = GetMMapLockSize();
    ASSERT_LT(v0LockSize, v1LockSize);

    // 4 doc * uint32_t field
    // float feature:  4 doc * (int8_t * 2) + sizeof(uint32_t)) * 2 * (2 + 1)
    uint64_t expectSize = sizeof(uint32_t) * 4 + sizeof(int8_t) * 2 * 4 + // fixlen = 2, float_encode = int8
                          sizeof(uint32_t) * 2 * (2 + 1);                 // offset : 2 segment, each 2 doc + length
    ASSERT_EQ(expectSize, v1LockSize - v0LockSize);
}

void PartitionResourceProviderInteTest::TestPartitionSizeCalculatorForPatch()
{
    OnlinePartitionPtr part0 = CreateOnlinePartition();
    Version version0 = part0->GetPartitionData()->GetOnDiskVersion();
    PartitionDataPtr partitionData = OnDiskPartitionData::CreateOnDiskPartitionData(
        part0->GetRootDirectory()->GetFileSystem(), version0, part0->GetRootDirectory()->GetLogicalPath(),
        part0->GetSchema()->GetSubIndexPartitionSchema() != NULL, false);
    PartitionSizeCalculatorPtr calculator0(
        new PartitionSizeCalculator(part0->GetRootDirectory(), part0->GetSchema(), true, plugin::PluginManagerPtr()));
    size_t v0LockSize = calculator0->CalculateDiffVersionLockSizeWithoutPatch(
        version0, index_base::Version(INVALID_VERSIONID), partitionData);
    PreparePatchIndex();

    OnlinePartitionPtr part1 = CreateOnlinePartition();
    Version version1 = part1->GetPartitionData()->GetOnDiskVersion();
    partitionData = OnDiskPartitionData::CreateOnDiskPartitionData(
        part1->GetRootDirectory()->GetFileSystem(), version1, part1->GetRootDirectory()->GetLogicalPath(),
        part1->GetSchema()->GetSubIndexPartitionSchema() != NULL, false);
    PartitionSizeCalculatorPtr calculator1(
        new PartitionSizeCalculator(part1->GetRootDirectory(), part1->GetSchema(), true, plugin::PluginManagerPtr()));
    size_t v1LockSize = calculator1->CalculateDiffVersionLockSizeWithoutPatch(
        version1, index_base::Version(INVALID_VERSIONID), partitionData);
    ASSERT_LT(v0LockSize, v1LockSize);

    size_t expectSize = calculator1->CalculateDiffVersionLockSizeWithoutPatch(version1, version0, partitionData);
    ASSERT_EQ(expectSize, v1LockSize - v0LockSize);
}

void PartitionResourceProviderInteTest::TestOnDiskSegmentSizeCalculatorForPatch()
{
    OnlinePartitionPtr part0 = CreateOnlinePartition();
    SegmentData segData0 = part0->GetPartitionData()->GetSegmentData(0);
    OnDiskSegmentSizeCalculator cal0;
    int64_t v0SegSize = cal0.GetSegmentSize(segData0, part0->GetSchema());

    PreparePatchIndex();

    OnlinePartitionPtr part1 = CreateOnlinePartition();
    SegmentData segData1 = part1->GetPartitionData()->GetSegmentData(0);
    OnDiskSegmentSizeCalculator cal1;
    int64_t v1SegSize = cal1.GetSegmentSize(segData1, part1->GetSchema());

    ASSERT_LT(v0SegSize, v1SegSize);

    string dpFileName = GET_TEMP_DATA_PATH() + "/index_table/patch_index_1/segment_0_level_0/deploy_index";
    DeployIndexMeta meta;
    ASSERT_TRUE(meta.Load(dpFileName).OK());
    size_t expectSize = 0;
    for (size_t i = 0; i < meta.deployFileMetas.size(); i++) {
        if (meta.deployFileMetas[i].filePath.find("meta") != string::npos ||
            meta.deployFileMetas[i].filePath.find("option") != string::npos ||
            meta.deployFileMetas[i].filePath.find("info") != string::npos) {
            continue;
        }

        if (meta.deployFileMetas[i].fileLength > 0) {
            expectSize += meta.deployFileMetas[i].fileLength;
        }
    }
    ASSERT_EQ(expectSize, v1SegSize - v0SegSize);
}

void PartitionResourceProviderInteTest::TestPatchMetaFileExist()
{
    string rootDir = GET_TEMP_DATA_PATH() + "/index_table/";
    ASSERT_FALSE(FslibWrapper::IsExist(GET_TEMP_DATA_PATH() + "/index_table/patch_meta.0").GetOrThrow());
    string dpFileName = GET_TEMP_DATA_PATH() + "/index_table/deploy_meta.0";
    CheckFileExistInDeployIndexFile(dpFileName, "patch_meta.0", false);

    PreparePatchIndex();
    // version.1, convert index
    dpFileName = GET_TEMP_DATA_PATH() + "/index_table/deploy_meta.1";
    CheckFileExistInDeployIndexFile(dpFileName, "patch_meta.1", true);

    // version.2, build comit version
    IndexPartitionOptions options;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mNewSchema, options, rootDir));
    string docs = "cmd=add,name=doc5,price=50,category=5 2,new_value=5,tags=test5 test,ts=10;"
                  "cmd=add,name=doc6,price=60,category=6 3,new_value=6,tags=test6 test,ts=10;"
                  "cmd=update_field,name=doc1,new_value=7,ts=10;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, docs, "", ""));
    ASSERT_TRUE(FslibWrapper::IsExist(rootDir + "patch_meta.2").GetOrThrow());
    dpFileName = GET_TEMP_DATA_PATH() + "/index_table/deploy_meta.2";
    CheckFileExistInDeployIndexFile(dpFileName, "patch_meta.2", true);

    Version version = psm.GetIndexPartition()->GetPartitionData()->GetOnDiskVersion();
    IndexPartitionSchemaPtr schema = psm.GetIndexPartition()->GetSchema();
    ASSERT_EQ((schemaid_t)1, version.GetSchemaVersionId());
    ASSERT_EQ((schemaid_t)1, schema->GetSchemaVersionId());

    // version.3, merger commit version
    ASSERT_TRUE(psm.Transfer(BUILD_INC, "", "", ""));
    ASSERT_TRUE(FslibWrapper::IsExist(rootDir + "patch_meta.3").GetOrThrow());
    dpFileName = GET_TEMP_DATA_PATH() + "/index_table/deploy_meta.3";
    CheckFileExistInDeployIndexFile(dpFileName, "patch_meta.3", true);
}

void PartitionResourceProviderInteTest::TestGetSegmentSizeByDeployIndexWrapper()
{
    string rootDir = GET_TEMP_DATA_PATH() + "/index_table/";
    int64_t totalLen1 = 0;
    IndexPartitionOptions options;
    {
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(mNewSchema, options, rootDir));
        psm.Transfer(QUERY, "", "pk:doc1", "docid=0");
        PartitionDataPtr partData = psm.GetIndexPartition()->GetPartitionData();
        SegmentData segData = partData->GetSegmentData(0);
        DeployIndexWrapper::TEST_GetSegmentSize(segData, true, totalLen1);
    }

    int64_t totalLen2 = 0;
    PreparePatchIndex();
    {
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(mNewSchema, options, rootDir));
        psm.Transfer(QUERY, "", "pk:doc1", "docid=0");
        PartitionDataPtr partData = psm.GetIndexPartition()->GetPartitionData();
        SegmentData segData = partData->GetSegmentData(0);
        DeployIndexWrapper::TEST_GetSegmentSize(segData, true, totalLen2);
    }
    ASSERT_LT(totalLen1, totalLen2);
}

void PartitionResourceProviderInteTest::TestPartitionPatchIndexAccessor()
{
    PreparePatchIndex();

    string rootDir = GET_TEMP_DATA_PATH() + "/index_table/";
    PartitionStateMachine psm;
    IndexPartitionOptions options;
    ASSERT_TRUE(psm.Init(mNewSchema, options, rootDir));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "pk:doc1", "docid=0"));
    PartitionDataPtr partData = psm.GetIndexPartition()->GetPartitionData();
    SegmentData segData = partData->GetSegmentData(0);

    PartitionPatchIndexAccessorPtr accessor = segData.GetPatchIndexAccessor();
    ASSERT_TRUE(accessor);

    string patchRoot = PartitionPatchIndexAccessor::GetPatchRootDirName(1);
    DirectoryPtr indexDir = accessor->GetIndexDirectory(0, "numberIndex", true);
    ASSERT_TRUE(indexDir->GetLogicalPath().find(patchRoot) != string::npos);
    indexDir = accessor->GetIndexDirectory(1, "numberIndex", true);
    ASSERT_TRUE(indexDir->GetLogicalPath().find(patchRoot) != string::npos);

    DirectoryPtr attrDir = accessor->GetAttributeDirectory(0, "new_value", true);
    ASSERT_TRUE(attrDir->GetLogicalPath().find(patchRoot) != string::npos);
    attrDir = accessor->GetAttributeDirectory(1, "new_value", true);
    ASSERT_TRUE(attrDir->GetLogicalPath().find(patchRoot) != string::npos);

    ASSERT_FALSE(accessor->GetAttributeDirectory(0, "not_exist", true));
    ASSERT_FALSE(accessor->GetIndexDirectory(0, "not_exist", true));
}

void PartitionResourceProviderInteTest::TestDeployIndexWrapperGetDeployFiles()
{
    string rootDir = GET_TEMP_DATA_PATH() + "/index_table/";
    PreparePatchIndex();

    DeployIndexWrapper::GetDeployIndexMetaInputParams params;
    params.rawPath = rootDir;
    params.targetVersionId = 1;
    params.baseVersionId = -1;
    DeployIndexMetaVec localDeployIndexMetaVec;
    DeployIndexMetaVec remoteDeployIndexMetaVec;
    ASSERT_TRUE(DeployIndexWrapper::GetDeployIndexMeta(params, localDeployIndexMetaVec, remoteDeployIndexMetaVec));
    fslib::FileList files;
    for (const auto& fileInfo : localDeployIndexMetaVec[0]->deployFileMetas) {
        files.push_back(fileInfo.filePath);
    }
    CheckFileExist("schema.json", files, false);
    CheckFileExist("schema.json.1", files, true);
    CheckFileExist("patch_meta.1", files, true);
    CheckFileExist("segment_0_level_0/package_file.__meta__", files, true);
    CheckFileExist("segment_0_level_0/package_file.__data__0", files, true);
    CheckFileExist("segment_1_level_0/package_file.__meta__", files, true);
    CheckFileExist("segment_1_level_0/package_file.__data__0", files, true);
    CheckFileExist("patch_index_1/segment_0_level_0/index/numberIndex/dictionary", files, true);
    CheckFileExist("patch_index_1/segment_0_level_0/index/numberIndex/posting", files, true);
    CheckFileExist("patch_index_1/segment_0_level_0/index/numberIndex/index_format_option", files, true);
    CheckFileExist("patch_index_1/segment_1_level_0/index/numberIndex/dictionary", files, true);
    CheckFileExist("patch_index_1/segment_1_level_0/index/numberIndex/posting", files, true);
    CheckFileExist("patch_index_1/segment_1_level_0/index/numberIndex/index_format_option", files, true);

    params.targetVersionId = 1;
    params.baseVersionId = 0;
    ASSERT_TRUE(DeployIndexWrapper::GetDeployIndexMeta(params, localDeployIndexMetaVec, remoteDeployIndexMetaVec));
    files.clear();
    for (const auto& fileInfo : localDeployIndexMetaVec[0]->deployFileMetas) {
        files.push_back(fileInfo.filePath);
    }
    CheckFileExist("schema.json", files, false);
    CheckFileExist("schema.json.1", files, true);
    CheckFileExist("patch_meta.1", files, true);
    CheckFileExist("segment_0_level_0/package_file.__meta__", files, false);
    CheckFileExist("segment_0_level_0/package_file.__data__0", files, false);
    CheckFileExist("segment_1_level_0/package_file.__meta__", files, false);
    CheckFileExist("segment_1_level_0/package_file.__data__0", files, false);
    CheckFileExist("patch_index_1/segment_0_level_0/index/numberIndex/dictionary", files, true);
    CheckFileExist("patch_index_1/segment_0_level_0/index/numberIndex/posting", files, true);
    CheckFileExist("patch_index_1/segment_0_level_0/index/numberIndex/index_format_option", files, true);
    CheckFileExist("patch_index_1/segment_1_level_0/index/numberIndex/dictionary", files, true);
    CheckFileExist("patch_index_1/segment_1_level_0/index/numberIndex/posting", files, true);
    CheckFileExist("patch_index_1/segment_1_level_0/index/numberIndex/index_format_option", files, true);

    params.targetVersionId = 0;
    params.baseVersionId = -1;
    ASSERT_TRUE(DeployIndexWrapper::GetDeployIndexMeta(params, localDeployIndexMetaVec, remoteDeployIndexMetaVec));
    files.clear();
    for (const auto& fileInfo : localDeployIndexMetaVec[0]->deployFileMetas) {
        files.push_back(fileInfo.filePath);
    }
    CheckFileExist("schema.json", files, true);
    CheckFileExist("schema.json.1", files, false);
    CheckFileExist("patch_meta.1", files, false);
    CheckFileExist("segment_0_level_0/package_file.__meta__", files, true);
    CheckFileExist("segment_0_level_0/package_file.__data__0", files, true);
    CheckFileExist("segment_1_level_0/package_file.__meta__", files, true);
    CheckFileExist("segment_1_level_0/package_file.__data__0", files, true);
    CheckFileExist("patch_index_1/segment_0_level_0/index/numberIndex/dictionary", files, false);
    CheckFileExist("patch_index_1/segment_0_level_0/index/numberIndex/posting", files, false);
    CheckFileExist("patch_index_1/segment_0_level_0/index/numberIndex/index_format_option", files, false);
    CheckFileExist("patch_index_1/segment_1_level_0/index/numberIndex/dictionary", files, false);
    CheckFileExist("patch_index_1/segment_1_level_0/index/numberIndex/posting", files, false);
    CheckFileExist("patch_index_1/segment_1_level_0/index/numberIndex/index_format_option", files, false);
}

void PartitionResourceProviderInteTest::TestCreateRollBackVersion()
{
    PreparePatchIndex();
    DirectoryPtr rootDir = GET_CHECK_DIRECTORY()->GetDirectory("index_table", false);
    string rootPath = rootDir->GetOutputPath();

    CheckVersion(rootDir, 0, "0,1,2", 2);
    CheckVersion(rootDir, 1, "0,1,2", 2);

    // version.2, build comit version
    IndexPartitionOptions options;
    options.GetBuildConfig(false).keepVersionCount = 10;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mNewSchema, options, rootPath));
    string docs = "cmd=add,name=doc5,price=50,category=5 2,new_value=5,tags=test5 test,ts=10;"
                  "cmd=add,name=doc6,price=60,category=6 3,new_value=6,tags=test6 test,ts=10;"
                  "cmd=update_field,name=doc1,new_value=7,ts=10;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, docs, "", ""));
    CheckVersion(rootDir, 2, "0,1,2,3", 3);

    ASSERT_FALSE(IndexRollBackUtil::CreateRollBackVersion(rootPath, 0, 3, "0"));
    ASSERT_TRUE(IndexRollBackUtil::CreateRollBackVersion(rootPath, 0, 3, EpochIdUtil::TEST_GenerateEpochId()));
    CheckVersion(rootDir, 3, "0,1,2", 3);
    CheckFileExistInDeployIndexFile(rootPath + "/deploy_meta.3", "schema.json", true);
    CheckFileExistInDeployIndexFile(rootPath + "/deploy_meta.3", "patch_meta.3", false);
    ASSERT_FALSE(rootDir->IsExist("patch_meta.3"));
    ASSERT_TRUE(rootDir->IsExist("entry_table.3"));

    ASSERT_TRUE(IndexRollBackUtil::CreateRollBackVersion(rootPath, 2, 4, EpochIdUtil::TEST_GenerateEpochId()));
    CheckVersion(rootDir, 4, "0,1,2,3", 3);
    CheckFileExistInDeployIndexFile(rootPath + "/deploy_meta.4", "schema.json.1", true);
    CheckFileExistInDeployIndexFile(rootPath + "/deploy_meta.4", "patch_meta.4", true);
    ASSERT_TRUE(rootDir->IsExist("patch_meta.4"));
    ASSERT_TRUE(rootDir->IsExist("entry_table.4"));

    ASSERT_TRUE(IndexRollBackUtil::CreateRollBackVersion(rootPath, 1, 5, EpochIdUtil::TEST_GenerateEpochId()));
    CheckVersion(rootDir, 5, "0,1,2", 3);
    CheckFileExistInDeployIndexFile(rootPath + "/deploy_meta.5", "schema.json.1", true);
    CheckFileExistInDeployIndexFile(rootPath + "/deploy_meta.5", "patch_meta.5", true);
    ASSERT_TRUE(rootDir->IsExist("patch_meta.5"));
    ASSERT_TRUE(rootDir->IsExist("patch_meta.5"));
}

void PartitionResourceProviderInteTest::PreparePatchIndex(const string& tableDirName)
{
    file_system::FileSystemOptions fsOptions;
    fsOptions.isOffline = true;
    // prepare patch index dir
    string rootPath = PathUtil::JoinPath(GET_TEMP_DATA_PATH(), tableDirName);
    uint32_t schemaId = mNewSchema->GetSchemaVersionId();

    IndexPartitionOptions options;
    PartitionResourceProviderPtr resourceProvider = PartitionResourceProviderFactory::GetInstance()->CreateProvider(
        GET_TEMP_DATA_PATH() + "/" + tableDirName, options, "");
    string branchRoot = resourceProvider->GetPartition()->GetRootDirectory()->GetOutputPath();
    string patchDir = PathUtil::JoinPath(branchRoot, PartitionPatchIndexAccessor::GetPatchRootDirName(schemaId));
    FslibWrapper::MkDirE(patchDir, true, true);
    string branchName;
    auto patchDirectory = BranchFS::GetDirectoryFromDefaultBranch(patchDir, fsOptions, nullptr, &branchName);
    PartitionPatcherPtr patcher = resourceProvider->CreatePartitionPatcher(mNewSchema, patchDirectory);
    PartitionSegmentIteratorPtr segIterator = resourceProvider->CreatePartitionSegmentIterator();
    uint32_t value = 0;
    uint32_t featureValue = 0;
    uint32_t indexValue = 0;
    uint32_t packIndexValue = 0;
    uint32_t priceValue = 0;
    uint64_t timeValue = 1516086000000;
    while (segIterator->IsValid()) {
        SegmentData segData = segIterator->GetSegmentData();
        // attribute
        AttributeDataPatcherPtr attrPatcher =
            patcher->CreateSingleAttributePatcher("new_value", segData.GetSegmentId());
        if (attrPatcher) {
            for (uint32_t i = 0; i < attrPatcher->GetTotalDocCount(); i++) {
                attrPatcher->AppendFieldValue(StringUtil::toString(value++));
            }
            attrPatcher->Close();
        }

        attrPatcher = patcher->CreateSingleAttributePatcher("feature", segData.GetSegmentId());
        if (attrPatcher) {
            for (uint32_t i = 0; i < attrPatcher->GetTotalDocCount(); i++) {
                attrPatcher->AppendFieldValue(StringUtil::toString(featureValue++));
            }
            attrPatcher->Close();
        }

        // number index
        fieldid_t fieldId = mNewSchema->GetFieldConfig("new_value")->GetFieldId();
        IndexDataPatcherPtr indexPatcher = patcher->CreateSingleIndexPatcher("numberIndex", segData.GetSegmentId());
        if (indexPatcher) {
            for (uint32_t i = 0; i < indexPatcher->GetTotalDocCount(); i++) {
                string valueStr = StringUtil::toString(indexValue++);
                indexPatcher->AddField(valueStr, fieldId);
                indexPatcher->EndDocument();
            }
            if (indexPatcher->GetTotalDocCount() != 0) {
                ASSERT_EQ(indexPatcher->GetPatchDocCount(), indexPatcher->GetDistinctTermCount());
            }
            indexPatcher->Close();
        }

        // expack index
        fieldid_t fieldId0 = mNewSchema->GetFieldConfig("field0")->GetFieldId();
        fieldid_t fieldId1 = mNewSchema->GetFieldConfig("field1")->GetFieldId();
        indexPatcher = patcher->CreateSingleIndexPatcher("packIndex", segData.GetSegmentId());
        if (indexPatcher) {
            for (uint32_t i = 0; i < indexPatcher->GetTotalDocCount(); i++) {
                string valueStr0 = StringUtil::toString(packIndexValue++);
                string valueStr1 = StringUtil::toString(packIndexValue);
                indexPatcher->AddField(valueStr0, fieldId0);
                indexPatcher->AddField(valueStr1, fieldId1);
                indexPatcher->EndDocument();
            }
            if (indexPatcher->GetTotalDocCount() != 0) {
                ASSERT_EQ(indexPatcher->GetPatchDocCount() + 1, indexPatcher->GetDistinctTermCount());
            }
            indexPatcher->Close();
        }

        // time index
        fieldId = mNewSchema->GetFieldConfig("time")->GetFieldId();
        indexPatcher = patcher->CreateSingleIndexPatcher("timeIndex", segData.GetSegmentId());
        if (indexPatcher) {
            for (uint32_t i = 0; i < indexPatcher->GetTotalDocCount(); i++) {
                string valueStr = StringUtil::toString(timeValue);
                timeValue += 1000000;
                indexPatcher->AddField(valueStr, fieldId);
                indexPatcher->EndDocument();
            }
            indexPatcher->Close();
        }

        // range index
        fieldId = mNewSchema->GetFieldConfig("price")->GetFieldId();
        indexPatcher = patcher->CreateSingleIndexPatcher("priceIndex", segData.GetSegmentId());
        if (indexPatcher) {
            for (uint32_t i = 0; i < indexPatcher->GetTotalDocCount(); i++) {
                string valueStr = StringUtil::toString(priceValue * 100);
                ++priceValue;
                indexPatcher->AddField(valueStr, fieldId);
                indexPatcher->EndDocument();
            }
            indexPatcher->Close();
        }

        // spatial index
        fieldId = mNewSchema->GetFieldConfig("coordinate")->GetFieldId();
        indexPatcher = patcher->CreateSingleIndexPatcher("spatial_index", segData.GetSegmentId());
        attrPatcher = patcher->CreateSingleAttributePatcher("coordinate", segData.GetSegmentId());
        if (indexPatcher) {
            assert(indexPatcher->GetTotalDocCount() == attrPatcher->GetTotalDocCount());
            string values = string("116.3906 39.92324");
            StringView valueStr(values);
            for (uint32_t i = 0; i < indexPatcher->GetTotalDocCount(); i++) {
                indexPatcher->AddField(values, fieldId);
                indexPatcher->EndDocument();
                attrPatcher->AppendFieldValue(valueStr);
            }
            indexPatcher->Close();
            attrPatcher->Close();
        }
        segIterator->MoveToNext();
    }
    resourceProvider->StoreVersion(mNewSchema, 1);
    ASSERT_TRUE(FslibWrapper::IsExist(branchRoot + "/version.1").GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(branchRoot + "/deploy_meta.1").GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(branchRoot + "/" + Version::GetSchemaFileName(schemaId)).GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(branchRoot + "/patch_meta.1").GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(patchDir + "/segment_0_level_0/index/packIndex_section").GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(patchDir + "/segment_0_level_0/index/timeIndex").GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(patchDir + "/segment_0_level_0/index/priceIndex").GetOrThrow());

    resourceProvider->GetPartition()->GetBranchFileSystem()->TEST_MoveToMainRoad();
    string patchMetaStr = StringUtil::toString(schemaId) + ":0,1";
    RESET_FILE_SYSTEM();
    auto rootDir = GET_PARTITION_DIRECTORY()->GetDirectory(tableDirName, false);
    ASSERT_NE(rootDir, nullptr);
    CheckPatchMeta(rootDir, 1, patchMetaStr);
}

// metaInfoStr: schemaId:seg0,seg1;...
void PartitionResourceProviderInteTest::CheckPatchMeta(const file_system::DirectoryPtr& rootDir, versionid_t versionId,
                                                       const string& metaInfoStr)
{
    vector<vector<string>> metaInfoVec;
    StringUtil::fromString(metaInfoStr, metaInfoVec, ":", ";");

    PartitionPatchMeta patchMeta;
    patchMeta.Load(rootDir, versionId);

    size_t schemaIdCount = 0;
    PartitionPatchMeta::Iterator metaIter = patchMeta.CreateIterator();
    while (metaIter.HasNext()) {
        ASSERT_TRUE(schemaIdCount < metaInfoVec.size());
        assert(metaInfoVec[schemaIdCount].size() == 2);
        schemaid_t expectSchemaId = StringUtil::fromString<schemaid_t>(metaInfoVec[schemaIdCount][0]);

        vector<segmentid_t> expectSegIds;
        StringUtil::fromString(metaInfoVec[schemaIdCount][1], expectSegIds, ",");

        SchemaPatchInfoPtr schemaInfo = metaIter.Next();
        assert(schemaInfo);
        ASSERT_EQ(expectSchemaId, schemaInfo->GetSchemaId());

        size_t segmentIdCount = 0;
        SchemaPatchInfo::Iterator sIter = schemaInfo->Begin();
        for (; sIter != schemaInfo->End(); sIter++) {
            const SchemaPatchInfo::PatchSegmentMeta& segMeta = *sIter;
            ASSERT_TRUE(find(expectSegIds.begin(), expectSegIds.end(), segMeta.GetSegmentId()) != expectSegIds.end())
                << segMeta.GetSegmentId();
            segmentIdCount++;
        }
        ASSERT_EQ(expectSegIds.size(), segmentIdCount);
        schemaIdCount++;
    }
    ASSERT_EQ(metaInfoVec.size(), schemaIdCount);
}

size_t PartitionResourceProviderInteTest::GetMMapLockSize()
{
    string rootDir = GET_TEMP_DATA_PATH() + "/index_table/";

    IndexPartitionOptions options;
    string mmapStr = R"(
    {
        "load_config" : [
        {
            "file_patterns" : [".*/coordinate/.*"],
            "load_strategy" : "mmap",
            "load_strategy_param" : {
        "lock" : false
            }
    },
        {
            "file_patterns" : ["_ATTRIBUTE_"],
            "load_strategy" : "mmap",
            "load_strategy_param" : {
        "lock" : true,
        "slice" : 4096,
        "interval" : 2,
        "advise_random" : true
            }
    }
        ]
    })";
    options.GetOnlineConfig().loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(mmapStr);
    PartitionStateMachine psm;
    psm.Init(mNewSchema, options, rootDir);
    psm.Transfer(QUERY, "", "pk:doc1", "docid=0");

    OnlinePartitionPtr onlinePart = DYNAMIC_POINTER_CAST(OnlinePartition, psm.GetIndexPartition());
    assert(onlinePart);
    IFileSystemPtr fileSystem = onlinePart->GetFileSystem();
    assert(fileSystem);
    StorageMetrics storageMetrics = fileSystem->GetFileSystemMetrics().GetInputStorageMetrics();
    return storageMetrics.GetMmapLockFileLength(FSMG_LOCAL);
}

OnlinePartitionPtr PartitionResourceProviderInteTest::CreateOnlinePartition()
{
    string rootDir = GET_TEMP_DATA_PATH() + "/index_table/";

    IndexPartitionOptions options;
    string mmapStr = R"(
    {
        "load_config" : [
        {
            "file_patterns" : ["_ATTRIBUTE_", "_INDEX_"],
            "load_strategy" : "mmap",
            "load_strategy_param" : {
        "lock" : true,
        "slice" : 4096,
        "interval" : 2,
        "advise_random" : true
            }
    }
        ]
    })";
    options.GetOnlineConfig().loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(mmapStr);
    OnlinePartitionPtr part(new OnlinePartition);
    part->Open(rootDir, "", mNewSchema, options);
    return part;
}

void PartitionResourceProviderInteTest::CheckFileExistInDeployIndexFile(const std::string& dpMetaFilePath,
                                                                        const string& filePath, bool isExist)
{
    file_system::DeployIndexMeta meta;
    ASSERT_TRUE(meta.Load(dpMetaFilePath).OK());
    for (size_t i = 0; i < meta.deployFileMetas.size(); i++) {
        if (meta.deployFileMetas[i].filePath == filePath) {
            ASSERT_TRUE(isExist) << filePath;
            return;
        }
    }
    ASSERT_TRUE(!isExist) << filePath;
}

void PartitionResourceProviderInteTest::CheckFileExist(const string& targetFile, const vector<string>& fileLists,
                                                       bool isExist)
{
    if (find(fileLists.begin(), fileLists.end(), targetFile) != fileLists.end()) {
        ASSERT_EQ(isExist, true) << targetFile;
    } else {
        ASSERT_EQ(isExist, false) << targetFile;
    }
}

void PartitionResourceProviderInteTest::CheckVersion(const file_system::DirectoryPtr& versionDir, versionid_t vid,
                                                     const string& segIdStr, segmentid_t lastSegId)
{
    Version version;
    VersionLoader::GetVersion(versionDir, version, vid);

    vector<segmentid_t> segIds;
    StringUtil::fromString(segIdStr, segIds, ",");

    ASSERT_EQ(segIds, version.GetSegmentVector());
    ASSERT_EQ(lastSegId, version.GetLastSegment());
}

void PartitionResourceProviderInteTest::TestAsyncWriteAttribute()
{
    file_system::FileSystemOptions fsOptions;
    fsOptions.isOffline = true;
    string tableDirName = "index_table";
    string rootPath = PathUtil::JoinPath(GET_TEMP_DATA_PATH(), tableDirName);
    uint32_t schemaId = mNewSchema->GetSchemaVersionId();
    IndexPartitionOptions options;
    options.GetOfflineConfig().mergeConfig.mergeIOConfig.enableAsyncWrite = true;
    options.GetOfflineConfig().mergeConfig.mergeIOConfig.writeBufferSize = 10;
    PartitionResourceProviderPtr resourceProvider = PartitionResourceProviderFactory::GetInstance()->CreateProvider(
        GET_TEMP_DATA_PATH() + "/" + tableDirName, options, "");
    string patchDir = PathUtil::JoinPath(resourceProvider->GetPartition()->GetRootDirectory()->GetOutputPath(),
                                         PartitionPatchIndexAccessor::GetPatchRootDirName(schemaId));
    FslibWrapper::MkDirE(patchDir, true, true);
    string branchName;
    auto patchDirectory = BranchFS::GetDirectoryFromDefaultBranch(patchDir, fsOptions, nullptr, &branchName);
    PartitionPatcherPtr patcher = resourceProvider->CreatePartitionPatcher(mNewSchema, patchDirectory);
    PartitionSegmentIteratorPtr segIterator = resourceProvider->CreatePartitionSegmentIterator();
    char buf[3] = "10";
    StringView value(buf, 2);
    while (segIterator->IsValid()) {
        SegmentData segData = segIterator->GetSegmentData();
        // cout <<  "deal segment " << segData.GetSegmentId() << endl;
        // attribute
        AttributeDataPatcherPtr attrPatcher =
            patcher->CreateSingleAttributePatcher("new_value", segData.GetSegmentId());
        if (!attrPatcher) {
            segIterator->MoveToNext();
            continue;
        }
        // cout <<  "create patcher " << segData.GetSegmentId() << endl;
        assert(attrPatcher);
        AttributePatchDataWriterPtr writer = attrPatcher->TEST_GetAttributePatchDataWriter();
        // cout <<  "get writer " << segData.GetSegmentId() << endl;
        if (!writer) {
            segIterator->MoveToNext();
            continue;
        }
        // cout <<  "assert writer " << segData.GetSegmentId() << endl;
        file_system::FileWriterPtr fileWriter = writer->TEST_GetDataFileWriter();
        // cout <<  "get file writer " << segData.GetSegmentId() << endl;
        file_system::BufferedFileWriter* fileWriterType =
            dynamic_cast<file_system::BufferedFileWriter*>(fileWriter.get());
        ASSERT_TRUE(fileWriterType->_switchBuffer);
        ASSERT_EQ(10, fileWriterType->_switchBuffer->GetBufferSize());
        ASSERT_TRUE(fileWriterType->_threadPool);
        for (uint32_t i = 0; i < attrPatcher->GetTotalDocCount(); i++) {
            attrPatcher->AppendFieldValue(value);
        }
        attrPatcher->Close();
        segIterator->MoveToNext();
    }
    FslibWrapper::IsExist(patchDir + "/segment_0_level_0/attribute/new_value").GetOrThrow();
    FslibWrapper::IsExist(patchDir + "/segment_0_level_0/attribute/new_value/data").GetOrThrow();
}
}} // namespace indexlib::partition
