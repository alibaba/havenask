#include "indexlib/partition/remote_access/test/partition_resource_provider_intetest.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/remote_access/partition_resource_provider_factory.h"
#include "indexlib/partition/remote_access/attribute_data_seeker.h"
#include "indexlib/partition/remote_access/attribute_patch_data_writer.h"
#include "indexlib/partition/index_roll_back_util.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/index/calculator/partition_size_calculator.h"
#include "indexlib/index/calculator/on_disk_segment_size_calculator.h"
#include "indexlib/index/normal/attribute/accessor/attribute_data_iterator.h"
#include "indexlib/index_base/patch/partition_patch_index_accessor.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/testlib/indexlib_partition_creator.h"
#include "indexlib/file_system/test/load_config_list_creator.h"
#include "indexlib/file_system/buffered_file_writer.h"
#include "indexlib/config/field_type_traits.h"
#include "indexlib/util/path_util.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/config/module_info.h"
#include <cmath>
#include <type_traits>

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(testlib);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, PartitionResourceProviderInteTest);

INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(PartitionResourceProviderInteTestMode,
                                     PartitionResourceProviderInteTest,
                                     testing::Values(true, false));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionResourceProviderInteTestMode, TestPartitionSeeker);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionResourceProviderInteTestMode, TestPartitionPatcher);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionResourceProviderInteTestMode, TestReadAndMergeWithPartitionPatchIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionResourceProviderInteTestMode, TestAutoDeletePartitionPatchMeta);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionResourceProviderInteTestMode, TestValidateDeployIndexForPatchIndexData);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionResourceProviderInteTestMode, TestOnlineCleanOnDiskIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionResourceProviderInteTestMode, TestOnDiskSegmentSizeCalculatorForPatch);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionResourceProviderInteTestMode, TestPatchMetaFileExist);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionResourceProviderInteTestMode, TestGetSegmentSizeByDeployIndexWrapper);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionResourceProviderInteTestMode, TestPartitionPatchIndexAccessor);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionResourceProviderInteTestMode, TestDeployIndexWrapperGetDeployFiles);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionResourceProviderInteTestMode, TestCreateRollBackVersion);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionResourceProviderInteTestMode, TestBuildIndexWithSchemaIdNotZero);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionResourceProviderInteTestMode, TestAsyncWriteAttribute);

PartitionResourceProviderInteTest::PartitionResourceProviderInteTest()
{
}

PartitionResourceProviderInteTest::~PartitionResourceProviderInteTest()
{
}

void PartitionResourceProviderInteTest::CaseSetUp()
{
    {
        // create index table
        string field = "name:string:false:false:uniq;price:uint32;"
                       "category:int32:true:true;tags:string:true;payload:float:true:true:fp16:4";
        string index = "pk:primarykey64:name";
        string attributes = "name;price;category;tags;payload";
        IndexPartitionSchemaPtr schema = IndexlibPartitionCreator::CreateSchema(
                "index_table", field, index, attributes, "");
        IndexPartitionOptions options;
        options.GetBuildConfig(false).maxDocCount = 2;
        string docs = "cmd=add,name=doc1,price=10,category=1 2,payload=1 2 3 4,tags=test1 test,ts=0;"
                      "cmd=add,name=doc2,price=20,category=2 3,payload=2 3 4 5,tags=test2 test,ts=0;"
                      "cmd=add,name=doc3,price=30,category=3 4,payload=3 4 5 6,tags=test3 test,ts=0;"
                      "cmd=add,name=doc4,price=40,category=4 5,payload=4 5 6 7,tags=test4 test,ts=0;"
                      "cmd=update_field,name=doc1,price=11,ts=0;"
                      "cmd=update_field,name=doc2,category=2 2,ts=0;";
        IndexlibPartitionCreator::CreateIndexPartition(schema,
                GET_TEST_DATA_PATH() + "/index_table", docs, options, "", false);
    }

    // create new schema
    string field =
        "name:string;price:uint32;category:int32:true:true;tags:string:true;payload:float:true:true:fp16:4;"
        "new_value:uint32;field0:text;field1:text;time:uint64;coordinate:location;feature:float:true:true:int8#127:2;";
    string index = "";
    bool shardingIndex = GET_CASE_PARAM();
    if (shardingIndex)
    {
        index = "pk:primarykey64:name;numberIndex:number:new_value;"
                "packIndex:expack:field0,field1:false:3;priceIndex:range:price;"
                "timeIndex:date:time;spatial_index:spatial:coordinate";
    }
    else
    {
        index = "pk:primarykey64:name;numberIndex:number:new_value;"
                "packIndex:expack:field0,field1;priceIndex:range:price;"
                "timeIndex:date:time;spatial_index:spatial:coordinate";
    }
    string attributes = "name;price;category;tags;payload;new_value;feature";
    mNewSchema = IndexlibPartitionCreator::CreateSchema("index_table", field, index, attributes, "");
    mNewSchema->SetSchemaVersionId(1);
}

void PartitionResourceProviderInteTest::CaseTearDown()
{
}

void PartitionResourceProviderInteTest::TestBuildIndexWithSchemaIdNotZero()
{
    // create index table
    string field = "name:string:false:false:uniq;price:uint32;"
                   "category:int32:true:true;tags:string:true";
    string index = "pk:primarykey64:name";
    string attributes = "name;price;category;tags";
    IndexPartitionSchemaPtr schema = IndexlibPartitionCreator::CreateSchema(
            "index_table", field, index, attributes, "");
    schema->SetSchemaVersionId(1);
    IndexPartitionOptions options;
    options.GetBuildConfig(false).maxDocCount = 2;
    string docs = "cmd=add,name=doc1,price=10,category=1 2,tags=test1 test,ts=0;"
                  "cmd=add,name=doc2,price=20,category=2 3,tags=test2 test,ts=0;"
                  "cmd=add,name=doc3,price=30,category=3 4,tags=test3 test,ts=0;"
                  "cmd=add,name=doc4,price=40,category=4 5,tags=test4 test,ts=0;"
                  "cmd=update_field,name=doc1,price=11,ts=0;"
                  "cmd=update_field,name=doc2,category=2 2,ts=0;";
    IndexlibPartitionCreator::CreateIndexPartition(schema,
            GET_TEST_DATA_PATH() + "/schema_1_table", docs, options, "", false);
    mNewSchema->SetSchemaVersionId(2);
    PreparePatchIndex("schema_1_table");
}

void PartitionResourceProviderInteTest::TestPartitionIterator()
{
    IndexPartitionOptions options;
    PartitionResourceProviderPtr resourceProvider =
        PartitionResourceProviderFactory::GetInstance()->CreateProvider(
                GET_TEST_DATA_PATH() + "/index_table", options, "");

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
        PartitionResourceProviderPtr resourceProvider =
            PartitionResourceProviderFactory::GetInstance()->CreateProvider(
                GET_TEST_DATA_PATH() + "/index_table", options, "");
        PartitionSeekerPtr seeker = resourceProvider->CreatePartitionSeeker();
        CheckSeeker(seeker, "doc1", "price=11;name=doc1;category=1,2;tags=test1,test;payload=1,2,3,4");
        CheckSeeker(seeker, "doc2", "price=20;name=doc2;category=2,2;tags=test2,test;payload=2,3,4,5");
        CheckSeeker(seeker, "doc3", "price=30;name=doc3;category=3,4;tags=test3,test;payload=3,4,5,6");
        CheckSeeker(seeker, "doc4", "price=40;name=doc4;category=4,5;tags=test4,test;payload=4,5,6,7");

        PartitionSeekerPtr newSeeker = resourceProvider->CreatePartitionSeeker();
        newSeeker->SetEnableCountedMultiValue(false);
        CheckSeeker(newSeeker, "doc1", "price=11;name=doc1;category=1,2;tags=test1,test;payload=1,2,3,4");
        CheckSeeker(newSeeker, "doc2", "price=20;name=doc2;category=2,2;tags=test2,test;payload=2,3,4,5");
        CheckSeeker(newSeeker, "doc3", "price=30;name=doc3;category=3,4;tags=test3,test;payload=3,4,5,6");
        CheckSeeker(newSeeker, "doc4", "price=40;name=doc4;category=4,5;tags=test4,test;payload=4,5,6,7");
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
    IndexPartitionSchemaPtr newSchema = IndexlibPartitionCreator::CreateSchema(
            "index_table", field, index, attributes, "");
    newSchema->SetSchemaVersionId(1);

    string rootPath = GET_TEST_DATA_PATH() + "/index_table";
    IndexPartitionOptions options;
    PartitionResourceProviderPtr resourceProvider =
        PartitionResourceProviderFactory::GetInstance()->CreateProvider(rootPath, options, "");

    string patchDir1 = GET_TEST_DATA_PATH() + "/index_table/patch_index_1";
    string patchDir2 = GET_TEST_DATA_PATH() + "/index_table/patch_index_2";
    FileSystemWrapper::MkDirIfNotExist(patchDir1);
    FileSystemWrapper::MkDirIfNotExist(patchDir2);
    {
        PartitionPatcherPtr patcher =
            resourceProvider->CreatePartitionPatcher(newSchema, patchDir1);
        AttributeConfigPtr attrConf = patcher->GetAlterAttributeConfig("score1");
        ASSERT_EQ(FieldType::ft_fp16, attrConf->GetFieldType());
        attrConf = patcher->GetAlterAttributeConfig("score2");
        ASSERT_EQ(FieldType::ft_fp8, attrConf->GetFieldType());
        AttributeDataPatcherPtr attr1Patcher = patcher->CreateSingleAttributePatcher(
                "new_value1", 0);
        AttributeDataPatcherPtr attr2Patcher = patcher->CreateSingleAttributePatcher(
                "new_value2", 1);
        AttributeDataPatcherPtr attr3Patcher = patcher->CreateSingleAttributePatcher(
                "score1", 1);
        AttributeDataPatcherPtr attr4Patcher = patcher->CreateSingleAttributePatcher(
                "score2", 1);

        IndexDataPatcherPtr indexPatcher = patcher->CreateSingleIndexPatcher(
                "numberIndex", 0);
        attr1Patcher->Close();
        attr2Patcher->Close();
        attr3Patcher->Close();
        attr4Patcher->Close();
        indexPatcher->Close();
        ASSERT_TRUE(FileSystemWrapper::IsExist(patchDir1 + "/segment_0_level_0/attribute/new_value1"));
        ASSERT_TRUE(FileSystemWrapper::IsExist(patchDir1 + "/segment_0_level_0/attribute/new_value1/data"));
        ASSERT_TRUE(FileSystemWrapper::IsExist(patchDir1 + "/segment_1_level_0/attribute/new_value2"));
        ASSERT_TRUE(FileSystemWrapper::IsExist(patchDir1 + "/segment_1_level_0/attribute/new_value2/data"));
        ASSERT_TRUE(FileSystemWrapper::IsExist(patchDir1 + "/segment_1_level_0/attribute/new_value2/offset"));
        ASSERT_TRUE(FileSystemWrapper::IsExist(patchDir1 + "/segment_1_level_0/attribute/score1"));
        ASSERT_TRUE(FileSystemWrapper::IsExist(patchDir1 + "/segment_1_level_0/attribute/score1/data"));
        ASSERT_TRUE(FileSystemWrapper::IsExist(patchDir1 + "/segment_1_level_0/attribute/score2"));
        ASSERT_TRUE(FileSystemWrapper::IsExist(patchDir1 + "/segment_1_level_0/attribute/score2/data"));
        ASSERT_TRUE(FileSystemWrapper::IsExist(patchDir1 + "/segment_0_level_0/index/numberIndex"));
    }

    resourceProvider->StoreVersion(newSchema, 1);
    CheckPatchMeta(rootPath, 1, "1:0,1");
    newSchema->SetSchemaVersionId(2);
    {
        // new attribute use equal & uniq compress
        FieldConfigPtr fieldConfig = newSchema->GetFieldSchema()->GetFieldConfig("new_value1");
        fieldConfig->SetCompressType("equal");
        fieldConfig = newSchema->GetFieldSchema()->GetFieldConfig("new_value2");
        fieldConfig->SetCompressType("equal|uniq");

        PartitionPatcherPtr patcher =
            resourceProvider->CreatePartitionPatcher(newSchema, patchDir2);
        AttributeDataPatcherPtr attr1Patcher = patcher->CreateSingleAttributePatcher(
                "new_value1", 0);
        AttributeDataPatcherPtr attr2Patcher = patcher->CreateSingleAttributePatcher(
                "new_value2", 1);
        IndexDataPatcherPtr indexPatcher = patcher->CreateSingleIndexPatcher(
                "numberIndex", 0);
        attr1Patcher->Close();
        attr2Patcher->Close();
        indexPatcher->Close();
        ASSERT_TRUE(FileSystemWrapper::IsExist(patchDir2 + "/segment_0_level_0/attribute/new_value1"));
        ASSERT_TRUE(FileSystemWrapper::IsExist(patchDir2 + "/segment_0_level_0/attribute/new_value1/data"));
        ASSERT_TRUE(FileSystemWrapper::IsExist(patchDir2 + "/segment_1_level_0/attribute/new_value2"));
        ASSERT_TRUE(FileSystemWrapper::IsExist(patchDir2 + "/segment_1_level_0/attribute/new_value2/data"));
        ASSERT_TRUE(FileSystemWrapper::IsExist(patchDir2 + "/segment_1_level_0/attribute/new_value2/offset"));
        ASSERT_TRUE(FileSystemWrapper::IsExist(patchDir2 + "/segment_0_level_0/index/numberIndex"));
    }
    resourceProvider->StoreVersion(newSchema, 2);
    CheckPatchMeta(rootPath, 2, "2:0,1;1:1");
    ASSERT_NE(FileSystemWrapper::GetFileLength(
                    patchDir1 + "/segment_0_level_0/attribute/new_value1/data"),
              FileSystemWrapper::GetFileLength(
                      patchDir2 + "/segment_0_level_0/attribute/new_value1/data"));

    ASSERT_NE(FileSystemWrapper::GetFileLength(
                    patchDir1 + "/segment_1_level_0/attribute/new_value2/data"),
              FileSystemWrapper::GetFileLength(
                      patchDir2 + "/segment_1_level_0/attribute/new_value2/data"));

    ASSERT_NE(FileSystemWrapper::GetFileLength(
                    patchDir1 + "/segment_1_level_0/attribute/new_value2/offset"),
              FileSystemWrapper::GetFileLength(
                      patchDir2 + "/segment_1_level_0/attribute/new_value2/offset"));
}

void PartitionResourceProviderInteTest::CheckIterator(
        const PartitionIteratorPtr& iterator,
        segmentid_t segmentId, const string& attrName,
        const string& attrValues)
{
    ASSERT_TRUE(iterator);
    AttributeDataIteratorPtr attrIter =
        iterator->CreateSingleAttributeIterator(attrName, segmentId);
    ASSERT_TRUE(attrIter);

    vector<string> valueStrs;
    StringUtil::fromString(attrValues, valueStrs, ";");

    size_t count = 0;
    while (attrIter->IsValid())
    {
        ASSERT_EQ(valueStrs[count], attrIter->GetValueStr());
        count++;
        attrIter->MoveToNext();
    }
    ASSERT_EQ(count, valueStrs.size());
}

void PartitionResourceProviderInteTest::CheckSeeker(
        const PartitionSeekerPtr& seeker,
        const string& key, const string& fieldValues)
{
    ASSERT_TRUE(seeker);

    vector<vector<string>> fieldInfoVec;
    StringUtil::fromString(fieldValues, fieldInfoVec, "=", ";");

    Pool pool;
    for (size_t i = 0; i < fieldInfoVec.size(); i++)
    {
        assert(fieldInfoVec[i].size() == 2);
        const string& fieldName = fieldInfoVec[i][0];
        const string& fieldValue = fieldInfoVec[i][1];
        AttributeDataSeeker* attrSeeker =
            seeker->GetSingleAttributeSeeker(fieldName);

        AttributeConfigPtr attrConf = attrSeeker->GetAttrConfig();
        CompressTypeOption compressType = attrConf->GetFieldConfig()->GetCompressType();
        FieldType ft = attrConf->GetFieldType();
        bool isMultiValue = attrConf->IsMultiValue();
        bool isFixedLen = attrConf->IsLengthFixed();

        autil::ConstString keyStr(key);
        switch (ft)
        {
#define MACRO(field_type)                                               \
            case field_type:                                            \
            {                                                           \
                if (!isMultiValue)                                      \
                {                                                       \
                    typedef typename FieldTypeTraits<field_type>::AttrItemType T; \
                    T value;                                            \
                    AttributeDataSeekerTyped<T>* typedSeeker = dynamic_cast<AttributeDataSeekerTyped<T>*>(attrSeeker); \
                    assert(typedSeeker);                                \
                    ASSERT_TRUE(typedSeeker->SeekByString(keyStr, value)); \
                    ASSERT_EQ(StringUtil::fromString<T>(fieldValue), value); \
                }                                                       \
                else if (!isFixedLen || !seeker->EnableCountedMultiValue()) \
                {                                                       \
                    typedef typename FieldTypeTraits<field_type>::AttrItemType T; \
                    typedef typename autil::MultiValueType<T> MT;       \
                    MT value;                                           \
                    AttributeDataSeekerTyped<MT>* typedSeeker = dynamic_cast<AttributeDataSeekerTyped<MT>*>(attrSeeker); \
                    assert(typedSeeker);                                \
                    ASSERT_TRUE(typedSeeker->SeekByString(keyStr, value)); \
                    vector<T> values;                                   \
                    StringUtil::fromString(fieldValue, values, ",");    \
                    ASSERT_EQ(value.size(), (uint32_t)values.size());   \
                    for (size_t i = 0; i < values.size(); i++)          \
                    {                                                   \
                        using T2 = std::conditional<std::is_unsigned<T>::value, int64_t, T>::type; \
                        if (compressType.HasBlockFpEncodeCompress())    \
                        {                                               \
                            ASSERT_TRUE(std::abs((T2)(values[i]) - (T2)(value[i])) <= 0.0001); \
                        }                                               \
                        else if (compressType.HasFp16EncodeCompress())  \
                        {                                               \
                            ASSERT_TRUE(std::abs((T2)(values[i]) - (T2)(value[i])) <= 0.01); \
                        }                                               \
                        else                                            \
                        {                                               \
                            ASSERT_EQ(values[i], value[i]);             \
                        }                                               \
                    }                                                   \
                }                                                       \
                else                                                    \
                {                                                       \
                    typedef typename FieldTypeTraits<field_type>::AttrItemType T; \
                    typedef typename autil::CountedMultiValueType<T> MT; \
                    MT value;                                           \
                    AttributeDataSeekerTyped<MT>* typedSeeker = dynamic_cast<AttributeDataSeekerTyped<MT>*>(attrSeeker); \
                    assert(typedSeeker);                                \
                    ASSERT_TRUE(typedSeeker->SeekByString(keyStr, value)); \
                    vector<T> values;                                   \
                    StringUtil::fromString(fieldValue, values, ",");    \
                    ASSERT_EQ(value.size(), (uint32_t)values.size());   \
                    for (size_t i = 0; i < values.size(); i++)          \
                    {                                                   \
                        using T2 = std::conditional<std::is_unsigned<T>::value, int64_t, T>::type; \
                        if (compressType.HasBlockFpEncodeCompress())    \
                        {                                               \
                            ASSERT_TRUE(std::abs((T2)(values[i]) - (T2)(value[i])) <= 0.0001); \
                        }                                               \
                        else if (compressType.HasFp16EncodeCompress())  \
                        {                                               \
                            ASSERT_TRUE(std::abs((T2)(values[i]) - (T2)(value[i])) <= 0.01); \
                        }                                               \
                        else                                            \
                        {                                               \
                            ASSERT_EQ(values[i], value[i]);             \
                        }                                               \
                    }                                                   \
                }                                                       \
                break;                                                  \
            }

            NUMBER_FIELD_MACRO_HELPER(MACRO);

        case ft_string:
            if (!isMultiValue)
            {
                MultiChar value;
                AttributeDataSeekerTyped<MultiChar>* typedSeeker =
                    dynamic_cast<AttributeDataSeekerTyped<MultiChar>*>(attrSeeker);
                assert(typedSeeker);
                ASSERT_TRUE(typedSeeker->SeekByString(keyStr, value));
                ASSERT_EQ(fieldValue, string(value.data(), value.size()));
            }
            else if (!isFixedLen || !seeker->EnableCountedMultiValue())
            {
                MultiString value;
                AttributeDataSeekerTyped<MultiString>* typedSeeker =
                    dynamic_cast<AttributeDataSeekerTyped<MultiString>*>(attrSeeker);
                assert(typedSeeker);
                ASSERT_TRUE(typedSeeker->SeekByString(keyStr, value));
                vector<string> values;
                StringUtil::fromString(fieldValue, values, ",");
                ASSERT_EQ(value.size(), (uint32_t)values.size());
                for (size_t i = 0; i < values.size(); i++)
                {
                    string valueStr(value[i].data(), value[i].size());
                    ASSERT_EQ(valueStr, values[i]);
                }
            }
            else
            {
                CountedMultiString value;
                AttributeDataSeekerTyped<CountedMultiString>* typedSeeker =
                    dynamic_cast<AttributeDataSeekerTyped<CountedMultiString>*>(attrSeeker);
                assert(typedSeeker);
                ASSERT_TRUE(typedSeeker->SeekByString(keyStr, value));
                vector<string> values;
                StringUtil::fromString(fieldValue, values, ",");
                ASSERT_EQ(value.size(), (uint32_t)values.size());
                for (size_t i = 0; i < values.size(); i++)
                {
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

    string rootDir = GET_TEST_DATA_PATH() + "/index_table";
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
    ASSERT_TRUE(psm.Transfer(QUERY, "", "timeIndex:[1516086000000, 1516088000000]",
                             "docid=0;docid=1;docid=2"));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "spatial_index:point(116.3906 39.92324)",
                             "docid=0;docid=1;docid=2;docid=3"));

    string docs = "cmd=add,name=doc5,price=50,category=5 2,new_value=5,feature=5 5,field0=5,field1=6,tags=test5 test,coordinate=116.3906 39.92324,time=1516086500000,ts=10;"
                  "cmd=add,name=doc6,price=60,category=6 3,new_value=6,feature=6 6,field0=6,field1=7,tags=test6 test,coordinate=116.3906 39.92324,time=1516088500000,ts=10;"
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
    ASSERT_TRUE(psm.Transfer(QUERY, "", "timeIndex:[1516086000000, 1516088000000]",
                             "docid=0;docid=1;docid=2;docid=4"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "spatial_index:point(116.3906 39.92324)",
                             "docid=0;docid=1;docid=2;docid=3;docid=4;docid=5"));

    CheckPatchMeta(rootDir, 2, "1:0,1");

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
    ASSERT_TRUE(psm.Transfer(QUERY, "", "timeIndex:[1516086000000, 1516088000000]",
                             "docid=0;docid=1;docid=2;docid=4"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "spatial_index:point(116.3906 39.92324)",
                             "docid=0;docid=1;docid=2;docid=3;docid=4;docid=5"));
    CheckPatchMeta(rootDir, 3, "");
}

void PartitionResourceProviderInteTest::TestAutoDeletePartitionPatchMeta()
{
    // version 1: segment 0, 1, 2
    PreparePatchIndex();
    string rootDir = GET_TEST_DATA_PATH() + "/index_table/";
    ASSERT_TRUE(FileSystemWrapper::IsExist(rootDir + "patch_index_1"));
    ASSERT_TRUE(FileSystemWrapper::IsExist(rootDir + "patch_index_1/segment_0_level_0"));
    ASSERT_TRUE(FileSystemWrapper::IsExist(rootDir + "patch_index_1/segment_1_level_0"));
    ASSERT_TRUE(FileSystemWrapper::IsExist(rootDir + "schema.json"));
    ASSERT_TRUE(FileSystemWrapper::IsExist(rootDir + "schema.json.1"));

    IndexPartitionOptions options;
    options.GetBuildConfig(false).keepVersionCount = 1;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mNewSchema, options, rootDir));
    string docs = "cmd=add,name=doc5,price=50,category=5 2,new_value=5,tags=test5 test,ts=10;"
                  "cmd=add,name=doc6,price=60,category=6 3,new_value=6,tags=test6 test,ts=10;"
                  "cmd=update_field,name=doc1,new_value=7,ts=10;";

    // version 2: segment 3,4
    ASSERT_TRUE(psm.Transfer(PE_BUILD_INC, docs, "", ""));
    CheckPatchMeta(rootDir, 2, "1:0,1");

    ASSERT_FALSE(FileSystemWrapper::IsExist(rootDir + "schema.json"));
    ASSERT_TRUE(FileSystemWrapper::IsExist(rootDir + "schema.json.1"));
    ASSERT_FALSE(FileSystemWrapper::IsExist(rootDir + "version.1"));
    ASSERT_FALSE(FileSystemWrapper::IsExist(rootDir + "deploy_meta.1"));
    ASSERT_FALSE(FileSystemWrapper::IsExist(rootDir + "patch_meta.1"));
    ASSERT_TRUE(FileSystemWrapper::IsExist(rootDir + "version.2"));
    ASSERT_TRUE(FileSystemWrapper::IsExist(rootDir + "deploy_meta.2"));
    ASSERT_TRUE(FileSystemWrapper::IsExist(rootDir + "patch_meta.2"));

    MergeConfig specificMerge;
    specificMerge.mergeStrategyStr = "specific_segments";
    specificMerge.mergeStrategyParameter.SetLegacyString("merge_segments=1,2,3");
    psm.SetMergeConfig(specificMerge);

    // version 3: segment 0, 4, 5, remove segment 1 in root & patch_index
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, "", "", ""));
    ASSERT_TRUE(FileSystemWrapper::IsExist(rootDir + "version.3"));
    ASSERT_TRUE(FileSystemWrapper::IsExist(rootDir + "deploy_meta.3"));
    ASSERT_TRUE(FileSystemWrapper::IsExist(rootDir + "patch_meta.3"));
    ASSERT_TRUE(FileSystemWrapper::IsExist(rootDir + "patch_index_1/segment_0_level_0"));
    ASSERT_FALSE(FileSystemWrapper::IsExist(rootDir + "patch_index_1/segment_1_level_0"));
    CheckPatchMeta(rootDir, 3, "1:0");

    // version 4: segment 6, remove patch_index_1
    MergeConfig optMergeConf;
    psm.SetMergeConfig(optMergeConf);
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, "", "", ""));
    ASSERT_FALSE(FileSystemWrapper::IsExist(rootDir + "patch_index_1"));
    CheckPatchMeta(rootDir, 4, "");
}

void PartitionResourceProviderInteTest::TestValidateDeployIndexForPatchIndexData()
{
    PreparePatchIndex();

    string rootDir = GET_TEST_DATA_PATH() + "/index_table";
    index_base::DeployIndexMeta dpMeta;
    dpMeta.Load(rootDir + "/deploy_meta.1");

    vector<string> patchIndexPathVec;
    for (const auto& meta : dpMeta.deployFileMetas)
    {
        if (meta.filePath.find(PARTITION_PATCH_DIR_PREFIX) != string::npos &&
            meta.fileLength != (int64_t)-1)
        {
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
        string toRemoveFilePath = FileSystemWrapper::JoinPath(rootDir, patchIndexPathVec[0]);
        FileSystemWrapper::DeleteIfExist(toRemoveFilePath);
        IndexPartitionOptions options;
        PartitionStateMachine psm;
        psm.Init(mNewSchema, options, rootDir);
        ASSERT_FALSE(psm.Transfer(BUILD_INC_NO_MERGE, "", "pk:doc1", "docid=0,new_value=0"));
    }
}

void PartitionResourceProviderInteTest::TestOnlineCleanOnDiskIndex()
{
    // version 1: segment 0, 1, 2
    PreparePatchIndex();
    string rootDir = GET_TEST_DATA_PATH() + "/index_table/";
    ASSERT_TRUE(FileSystemWrapper::IsExist(rootDir + "schema.json"));
    ASSERT_TRUE(FileSystemWrapper::IsExist(rootDir + "schema.json.1"));
    ASSERT_TRUE(FileSystemWrapper::IsExist(rootDir + "patch_index_1"));
    ASSERT_TRUE(FileSystemWrapper::IsExist(rootDir + "patch_index_1/segment_0_level_0"));
    ASSERT_TRUE(FileSystemWrapper::IsExist(rootDir + "patch_index_1/segment_1_level_0"));

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
    ASSERT_TRUE(FileSystemWrapper::IsExist(rootDir + "version.3"));
    ASSERT_TRUE(FileSystemWrapper::IsExist(rootDir + "deploy_meta.3"));
    ASSERT_TRUE(FileSystemWrapper::IsExist(rootDir + "patch_meta.3"));
    ASSERT_TRUE(FileSystemWrapper::IsExist(rootDir + "patch_index_1/segment_0_level_0"));
    ASSERT_TRUE(FileSystemWrapper::IsExist(rootDir + "patch_index_1/segment_1_level_0"));

    // create online partition
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:doc1", "docid=0,new_value=7"));

    sleep(3);

    ASSERT_FALSE(FileSystemWrapper::IsExist(rootDir + "schema.json"));
    ASSERT_TRUE(FileSystemWrapper::IsExist(rootDir + "schema.json.1"));
    ASSERT_FALSE(FileSystemWrapper::IsExist(rootDir + "patch_index_1/segment_1_level_0"));
    ASSERT_TRUE(FileSystemWrapper::IsExist(rootDir + "patch_index_1/segment_0_level_0"));
    ASSERT_FALSE(FileSystemWrapper::IsExist(rootDir + "deploy_meta.2"));
    ASSERT_FALSE(FileSystemWrapper::IsExist(rootDir + "deploy_meta.1"));

    // version 4: segment 6, online remove useless patch_index data
    MergeConfig optMerger;
    psm.SetMergeConfig(optMerger);
    ASSERT_TRUE(psm.Transfer(BUILD_INC, "", "pk:doc1", "docid=0,new_value=7"));

    sleep(3);
    ASSERT_FALSE(FileSystemWrapper::IsExist(rootDir + "patch_index_1/segment_0_level_0"));
    ASSERT_FALSE(FileSystemWrapper::IsExist(rootDir + "version.3"));
    ASSERT_FALSE(FileSystemWrapper::IsExist(rootDir + "patch_meta.3"));
    ASSERT_FALSE(FileSystemWrapper::IsExist(rootDir + "deploy_meta.3"));
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

    string dpFileName = GET_TEST_DATA_PATH() + "/index_table/patch_index_1/segment_0_level_0/deploy_index";
    DeployIndexMeta meta;
    meta.Load(dpFileName);
    size_t expectSize = 0;
    for (size_t i = 0; i < meta.deployFileMetas.size(); i++)
    {
        if (meta.deployFileMetas[i].filePath.find("meta") != string::npos ||
            meta.deployFileMetas[i].filePath.find("option") != string::npos ||
            meta.deployFileMetas[i].filePath.find("info") != string::npos)
        {
            continue;
        }

        if (meta.deployFileMetas[i].fileLength > 0)
        {
            expectSize += meta.deployFileMetas[i].fileLength;
        }
    }
    ASSERT_EQ(expectSize, v1SegSize- v0SegSize);
}

void PartitionResourceProviderInteTest::TestPatchMetaFileExist()
{
    string rootDir = GET_TEST_DATA_PATH() + "/index_table/";
    ASSERT_FALSE(FileSystemWrapper::IsExist(GET_TEST_DATA_PATH() + "/index_table/patch_meta.0"));
    string dpFileName = GET_TEST_DATA_PATH() + "/index_table/deploy_meta.0";
    CheckFileExistInDeployIndexFile(dpFileName, "patch_meta.0", false);

    PreparePatchIndex();
    // version.1, convert index
    dpFileName = GET_TEST_DATA_PATH() + "/index_table/deploy_meta.1";
    CheckFileExistInDeployIndexFile(dpFileName, "patch_meta.1", true);

    // version.2, build comit version
    IndexPartitionOptions options;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mNewSchema, options, rootDir));
    string docs = "cmd=add,name=doc5,price=50,category=5 2,new_value=5,tags=test5 test,ts=10;"
                  "cmd=add,name=doc6,price=60,category=6 3,new_value=6,tags=test6 test,ts=10;"
                  "cmd=update_field,name=doc1,new_value=7,ts=10;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, docs, "", ""));
    ASSERT_TRUE(FileSystemWrapper::IsExist(rootDir + "patch_meta.2"));
    dpFileName = GET_TEST_DATA_PATH() + "/index_table/deploy_meta.2";
    CheckFileExistInDeployIndexFile(dpFileName, "patch_meta.2", true);

    Version version = psm.GetIndexPartition()->GetPartitionData()->GetOnDiskVersion();
    IndexPartitionSchemaPtr schema = psm.GetIndexPartition()->GetSchema();
    ASSERT_EQ((schemavid_t)1, version.GetSchemaVersionId());
    ASSERT_EQ((schemavid_t)1, schema->GetSchemaVersionId());

    // version.3, merger commit version
    ASSERT_TRUE(psm.Transfer(BUILD_INC, "", "", ""));
    ASSERT_TRUE(FileSystemWrapper::IsExist(rootDir + "patch_meta.3"));
    dpFileName = GET_TEST_DATA_PATH() + "/index_table/deploy_meta.3";
    CheckFileExistInDeployIndexFile(dpFileName, "patch_meta.3", true);
}

void PartitionResourceProviderInteTest::TestGetSegmentSizeByDeployIndexWrapper()
{
    string rootDir = GET_TEST_DATA_PATH() + "/index_table/";
    int64_t totalLen1 = 0;
    IndexPartitionOptions options;
    {
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(mNewSchema, options, rootDir));
        psm.Transfer(QUERY, "", "pk:doc1", "docid=0");
        PartitionDataPtr partData = psm.GetIndexPartition()->GetPartitionData();
        SegmentData segData = partData->GetSegmentData(0);
        DeployIndexWrapper::GetSegmentSize(segData, true, totalLen1);
    }

    int64_t totalLen2 = 0;
    PreparePatchIndex();
    {
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(mNewSchema, options, rootDir));
        psm.Transfer(QUERY, "", "pk:doc1", "docid=0");
        PartitionDataPtr partData = psm.GetIndexPartition()->GetPartitionData();
        SegmentData segData = partData->GetSegmentData(0);
        DeployIndexWrapper::GetSegmentSize(segData, true, totalLen2);
    }
    ASSERT_LT(totalLen1, totalLen2);
}

void PartitionResourceProviderInteTest::TestPartitionPatchIndexAccessor()
{
    PreparePatchIndex();

    string rootDir = GET_TEST_DATA_PATH() + "/index_table/";
    PartitionStateMachine psm;
    IndexPartitionOptions options;
    ASSERT_TRUE(psm.Init(mNewSchema, options, rootDir));
    psm.Transfer(QUERY, "", "pk:doc1", "docid=0");
    PartitionDataPtr partData = psm.GetIndexPartition()->GetPartitionData();
    SegmentData segData = partData->GetSegmentData(0);

    PartitionPatchIndexAccessorPtr accessor = segData.GetPatchIndexAccessor();
    ASSERT_TRUE(accessor);

    string patchRoot = PartitionPatchIndexAccessor::GetPatchRootDirName(1);
    DirectoryPtr indexDir = accessor->GetIndexDirectory(0, "numberIndex", true);
    ASSERT_TRUE(indexDir->GetPath().find(patchRoot) != string::npos);
    indexDir = accessor->GetIndexDirectory(1, "numberIndex", true);
    ASSERT_TRUE(indexDir->GetPath().find(patchRoot) != string::npos);

    DirectoryPtr attrDir = accessor->GetAttributeDirectory(0, "new_value", true);
    ASSERT_TRUE(attrDir->GetPath().find(patchRoot) != string::npos);
    attrDir = accessor->GetAttributeDirectory(1, "new_value", true);
    ASSERT_TRUE(attrDir->GetPath().find(patchRoot) != string::npos);

    ASSERT_FALSE(accessor->GetAttributeDirectory(0, "not_exist", true));
    ASSERT_FALSE(accessor->GetIndexDirectory(0, "not_exist", true));
}

void PartitionResourceProviderInteTest::TestDeployIndexWrapperGetDeployFiles()
{
    string rootDir = GET_TEST_DATA_PATH() + "/index_table/";
    PreparePatchIndex();

    DeployIndexWrapper indexWrapper(rootDir);
    fslib::FileList files;
    indexWrapper.GetDeployFiles(files, 1);
    CheckFileExist("schema.json", files, false);
    CheckFileExist("schema.json.1", files, true);
    CheckFileExist("patch_meta.1", files, true);
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

    files.clear();
    indexWrapper.GetDeployFiles(files, 1, 0);
    CheckFileExist("schema.json.1", files, true);
    CheckFileExist("patch_meta.1", files, true);
    CheckFileExist("patch_meta.1", files, true);
    CheckFileExist("patch_index_1/segment_0_level_0/index/numberIndex/dictionary", files, true);
    CheckFileExist("patch_index_1/segment_0_level_0/index/numberIndex/posting", files, true);
    CheckFileExist("patch_index_1/segment_0_level_0/index/numberIndex/index_format_option", files, true);
    CheckFileExist("patch_index_1/segment_1_level_0/index/numberIndex/dictionary", files, true);
    CheckFileExist("patch_index_1/segment_1_level_0/index/numberIndex/posting", files, true);
    CheckFileExist("patch_index_1/segment_1_level_0/index/numberIndex/index_format_option", files, true);
}

void PartitionResourceProviderInteTest::TestCreateRollBackVersion()
{
    PreparePatchIndex();
    string rootPath = PathUtil::JoinPath(GET_TEST_DATA_PATH(), "index_table");
    CheckVersion(rootPath + "/version.0", "0,1,2", 2);
    CheckVersion(rootPath + "/version.1", "0,1,2", 2);

    // version.2, build comit version
    IndexPartitionOptions options;
    options.GetBuildConfig(false).keepVersionCount = 10;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mNewSchema, options, rootPath));
    string docs = "cmd=add,name=doc5,price=50,category=5 2,new_value=5,tags=test5 test,ts=10;"
                  "cmd=add,name=doc6,price=60,category=6 3,new_value=6,tags=test6 test,ts=10;"
                  "cmd=update_field,name=doc1,new_value=7,ts=10;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, docs, "", ""));
    CheckVersion(rootPath + "/version.2", "0,1,2,3", 3);

    IndexRollBackUtil::CreateRollBackVersion(rootPath, 0, 3);
    CheckVersion(rootPath + "/version.3", "0,1,2", 3);
    CheckFileExistInDeployIndexFile(rootPath + "/deploy_meta.3", "schema.json", true);
    CheckFileExistInDeployIndexFile(rootPath + "/deploy_meta.3", "patch_meta.3", false);
    ASSERT_FALSE(FileSystemWrapper::IsExist(rootPath + "/patch_meta.3"));

    IndexRollBackUtil::CreateRollBackVersion(rootPath, 2, 4);
    CheckVersion(rootPath + "/version.4", "0,1,2,3", 3);
    CheckFileExistInDeployIndexFile(rootPath + "/deploy_meta.4", "schema.json.1", true);
    CheckFileExistInDeployIndexFile(rootPath + "/deploy_meta.4", "patch_meta.4", true);
    ASSERT_TRUE(FileSystemWrapper::IsExist(rootPath + "/patch_meta.4"));

    IndexRollBackUtil::CreateRollBackVersion(rootPath, 1, 5);
    CheckVersion(rootPath + "/version.5", "0,1,2", 3);
    CheckFileExistInDeployIndexFile(rootPath + "/deploy_meta.5", "schema.json.1", true);
    CheckFileExistInDeployIndexFile(rootPath + "/deploy_meta.5", "patch_meta.5", true);
    ASSERT_TRUE(FileSystemWrapper::IsExist(rootPath + "/patch_meta.5"));
}

void PartitionResourceProviderInteTest::PreparePatchIndex(const string& tableDirName)
{
    // prepare patch index dir
    string rootPath = PathUtil::JoinPath(GET_TEST_DATA_PATH(), tableDirName);
    uint32_t schemaId = mNewSchema->GetSchemaVersionId();
    string patchDir = PathUtil::JoinPath(rootPath,
            PartitionPatchIndexAccessor::GetPatchRootDirName(schemaId));
    FileSystemWrapper::MkDirIfNotExist(patchDir);

    IndexPartitionOptions options;
    PartitionResourceProviderPtr resourceProvider =
        PartitionResourceProviderFactory::GetInstance()->CreateProvider(
            GET_TEST_DATA_PATH() + "/" + tableDirName, options, "");

    PartitionPatcherPtr patcher =
        resourceProvider->CreatePartitionPatcher(mNewSchema, patchDir);
    PartitionSegmentIteratorPtr segIterator =
        resourceProvider->CreatePartitionSegmentIterator();
    uint32_t value = 0;
    uint32_t featureValue = 0;
    uint32_t indexValue = 0;
    uint32_t packIndexValue = 0;
    uint32_t priceValue = 0;
    uint64_t timeValue = 1516086000000;
    while (segIterator->IsValid())
    {
        SegmentData segData = segIterator->GetSegmentData();
        // attribute
        AttributeDataPatcherPtr attrPatcher = patcher->CreateSingleAttributePatcher(
                "new_value", segData.GetSegmentId());
        if (attrPatcher)
        {
            for (uint32_t i = 0; i < attrPatcher->GetTotalDocCount(); i++)
            {
                attrPatcher->AppendFieldValue(StringUtil::toString(value++));
            }
            attrPatcher->Close();
        }

        attrPatcher = patcher->CreateSingleAttributePatcher(
                "feature", segData.GetSegmentId());
        if (attrPatcher)
        {
            for (uint32_t i = 0; i < attrPatcher->GetTotalDocCount(); i++)
            {
                attrPatcher->AppendFieldValue(StringUtil::toString(featureValue++));
            }
            attrPatcher->Close();
        }

        // number index
        fieldid_t fieldId = mNewSchema->GetFieldSchema()->GetFieldConfig("new_value")->GetFieldId();
        IndexDataPatcherPtr indexPatcher = patcher->CreateSingleIndexPatcher(
                "numberIndex", segData.GetSegmentId());
        if (indexPatcher)
        {
            for (uint32_t i = 0; i < indexPatcher->GetTotalDocCount(); i++)
            {
                string valueStr = StringUtil::toString(indexValue++);
                indexPatcher->AddField(valueStr, fieldId);
                indexPatcher->EndDocument();
            }
            if (indexPatcher->GetTotalDocCount() != 0)
            {
                ASSERT_EQ(indexPatcher->GetPatchDocCount(), indexPatcher->GetDistinctTermCount());
            }
            indexPatcher->Close();
        }

        // expack index
        fieldid_t fieldId0 = mNewSchema->GetFieldSchema()->GetFieldConfig("field0")->GetFieldId();
        fieldid_t fieldId1 = mNewSchema->GetFieldSchema()->GetFieldConfig("field1")->GetFieldId();
        indexPatcher = patcher->CreateSingleIndexPatcher("packIndex", segData.GetSegmentId());
        if (indexPatcher)
        {
            for (uint32_t i = 0; i < indexPatcher->GetTotalDocCount(); i++)
            {
                string valueStr0 = StringUtil::toString(packIndexValue++);
                string valueStr1 = StringUtil::toString(packIndexValue);
                indexPatcher->AddField(valueStr0, fieldId0);
                indexPatcher->AddField(valueStr1, fieldId1);
                indexPatcher->EndDocument();
            }
            if (indexPatcher->GetTotalDocCount() != 0)
            {
                ASSERT_EQ(indexPatcher->GetPatchDocCount() + 1, indexPatcher->GetDistinctTermCount());
            }
            indexPatcher->Close();
        }

        // time index
        fieldId = mNewSchema->GetFieldSchema()->GetFieldConfig("time")->GetFieldId();
        indexPatcher = patcher->CreateSingleIndexPatcher("timeIndex", segData.GetSegmentId());
        if (indexPatcher)
        {
            for (uint32_t i = 0; i < indexPatcher->GetTotalDocCount(); i++)
            {
                string valueStr = StringUtil::toString(timeValue);
                timeValue += 1000000;
                indexPatcher->AddField(valueStr, fieldId);
                indexPatcher->EndDocument();
            }
            indexPatcher->Close();
        }

        // range index
        fieldId = mNewSchema->GetFieldSchema()->GetFieldConfig("price")->GetFieldId();
        indexPatcher = patcher->CreateSingleIndexPatcher("priceIndex", segData.GetSegmentId());
        if (indexPatcher)
        {
            for (uint32_t i = 0; i < indexPatcher->GetTotalDocCount(); i++)
            {
                string valueStr = StringUtil::toString(priceValue * 100);
                ++priceValue;
                indexPatcher->AddField(valueStr, fieldId);
                indexPatcher->EndDocument();
            }
            indexPatcher->Close();
        }

        // spatial index
        fieldId = mNewSchema->GetFieldSchema()->GetFieldConfig("coordinate")->GetFieldId();
        indexPatcher = patcher->CreateSingleIndexPatcher("spatial_index", segData.GetSegmentId());
        attrPatcher = patcher->CreateSingleAttributePatcher("coordinate", segData.GetSegmentId());
        if (indexPatcher)
        {
            assert(indexPatcher->GetTotalDocCount() == attrPatcher->GetTotalDocCount());
            string values = string("116.3906 39.92324");
            ConstString valueStr(values);
            for (uint32_t i = 0; i < indexPatcher->GetTotalDocCount(); i++)
            {
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
    ASSERT_TRUE(FileSystemWrapper::IsExist(rootPath + "/version.1"));
    ASSERT_TRUE(FileSystemWrapper::IsExist(rootPath + "/deploy_meta.1"));
    ASSERT_TRUE(FileSystemWrapper::IsExist(
                    rootPath + "/" + Version::GetSchemaFileName(schemaId)));
    ASSERT_TRUE(FileSystemWrapper::IsExist(rootPath + "/patch_meta.1"));
    ASSERT_TRUE(FileSystemWrapper::IsExist(
                    patchDir + "/segment_0_level_0/index/packIndex_section"));
    ASSERT_TRUE(FileSystemWrapper::IsExist(
                    patchDir + "/segment_0_level_0/index/timeIndex"));
    ASSERT_TRUE(FileSystemWrapper::IsExist(
                    patchDir + "/segment_0_level_0/index/priceIndex"));
    string patchMetaStr = StringUtil::toString(schemaId) + ":0,1";
    CheckPatchMeta(rootPath, 1, patchMetaStr);
}

// metaInfoStr: schemaId:seg0,seg1;...
void PartitionResourceProviderInteTest::CheckPatchMeta(
        const string& rootDir, versionid_t versionId, const string& metaInfoStr)
{
    vector<vector<string>> metaInfoVec;
    StringUtil::fromString(metaInfoStr, metaInfoVec, ":", ";");

    PartitionPatchMeta patchMeta;
    patchMeta.Load(rootDir, versionId);

    size_t schemaIdCount = 0;
    PartitionPatchMeta::Iterator metaIter = patchMeta.CreateIterator();
    while (metaIter.HasNext())
    {
        ASSERT_TRUE(schemaIdCount < metaInfoVec.size());
        assert(metaInfoVec[schemaIdCount].size() == 2);
        schemavid_t expectSchemaId =
            StringUtil::fromString<schemavid_t>(metaInfoVec[schemaIdCount][0]);

        vector<segmentid_t> expectSegIds;
        StringUtil::fromString(metaInfoVec[schemaIdCount][1], expectSegIds, ",");

        SchemaPatchInfoPtr schemaInfo = metaIter.Next();
        assert(schemaInfo);
        ASSERT_EQ(expectSchemaId, schemaInfo->GetSchemaId());

        size_t segmentIdCount = 0;
        SchemaPatchInfo::Iterator sIter = schemaInfo->Begin();
        for (; sIter != schemaInfo->End(); sIter++)
        {
            const SchemaPatchInfo::PatchSegmentMeta& segMeta = *sIter;
            ASSERT_TRUE(find(expectSegIds.begin(), expectSegIds.end(),
                            segMeta.GetSegmentId()) != expectSegIds.end()) << segMeta.GetSegmentId();
            segmentIdCount++;
        }
        ASSERT_EQ(expectSegIds.size(), segmentIdCount);
        schemaIdCount++;
    }
    ASSERT_EQ(metaInfoVec.size(), schemaIdCount);
}

size_t PartitionResourceProviderInteTest::GetMMapLockSize()
{
    string rootDir = GET_TEST_DATA_PATH() + "/index_table/";

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
    options.GetOnlineConfig().loadConfigList =
        LoadConfigListCreator::CreateLoadConfigListFromJsonString(mmapStr);
    PartitionStateMachine psm;
    psm.Init(mNewSchema, options, rootDir);
    psm.Transfer(QUERY, "", "pk:doc1", "docid=0");

    OnlinePartitionPtr onlinePart = DYNAMIC_POINTER_CAST(OnlinePartition, psm.GetIndexPartition());
    assert(onlinePart);
    IndexlibFileSystemPtr fileSystem = onlinePart->GetFileSystem();
    assert(fileSystem);
    StorageMetrics storageMetrics = fileSystem->GetStorageMetrics(FSST_LOCAL);
    return storageMetrics.GetMmapLockFileLength();
}

OnlinePartitionPtr PartitionResourceProviderInteTest::CreateOnlinePartition()
{
    string rootDir = GET_TEST_DATA_PATH() + "/index_table/";

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
    options.GetOnlineConfig().loadConfigList =
        LoadConfigListCreator::CreateLoadConfigListFromJsonString(mmapStr);
    OnlinePartitionPtr part(new OnlinePartition);
    part->Open(rootDir, "", mNewSchema, options);
    return part;
}

void PartitionResourceProviderInteTest::CheckFileExistInDeployIndexFile(
        const std::string& dpMetaFilePath, const string& filePath, bool isExist)
{
    index_base::DeployIndexMeta meta;
    meta.Load(dpMetaFilePath);
    for (size_t i = 0; i < meta.deployFileMetas.size(); i++)
    {
        if (meta.deployFileMetas[i].filePath == filePath)
        {
            ASSERT_EQ(true, isExist) << filePath;
            return;
        }
    }
    ASSERT_EQ(false, isExist) << filePath;
}

void PartitionResourceProviderInteTest::CheckFileExist(const string& targetFile,
        const vector<string>& fileLists, bool isExist)
{
    if (find(fileLists.begin(), fileLists.end(), targetFile) != fileLists.end())
    {
        ASSERT_EQ(isExist, true) << targetFile;
    }
    else
    {
        ASSERT_EQ(isExist, false) << targetFile;
    }
}

void PartitionResourceProviderInteTest::CheckVersion(const string& versionFile,
        const string& segIdStr, segmentid_t lastSegId)
{
    Version version;
    VersionLoader::GetVersion(versionFile, version);

    vector<segmentid_t> segIds;
    StringUtil::fromString(segIdStr, segIds, ",");

    ASSERT_EQ(segIds, version.GetSegmentVector());
    ASSERT_EQ(lastSegId, version.GetLastSegment());
}

void PartitionResourceProviderInteTest::TestAsyncWriteAttribute()
{
    string tableDirName = "index_table";
    string rootPath = PathUtil::JoinPath(GET_TEST_DATA_PATH(), tableDirName);
    uint32_t schemaId = mNewSchema->GetSchemaVersionId();
    string patchDir = PathUtil::JoinPath(rootPath,
            PartitionPatchIndexAccessor::GetPatchRootDirName(schemaId));
    FileSystemWrapper::MkDirIfNotExist(patchDir);
    IndexPartitionOptions options;
    options.GetOfflineConfig().mergeConfig.mergeIOConfig.enableAsyncWrite = true;
    options.GetOfflineConfig().mergeConfig.mergeIOConfig.writeBufferSize = 10;
    PartitionResourceProviderPtr resourceProvider =
        PartitionResourceProviderFactory::GetInstance()->CreateProvider(
            GET_TEST_DATA_PATH() + "/" + tableDirName, options, "");
    PartitionPatcherPtr patcher =
        resourceProvider->CreatePartitionPatcher(mNewSchema, patchDir);
    PartitionSegmentIteratorPtr segIterator =
        resourceProvider->CreatePartitionSegmentIterator();
    char buf[3] = "10";
    ConstString value(buf, 2);
    while (segIterator->IsValid())
    {
        SegmentData segData = segIterator->GetSegmentData();
        //cout <<  "deal segment " << segData.GetSegmentId() << endl;
        // attribute
        AttributeDataPatcherPtr attrPatcher = patcher->CreateSingleAttributePatcher(
                "new_value", segData.GetSegmentId());
        if (!attrPatcher) {
            segIterator->MoveToNext();
            continue;
        }
        //cout <<  "create patcher " << segData.GetSegmentId() << endl;
        assert(attrPatcher);
        AttributePatchDataWriterPtr writer = attrPatcher->TEST_GetAttributePatchDataWriter();
        //cout <<  "get writer " << segData.GetSegmentId() << endl;
        if (!writer) {
            segIterator->MoveToNext();
            continue;
        }
        //cout <<  "assert writer " << segData.GetSegmentId() << endl;
        file_system::FileWriterPtr fileWriter = writer->TEST_GetDataFileWriter();
        //cout <<  "get file writer " << segData.GetSegmentId() << endl;
        file_system::BufferedFileWriter* fileWriterType =
            dynamic_cast<file_system::BufferedFileWriter*>(fileWriter.get());
        ASSERT_TRUE(fileWriterType->mSwitchBuffer);
        ASSERT_EQ(10, fileWriterType->mSwitchBuffer->GetBufferSize());
        ASSERT_TRUE(fileWriterType->mThreadPool);
        for (uint32_t i = 0; i < attrPatcher->GetTotalDocCount(); i++)
        {
            attrPatcher->AppendFieldValue(value);
        }
        attrPatcher->Close();
        segIterator->MoveToNext();
    }
    FileSystemWrapper::IsExist(patchDir + "/segment_0_level_0/attribute/new_value");
    FileSystemWrapper::IsExist(patchDir + "/segment_0_level_0/attribute/new_value/data");
}

IE_NAMESPACE_END(partition);
