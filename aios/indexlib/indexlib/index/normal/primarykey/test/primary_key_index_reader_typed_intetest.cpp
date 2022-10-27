#include "indexlib/index/normal/primarykey/test/primary_key_index_reader_typed_intetest.h"
#include "indexlib/partition/index_builder.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader_factory.h"
#include "indexlib/index/normal/primarykey/primary_key_hash_table.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/test/fake_partition_data_creator.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/index_base/schema_rewriter.h"
#include "indexlib/common/file_system_factory.h"
#include "indexlib/util/path_util.h"
#include "indexlib/index_base/index_meta/segment_file_list_wrapper.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PrimaryKeyIndexReaderTypedIntetestTest);

PrimaryKeyIndexReaderTypedIntetestTest::PrimaryKeyIndexReaderTypedIntetestTest()
{
}

PrimaryKeyIndexReaderTypedIntetestTest::~PrimaryKeyIndexReaderTypedIntetestTest()
{
}

void PrimaryKeyIndexReaderTypedIntetestTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    OnlineConfig& onlineConfig = mOptions.GetOnlineConfig();
    string jsonStr = "\
        {                                                        \
            \"file_patterns\" : [\".*\"],                        \
            \"load_strategy\" : \"mmap\",                        \
            \"load_strategy_param\" : {                          \
            \"lock\" : true                                      \
            }                                                    \
        }";
    LoadConfigList& loadConfigList = onlineConfig.loadConfigList;
    loadConfigList.Clear();
    LoadConfig loadConfig;
    FromJsonString(loadConfig, jsonStr);
    loadConfigList.PushBack(loadConfig);
}

void PrimaryKeyIndexReaderTypedIntetestTest::CaseTearDown()
{
}

template<typename Key>
bool PrimaryKeyIndexReaderTypedIntetestTest::CheckLookup(
        PrimaryKeyIndexReaderTyped<Key>& reader, const std::string& key,
        const docid_t expectDocId, const docid_t expectLastDocId, bool isNumberHash)
{
#define MY_ASSERT_EQ(expect, actual)            \
    if ((expect) != (actual))                   \
    {                                           \
        EXPECT_EQ(expect, actual);              \
        return false;                           \
    }

    docid_t lastDocId, docid;
    Key keyHash;
    MY_ASSERT_EQ(true, reader.Hash(key, keyHash));

    // docid_t Lookup(const std::string& strKey) const;
    docid = reader.Lookup(key);
    MY_ASSERT_EQ(expectDocId, docid);

    // PostingIterator *Lookup(const common::Term& term...
    common::Term term(key, "");
    PostingIterator *iter = reader.Lookup(term, 1000);
    MY_ASSERT_EQ((expectDocId == INVALID_DOCID), (iter == NULL));
    if (iter)
    {
        MY_ASSERT_EQ((docid_t)expectDocId, iter->SeekDoc(0));
        MY_ASSERT_EQ((docid_t)INVALID_DOCID, iter->SeekDoc(expectDocId));
    }
    IE_POOL_COMPATIBLE_DELETE_CLASS(0, iter);

    // docid_t Lookup(const autil::ConstString& pkStr) const;
    docid = reader.Lookup(autil::ConstString(key));
    MY_ASSERT_EQ(expectDocId, docid);

    // docid_t LookupWithPKHash(const autil::uint128_t& pkHash) const;
    docid = reader.LookupWithPKHash(keyHash);
    MY_ASSERT_EQ(expectDocId, docid);

    // docid_t Lookup(const Key& key) const __ALWAYS_INLINE;
    docid = reader.Lookup(keyHash);
    MY_ASSERT_EQ(expectDocId, docid);

    // docid_t Lookup(const Key& key, docid_t& lastDocId) const;
    docid = reader.Lookup(keyHash, lastDocId);
    MY_ASSERT_EQ(expectDocId, docid);
    MY_ASSERT_EQ(expectLastDocId, lastDocId);

    // docid_t Lookup(const std::string& pkStr, docid_t& lastDocId) const;
    docid = reader.Lookup(key, lastDocId);
    MY_ASSERT_EQ(expectDocId, docid);
    MY_ASSERT_EQ(expectLastDocId, lastDocId);

    // docid_t LookupWithType(const T& key) const;
    if (isNumberHash)
    {
        keyHash = StringUtil::fromString<Key>(key);
        docid = reader.LookupWithType(keyHash);
    }
    else
    {
        docid = reader.LookupWithType(key);
    }
    MY_ASSERT_EQ(expectDocId, docid);

    return true;

#undef MY_ASSERT_EQ
}

//TODO: add ut for last docid
void PrimaryKeyIndexReaderTypedIntetestTest::TestPkValidInPrevSegment()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "string", SFP_PK_INDEX);
    provider.Build("1#1,9#1,delete:1", SFP_OFFLINE);

    PartitionDataPtr onDiskPartData =
        PartitionDataCreator::CreateOnDiskPartitionData(
                GET_FILE_SYSTEM());
    file_system::DirectoryPtr rootDirectory =
        onDiskPartData->GetRootDirectory();
    // make string1=1 in segment_1 valid
    rootDirectory->RemoveFile("segment_3_level_0/deletionmap/data_1");
    auto segment3Dir = rootDirectory->GetDirectory("segment_3_level_0", false);
    segment3Dir->RemoveFile("segment_file_list");
    segment3Dir->RemoveFile("deploy_index");
    SegmentFileListWrapper::Dump(segment3Dir);

    PartitionDataPtr partitionData =
        PartitionDataCreator::CreateOnDiskPartitionData(
                GET_FILE_SYSTEM(), INVALID_VERSION, "", false, true);

    UInt64PrimaryKeyIndexReader pkReader;
    pkReader.Open(provider.GetIndexConfig(), partitionData);
    ASSERT_TRUE(CheckLookup(pkReader, "1", 1, 1));
}

void PrimaryKeyIndexReaderTypedIntetestTest::TestLookup()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "string", SFP_PK_INDEX);
    provider.Build("1#1,9#1,delete:1", SFP_OFFLINE);

    PartitionDataPtr partitionData = provider.GetPartitionData();
    UInt64PrimaryKeyIndexReader pkReader;
    pkReader.Open(provider.GetIndexConfig(), partitionData);

    ASSERT_TRUE(CheckLookup(pkReader, "1", INVALID_DOCID, 3));
    ASSERT_TRUE(CheckLookup(pkReader, "2", INVALID_DOCID, INVALID_DOCID));
}

void PrimaryKeyIndexReaderTypedIntetestTest::TestLookupRealtimeSegment()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "string", SFP_PK_INDEX);
    provider.Build("", SFP_OFFLINE);
    provider.Build("1,2,1", SFP_REALTIME);

    PartitionDataPtr partitionData = provider.GetPartitionData();
    UInt64PrimaryKeyIndexReader pkReader;
    pkReader.Open(provider.GetIndexConfig(), partitionData);

    ASSERT_TRUE(CheckLookup(pkReader, "2", 1, 1));
    ASSERT_TRUE(CheckLookup(pkReader, "1", 2, 2));
}

void PrimaryKeyIndexReaderTypedIntetestTest::TestPkSegmentCombineForRead()
{
    DoTestPkSegmentCombineForRead(pk_sort_array);
    DoTestPkSegmentCombineForRead(pk_hash_table);
}

void PrimaryKeyIndexReaderTypedIntetestTest::DoTestPkSegmentCombineForRead(
        PrimaryKeyIndexType type)
{
    //test only rt or inc index, combine to one segment
    InnerTestCombineSegmentForRead(
            "1#2#3", "", "combine_segments=true,max_doc_count=10",
            "1,0;2,1;3,2", type, 1);
    InnerTestCombineSegmentForRead(
            "", "4#5", "combine_segments=true,max_doc_count=10",
            "4,0;5,1", type, 1);
    InnerTestCombineSegmentForRead(
            "", "4#5#6", "combine_segments=true,max_doc_count=10",
            "4,0;5,1;6,2", type, 1);

    //test inc+rt index combine to 2 segments
    InnerTestCombineSegmentForRead(
            "1#2#3", "4#5#6", "combine_segments=true,max_doc_count=10",
            "1,0;2,1;3,2;4,3;5,4;6,5", type, 2);

    //test some segment not combine
    InnerTestCombineSegmentForRead(
            "1,2,3#4#5", "6,7#8#9#10", "combine_segments=true,max_doc_count=2",
            "1,0;2,1;3,2;4,3;5,4;6,5;7,6;8,7;9,8;10,9", type, 4);

    //test max_doc_count smaller than every segment
    InnerTestCombineSegmentForRead(
            "1,2,3#4#5", "6,7#8#9#10", "combine_segments=true,max_doc_count=0",
            "1,0;2,1;3,2;4,3;5,4;6,5;7,6;8,7;9,8;10,9", type, 6);

    //test merge multi segment with same pk
    InnerTestCombineSegmentForRead(
            "1,2,3#1,2,4#4,5", "5,6,7#6,8#8,9#9,10",
            "combine_segments=true,max_doc_count=10",
            "1,3;2,4;3,2;4,6;5,8;6,11;7,10;8,13;9,15;10,16", type, 2);

    //test merge with doccount not equal with pk count
    InnerTestCombineSegmentForRead(
            "1,2,3#4,4,5", "6,7,8,8#8,9,8,9,10#10",
            "combine_segments=true,max_doc_count=10",
            "1,0;2,1;3,2;4,4;5,5;6,6;7,7;8,12;9,13;10,15", type, 2);

    //test merge with segment whose doc count is 0
    InnerTestCombineSegmentForRead(
            "1#2#delete:2#3#4", "5#6#7#delete:5#delete:7#delete:6",
            "combine_segments=true,max_doc_count=3",
            "1,0;2,-1,1;3,2;4,3;5,-1,4;6,-1,5;7,-1,6", type, 3);
}

//expectResults = "key,docid;key1,docid;..."
void PrimaryKeyIndexReaderTypedIntetestTest::InnerTestCombineSegmentForRead(
        const string& offlineBuildDocs, const string& rtBuildDocs,
        const string& speedUpParam, const string& expectResults,
        PrimaryKeyIndexType type, size_t expectSegmentNum)
{
    TearDown();
    SetUp();
    IndexPartitionOptions options;
    options.GetOnlineConfig().buildConfig.speedUpPrimaryKeyReader = true;
    options.GetOnlineConfig().buildConfig.speedUpPrimaryKeyReaderParam = speedUpParam;
    options.GetOfflineConfig().buildConfig.speedUpPrimaryKeyReader = true;
    options.GetOfflineConfig().buildConfig.speedUpPrimaryKeyReaderParam = speedUpParam;
    SingleFieldPartitionDataProvider provider(options);
    provider.Init(mRootDir, "int32", SFP_PK_INDEX);
    PrimaryKeyIndexConfigPtr pkConfig =
        DYNAMIC_POINTER_CAST(PrimaryKeyIndexConfig, provider.GetIndexConfig());
    pkConfig->SetPrimaryKeyHashType(pk_number_hash);
    pkConfig->SetPrimaryKeyLoadParam(
            PrimaryKeyLoadStrategyParam::HASH_TABLE, true, speedUpParam);
    pkConfig->SetPrimaryKeyIndexType(type);

    provider.Build(offlineBuildDocs, SFP_OFFLINE);
    if (!rtBuildDocs.empty())
    {
        provider.Build(rtBuildDocs, SFP_REALTIME);
    }

    PartitionDataPtr partitionData = provider.GetPartitionData();
    UInt64PrimaryKeyIndexReader pkReader;
    pkReader.Open(pkConfig, partitionData);
    ASSERT_EQ(expectSegmentNum, pkReader.mSegmentList.size());

    vector<vector<string> > values;
    StringUtil::fromString(expectResults, values, ",", ";");
    for (size_t i = 0; i < values.size(); i++)
    {
        ASSERT_TRUE(values[i].size() == 2 || values[i].size() ==3);
        const string& key = values[i][0];
        docid_t expectDocId = StringUtil::fromString<docid_t>(values[i][1]);
        docid_t expectLastDocId = expectDocId;
        if (values[i].size() == 3)
        {
            expectLastDocId = StringUtil::fromString<docid_t>(values[i][2]);
        }
        ASSERT_TRUE(CheckLookup(pkReader, key, expectDocId, expectLastDocId, true))
            << key << "" << expectDocId;
    }
}

void PrimaryKeyIndexReaderTypedIntetestTest::TestLookupIncAndRtSegment()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "string", SFP_PK_INDEX);
    provider.Build("1#1,1,9", SFP_OFFLINE);
    provider.Build("5", SFP_REALTIME);

    PartitionDataPtr partitionData = provider.GetPartitionData();
    UInt64PrimaryKeyIndexReader pkReader;
    pkReader.Open(provider.GetIndexConfig(), partitionData);

    ASSERT_TRUE(CheckLookup(pkReader, "5", 4, 4));
    ASSERT_TRUE(CheckLookup(pkReader, "1", 2, 2));
    ASSERT_EQ((docid_t)4, pkReader.LookupWithType(string("5")));
    ASSERT_EQ((docid_t)2, pkReader.LookupWithType((uint32_t)1));
}

void PrimaryKeyIndexReaderTypedIntetestTest::TestEstimateLoadSize()
{
    //test without attribute: pkSize = 1 * 12
    InnerTestEstimateLoadSize("1", "2", true, false, pk_sort_array, false, 12);
    //test without attribute: pkSize = (headerSize(24) +
    //                                  hashBucketSize(7 * 4) + pkData(12)
    InnerTestEstimateLoadSize("1", "2", true, false, pk_hash_table, true, 64);
    //test load with mmap unlock
    InnerTestEstimateLoadSize("1", "2", true, false, pk_hash_table, true, 0, false);
}

void PrimaryKeyIndexReaderTypedIntetestTest::TestEstimateLoadSizeWithPkSegmentMerge()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "int64", SFP_PK_INDEX);
    PrimaryKeyIndexConfigPtr indexConfig = DYNAMIC_POINTER_CAST(
            PrimaryKeyIndexConfig, provider.GetIndexConfig());
    indexConfig->SetPrimaryKeyLoadParam(
            PrimaryKeyLoadStrategyParam::HASH_TABLE, false,
            "combine_segments=true,max_doc_count=100");
    provider.Build("1#2#3#4", SFP_OFFLINE);
    Version version;
    version.SetVersionId(0);
    //test load with hash: pkSize = headerSize(24) +
    //                              hashBucketSize(7 * 4) +
    //                              pkData(4 * 12) + pkAttribute(4 * 8)
    IndexlibFileSystemPtr fileSystem =
        common::FileSystemFactory::Create(mRootDir, "", mOptions, NULL);
    PartitionDataPtr partitionData =
        PartitionDataCreator::CreateOnDiskPartitionData(fileSystem);
    IndexReaderPtr indexReader(IndexReaderFactory::CreateIndexReader(
                    indexConfig->GetIndexType()));
    ASSERT_EQ((size_t)132, indexReader->EstimateLoadSize(
                    partitionData, indexConfig, version));
    indexReader->Open(indexConfig, partitionData);
    //test after load estimate zero pk and attribute 32 bytes
    ASSERT_EQ((size_t)32, indexReader->EstimateLoadSize(
                    partitionData, indexConfig, version));
    string mergeFilePath = PathUtil::JoinPath(
            mRootDir, "segment_3_level_0/index/pk_index/slice_data_0_1_2_3");
    ASSERT_EQ((size_t)100, fileSystem->GetFileLength(mergeFilePath));
}

void PrimaryKeyIndexReaderTypedIntetestTest::InnerTestEstimateLoadSize(
        const string& docStr, const string& newDocStr,
        bool isPkHash64, bool hasPkAttribute,
        PrimaryKeyIndexType type, bool loadWithPkHash,
        size_t expectSize, bool isLock)
{
    TearDown();
    SetUp();
    SFP_IndexType indexType = isPkHash64 ? SFP_PK_INDEX : SFP_PK_128_INDEX;
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "int64", indexType);
    PrimaryKeyIndexConfigPtr indexConfig = DYNAMIC_POINTER_CAST(
            PrimaryKeyIndexConfig, provider.GetIndexConfig());
    indexConfig->SetPrimaryKeyAttributeFlag(hasPkAttribute);
    indexConfig->SetPrimaryKeyIndexType(type);
    if (loadWithPkHash)
    {
        indexConfig->SetPrimaryKeyLoadParam(
                PrimaryKeyLoadStrategyParam::HASH_TABLE, false);
    }
    provider.Build(docStr, SFP_OFFLINE);

    RewriteLoadConfig(mOptions, isLock);
    IndexlibFileSystemPtr fileSystem =
        common::FileSystemFactory::Create(mRootDir, "", mOptions, NULL);

    PartitionDataPtr partitionData =
        PartitionDataCreator::CreateOnDiskPartitionData(fileSystem);
    PrimaryKeyIndexReaderPtr pkReader;
    if (isPkHash64)
    {
        pkReader.reset(new UInt64PrimaryKeyIndexReader());
    }
    else
    {
        pkReader.reset(new UInt128PrimaryKeyIndexReader());
    }
    pkReader->Open(indexConfig, partitionData);

    provider.Build(newDocStr, SFP_OFFLINE);
    PartitionDataPtr newPartitionData =
        PartitionDataCreator::CreateOnDiskPartitionData(fileSystem);
    IndexReaderPtr indexReader(IndexReaderFactory::CreateIndexReader(
                    indexConfig->GetIndexType()));
    ASSERT_EQ(expectSize, indexReader->EstimateLoadSize(
                    newPartitionData, indexConfig, partitionData->GetVersion()));
    pkReader.reset();
}

void PrimaryKeyIndexReaderTypedIntetestTest::TestLookupForLastDocId()
{
    DoTestLookupForLastDocId(pk_sort_array);
    DoTestLookupForLastDocId(pk_hash_table);
}

// offlineBuildDocs, rtBuildDocs, speedUpParam, expectResults, expectSegmentNum)
void PrimaryKeyIndexReaderTypedIntetestTest::DoTestLookupForLastDocId(PrimaryKeyIndexType type)
{
     // reorder
    InnerTestCombineSegmentForRead("0,1#2,3,4", "", "",
                                   "0,0,0;1,1,1;2,2,2;3,3,3;4,4,4;", type, 2);
    // delete
    InnerTestCombineSegmentForRead("1,delete:1#2#3", "", "", "1,-1,0;2,1;3,2", type, 3);
    // dup 1
    InnerTestCombineSegmentForRead("1#1,3,4#1", "", "", "1,4,4;", type, 3);
    // dup and delete
    InnerTestCombineSegmentForRead("1#1,3,4#1,delete:1", "", "", "1,-1,4;", type, 3);
    // delete all
    InnerTestCombineSegmentForRead("1#2,3,4#delete:1,delete:2,delete:3,delete:4",
                                   "", "", "1,-1,0;2,-1,1;3,-1,2;4,-1,3", type, 2);
}

void PrimaryKeyIndexReaderTypedIntetestTest::TestBuildAndLoadHashPk()
{
    DoTestBuildAndLoadHashPk(it_primarykey64);
    DoTestBuildAndLoadHashPk(it_primarykey128);
}

void PrimaryKeyIndexReaderTypedIntetestTest::DoTestBuildAndLoadHashPk(IndexType type)
{
    TearDown();
    SetUp();
    string field = "string1:string";
    string index = "pk:primarykey64:string1";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "", "");
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();

    PrimaryKeyIndexConfigPtr pkIndexConfig = DYNAMIC_POINTER_CAST(
        PrimaryKeyIndexConfig, indexSchema->GetIndexConfig("pk"));
    pkIndexConfig->SetPrimaryKeyIndexType(pk_hash_table);
    pkIndexConfig->SetIndexType(type);
    IndexPartitionOptions options;

    string docString = "cmd=add,string1=hello;cmd=add,string1=kitty;";

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "pk:hello", "docid=0"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:kitty", "docid=1"));
}

void PrimaryKeyIndexReaderTypedIntetestTest::TestLoadHashPkWithLoadConfig()
{
    string field = "string1:string";
    string index = "pk:primarykey64:string1";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "", "");
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();

    PrimaryKeyIndexConfigPtr pkIndexConfig = DYNAMIC_POINTER_CAST(
        PrimaryKeyIndexConfig, indexSchema->GetIndexConfig("pk"));
    pkIndexConfig->SetPrimaryKeyIndexType(pk_hash_table);
    pkIndexConfig->SetPrimaryKeyAttributeFlag(false);

    IndexPartitionOptions options;
    {
        PartitionStateMachine psm;
        string docString = "cmd=add,string1=hello;cmd=add,string1=kitty;";
        ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    }

    IndexlibFileSystemPtr fileSystem =
        common::FileSystemFactory::Create(mRootDir, "", options, NULL);
    PartitionDataPtr partitionData =
        PartitionDataCreator::CreateOnDiskPartitionData(fileSystem);

    SchemaRewriter::Rewrite(schema, options, mRootDir);
    PrimaryKeyIndexReaderTyped<uint64_t> pkReader;
    pkReader.Open(pkIndexConfig, partitionData);

    // pk in single segment will be loaded as load_config
    StorageMetrics localStorageMetrics =
        fileSystem->GetFileSystemMetrics().GetLocalStorageMetrics();
    EXPECT_EQ((int64_t)0, localStorageMetrics.GetFileLength(FSFT_SLICE));
    int64_t pkDataFileLen = PrimaryKeyHashTable<uint64_t>::CalculateMemorySize(2, 2);
    EXPECT_EQ(pkDataFileLen,  localStorageMetrics.GetMmapNonlockFileLength());
}

void PrimaryKeyIndexReaderTypedIntetestTest::RewriteLoadConfig(
        IndexPartitionOptions& options, bool isLock)
{
    string jsonStr = "                                                 \
    {                                                                  \
    \"load_config\" :                                                  \
    [{                                                                 \
        \"file_patterns\" : [\".*\"],                                  \
        \"load_strategy\" : \"mmap\",                                  \
        \"load_strategy_param\" : {                                    \
            \"lock\" : true                                            \
        }                                                              \
    }]                                                                 \
    }                                                                  \
    ";
    if (!isLock)
    {
        jsonStr.replace(jsonStr.find("true"), strlen("true"), "false");
    }
    LoadConfigList loadConfigList;
    FromJsonString(loadConfigList, jsonStr);
    options.GetOnlineConfig().loadConfigList = loadConfigList;
}

void PrimaryKeyIndexReaderTypedIntetestTest::TestPkWithMurmurHash()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "string", SFP_PK_INDEX);
    PrimaryKeyIndexConfigPtr pkConfig =
        DYNAMIC_POINTER_CAST(PrimaryKeyIndexConfig, provider.GetIndexConfig());
    pkConfig->SetPrimaryKeyHashType(pk_murmur_hash);
    provider.Build("1#1,9", SFP_OFFLINE);

    PartitionDataPtr partitionData = provider.GetPartitionData();
    UInt64PrimaryKeyIndexReader pkReader;
    pkReader.Open(pkConfig, partitionData);

    ASSERT_TRUE(CheckLookup(pkReader, "1", 1, 1));
    ASSERT_TRUE(CheckLookup(pkReader, "9", 2, 2));
}

void PrimaryKeyIndexReaderTypedIntetestTest::TestMultiInMemSegments()
{
    IndexPartitionOptions options;
    string field = "pk:uint64:pk;string1:string;text1:text;long1:uint32;";
    string index = "pk:primarykey64:pk;index1:string:string1;pack1:pack:text1;";
    config::IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(field, index, "long1", "");

    FakePartitionDataCreator pdc;
    pdc.Init(schema, options, GET_TEST_DATA_PATH(), false);

    string fullDocs = "cmd=add,pk=1,string1=A,long1=1;"
                      "cmd=add,pk=2,string1=B,long1=2;";
    pdc.AddFullDocs(fullDocs);

    string builtRtDocs = "cmd=add,pk=3,string1=B,long1=3,ts=0;"
                         "cmd=add,pk=4,string1=A,long1=4,ts=0;";
    pdc.AddBuiltSegment(builtRtDocs);

    string inMemSegment1 = "cmd=add,pk=5,string1=B,long1=5,ts=0;"
                           "cmd=add,pk=6,string1=A,long1=6,ts=0;";
    pdc.AddInMemSegment(inMemSegment1);

    string inMemSegment2 = "cmd=add,pk=7,string1=B,long1=7,ts=0;"
                           "cmd=add,pk=8,string1=A,long1=8,ts=0;"
                           "cmd=add,pk=9,string1=C,long1=9,ts=0;";
    pdc.AddInMemSegment(inMemSegment2);

    string buildingDocs = "cmd=add,pk=10,string1=A,long1=10,ts=0;"
                          "cmd=add,pk=11,string1=C,long1=11,ts=0;";
    pdc.AddBuildingData(buildingDocs);

    PartitionDataPtr partData = pdc.CreatePartitionData(false);
    UInt64PrimaryKeyIndexReader pkReader;
    pkReader.Open(schema->GetIndexSchema()->GetPrimaryKeyIndexConfig(), partData);
    ASSERT_TRUE(CheckLookup(pkReader, "1", 0, 0));
    ASSERT_TRUE(CheckLookup(pkReader, "2", 1, 1));
    ASSERT_TRUE(CheckLookup(pkReader, "3", 2, 2));
    ASSERT_TRUE(CheckLookup(pkReader, "4", 3, 3));
    ASSERT_TRUE(CheckLookup(pkReader, "5", 4, 4));
    ASSERT_TRUE(CheckLookup(pkReader, "6", 5, 5));
    ASSERT_TRUE(CheckLookup(pkReader, "7", 6, 6));
    ASSERT_TRUE(CheckLookup(pkReader, "8", 7, 7));
    ASSERT_TRUE(CheckLookup(pkReader, "9", 8, 8));
    ASSERT_TRUE(CheckLookup(pkReader, "10", 9, 9));
    ASSERT_TRUE(CheckLookup(pkReader, "11", 10, 10));

    AttributeReaderPtr pkAttrReader = pkReader.GetPKAttributeReader();
    PrimaryKeyIndexConfigPtr pkConfig = DYNAMIC_POINTER_CAST(
            PrimaryKeyIndexConfig, schema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    CheckPkAttribute(pkAttrReader, 0, "1", pkConfig);
    CheckPkAttribute(pkAttrReader, 1, "2", pkConfig);
    CheckPkAttribute(pkAttrReader, 2, "3", pkConfig);
    CheckPkAttribute(pkAttrReader, 3, "4", pkConfig);
    CheckPkAttribute(pkAttrReader, 4, "5", pkConfig);
    CheckPkAttribute(pkAttrReader, 5, "6", pkConfig);
    CheckPkAttribute(pkAttrReader, 6, "7", pkConfig);
    CheckPkAttribute(pkAttrReader, 7, "8", pkConfig);
    CheckPkAttribute(pkAttrReader, 8, "9", pkConfig);
    CheckPkAttribute(pkAttrReader, 9, "10", pkConfig);
    CheckPkAttribute(pkAttrReader, 10, "11", pkConfig);
}

void PrimaryKeyIndexReaderTypedIntetestTest::CheckPkAttribute(
        const AttributeReaderPtr& attrReader, docid_t docId, const string& pkStr,
        const PrimaryKeyIndexConfigPtr& pkConfig)
{
    typedef PrimaryKeyAttributeReader<uint64_t> PKAttributeReader;
    typedef std::tr1::shared_ptr<PKAttributeReader> PKAttributeReaderPtr;
    PKAttributeReaderPtr pkAttrReader =
        DYNAMIC_POINTER_CAST(PKAttributeReader, attrReader);
    ASSERT_TRUE(pkAttrReader);

    KeyHasherPtr hashFunc(KeyHasherFactory::CreatePrimaryKeyHasher(
            pkConfig->GetFieldConfig()->GetFieldType(),
            pkConfig->GetPrimaryKeyHashType()));
    uint64_t keyHash;
    hashFunc->GetHashKey(pkStr.data(), pkStr.size(), keyHash);

    uint64_t pkAttrValue;
    ASSERT_TRUE(pkAttrReader->Read(docId, pkAttrValue));
    ASSERT_EQ(keyHash, pkAttrValue);
}

IE_NAMESPACE_END(index);
