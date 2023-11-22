#include "indexlib/partition/segment/test/kkv_segment_writer_unittest.h"

#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/document/document_rewriter/pack_attribute_rewriter.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/index/kkv/building_kkv_segment_iterator.h"
#include "indexlib/index/kkv/building_kkv_segment_reader.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/schema_rewriter.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/util/PrimeNumberTable.h"

using namespace std;
using namespace autil;
using namespace indexlib::index;
using namespace indexlib::common;
using namespace indexlib::util;
using namespace indexlib::document;
using namespace indexlib::config;
using namespace indexlib::test;
using namespace indexlib::file_system;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {

IE_LOG_SETUP(segment, KKVSegmentWriterTest);

KKVSegmentWriterTest::KKVSegmentWriterTest() {}

KKVSegmentWriterTest::~KKVSegmentWriterTest() {}

void KKVSegmentWriterTest::CaseSetUp()
{
    string fields = "single_int8:int8;single_int16:int16;pkey:string;skey:int32";
    mSchema = SchemaMaker::MakeKKVSchema(fields, "pkey", "skey", "single_int8;single_int16");
    const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
    mKkvConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
    const_cast<KKVIndexFieldInfo&>(mKkvConfig->GetSuffixFieldInfo()).skipListThreshold = 200;

    PKeyTableType tableType = GET_CASE_PARAM();
    KKVIndexPreference::HashDictParam param(PrefixKeyTableType2Str(tableType));
    mKkvConfig->GetIndexPreference().SetHashDictParam(param);

    mFormatter.Init(mKkvConfig->GetValueConfig()->CreatePackAttributeConfig());
    mQuotaControl.reset(new util::QuotaControl(64 * 1024 * 1024));
    file_system::FileSystemOptions fsOptions;
    fsOptions.isOffline = false;
    fsOptions.outputStorage = file_system::FSST_MEM;
    RESET_FILE_SYSTEM("ut", false, fsOptions);
}

void KKVSegmentWriterTest::CaseTearDown() {}

void KKVSegmentWriterTest::CheckReader(const InMemorySegmentReaderPtr& reader, const string& pkeyStr,
                                       const string& skeyStr, const string& expectedValuesStr,
                                       const string& expectedTsStr)
{
    typedef uint64_t SKeyType;
    ASSERT_TRUE(reader);
    KKVSegmentReaderPtr kkvSegReader = reader->GetKKVSegmentReader();
    auto prefixKeyHasherType = KVIndexConfig::GetHashFunctionType(ft_string, true);
    uint64_t pkey = 0;
    StringView pkeyCS(pkeyStr);
    GetHashKey(prefixKeyHasherType, pkeyCS, pkey);

    KKVIndexOptions options;
    KKVIndexConfigPtr kkvConfig(new KKVIndexConfig("", it_kkv));
    ValueConfigPtr valueConfig(new ValueConfig());
    kkvConfig->SetValueConfig(valueConfig);
    options.kkvConfig = kkvConfig;

    auto buildingSegReader = DYNAMIC_POINTER_CAST(BuildingKKVSegmentReader<SKeyType>, kkvSegReader);

    KKVSegmentIterator* kkvIter =
        buildingSegReader->template Lookup<index::BuildingKKVSegmentIterator<SKeyType>>(pkey, &mPool, &options);
    ASSERT_TRUE(kkvIter);

    vector<string> skeys;
    StringUtil::fromString(skeyStr, skeys, ";");
    vector<string> expectedValues;
    StringUtil::fromString(expectedValuesStr, expectedValues, ";");
    vector<string> expectedTs;
    StringUtil::fromString(expectedTsStr, expectedTs, ";");

    vector<pair<SKeyType, pair<int16_t, uint32_t>>> nodes;
    auto suffixKeyHasherType = KVIndexConfig::GetHashFunctionType(ft_int32, true);
    for (size_t i = 0; i < skeys.size(); ++i) {
        SKeyType skey;
        StringView skeyCS(skeys[i]);
        dictkey_t key = 0;
        GetHashKey(suffixKeyHasherType, skeyCS, key);
        skey = (SKeyType)key;
        int16_t value;
        StringUtil::fromString(expectedValues[i], value);
        uint32_t ts;
        StringUtil::fromString(expectedTs[i], ts);
        nodes.push_back(make_pair(skey, make_pair(value, ts)));
    }
    sort(nodes.begin(), nodes.end());

    assert(skeys.size() == expectedValues.size());
    assert(skeys.size() == expectedTs.size());
    string attrName = "single_int16";
    AttributeReferenceTyped<int16_t>* attrRef = mFormatter.GetAttributeReferenceTyped<int16_t>(attrName);
    for (size_t i = 0; i < skeys.size(); ++i) {
        SKeyType skey = nodes[i].first;
        ASSERT_TRUE(kkvIter->IsValid());
        bool isDeleted = false;
        uint64_t skeyValue = 0;
        kkvIter->GetCurrentSkey(skeyValue, isDeleted);
        ASSERT_EQ((uint64_t)skey, skeyValue);
        ASSERT_FALSE(isDeleted);

        uint32_t actualTs = kkvIter->GetCurrentTs();
        ASSERT_EQ(nodes[i].second.second, actualTs);

        StringView value;
        kkvIter->GetCurrentValue(value);
        ASSERT_TRUE(!value.empty());
        ASSERT_TRUE(attrRef);
        int16_t actualValue = 0;
        attrRef->GetValue(value.data(), actualValue);
        ASSERT_EQ(nodes[i].second.first, actualValue);

        kkvIter->MoveToNext();
    }
    ASSERT_FALSE(kkvIter->IsValid());
    IE_POOL_COMPATIBLE_DELETE_CLASS(&mPool, kkvIter);
}

void KKVSegmentWriterTest::BuildDocs(KKVSegmentWriter<uint64_t>& writer, const string& docStr)
{
    auto docs = test::DocumentCreator::CreateKVDocuments(mSchema, docStr);
    for (size_t i = 0; i < docs.size(); i++) {
        writer.AddDocument(docs[i]);
    }
}

size_t KKVSegmentWriterTest::CalcucateSize(size_t docCount, size_t pkeyCount, const IndexPartitionOptions& options,
                                           const std::shared_ptr<indexlib::framework::SegmentMetrics>& segmentMetrics,
                                           bool useSwapMmapFile)
{
    size_t dataSize = 4; // 1byte header, 1byte single int8, 2bytes single int16
    size_t skeyNodeSize = sizeof(SuffixKeyWriter<uint64_t>::SKeyNode);
    size_t pkeySize = 0;

    index::PrefixKeyResourceAssigner assigner(mKkvConfig, 0, 1);
    assigner.Init(QuotaControlPtr(), options);
    size_t distPkeyCountAdvisor = assigner.EstimatePkeyCount(segmentMetrics);
    string hashType = mKkvConfig->GetIndexPreference().GetHashDictParam().GetHashType();
    if (hashType == "separate_chain") {
        size_t hashTableBucketSize =
            SeparateChainPrefixKeyTable<index::SKeyListInfo>::GetRecommendBucketCount(distPkeyCountAdvisor) *
            sizeof(uint32_t);
        size_t pkeyNodeSize = sizeof(common::SeparateChainHashTable<PKeyType, index::SKeyListInfo>::KeyNode);
        pkeySize = pkeyNodeSize * pkeyCount + hashTableBucketSize;
    } else {
        assert(hashType == "dense");
        pkeySize =
            ClosedHashPrefixKeyTable<SKeyListInfo, PKT_DENSE>::CalculateMemoryUse(PKOT_RW, distPkeyCountAdvisor, 50);
    }
    if (useSwapMmapFile) {
        return skeyNodeSize * docCount + pkeySize;
    }
    return (dataSize + skeyNodeSize) * docCount + pkeySize;
}

void KKVSegmentWriterTest::TestSimpleProcess()
{
    string docStr = "cmd=add,single_int8=-8,single_int16=-16,pkey=1,skey=2,ts=1000000;"
                    "cmd=add,single_int8=-9,single_int16=-17,pkey=1,skey=3,ts=2000000;";

    IndexPartitionOptions options;
    KKVSegmentWriter<uint64_t> segWriter(mSchema, options);
    BuildingSegmentData segmentData;
    segmentData.SetSegmentId(0);
    segmentData.SetSegmentDirName("test_segment");
    MAKE_SEGMENT_DIRECTORY(0);
    segmentData.Init(GET_SEGMENT_DIRECTORY(), false);

    std::shared_ptr<indexlib::framework::SegmentMetrics> metrics(new framework::SegmentMetrics);
    segWriter.Init(segmentData, metrics, mQuotaControl);
    // ASSERT_EQ(CalcucateSize(0, 0, options, metrics), segWriter.GetTotalMemoryUse());
    BuildDocs(segWriter, docStr);

    SegmentInfoPtr segmentInfo = segWriter.GetSegmentInfo();
    ASSERT_EQ((size_t)2, segmentInfo->docCount);
    // ASSERT_EQ(CalcucateSize(2, 1, options, metrics), segWriter.GetTotalMemoryUse());

    InMemorySegmentReaderPtr reader = segWriter.CreateInMemSegmentReader();
    CheckReader(reader, "1", "2;3", "-16;-17", "1;2");

    docStr = "cmd=add,single_int8=9,single_int16=17,pkey=1,skey=3,ts=3000000;"
             "cmd=add,single_int8=9,single_int16=16,pkey=1,skey=2,ts=3000000;"
             "cmd=add,single_int8=9,single_int16=18,pkey=1,skey=4,ts=3000000;";
    BuildDocs(segWriter, docStr);
    CheckReader(reader, "1", "2;3;4", "16;17;18", "3;3;3");
    // ASSERT_EQ(CalcucateSize(5, 1, options, metrics), segWriter.GetTotalMemoryUse());

    DirectoryPtr directory = MAKE_SEGMENT_DIRECTORY(0);
    segWriter.EndSegment();
    vector<std::unique_ptr<common::DumpItem>> dumpItems;
    segWriter.CreateDumpItems(directory, dumpItems);

    ASSERT_EQ((size_t)1, dumpItems.size());
    auto item = dumpItems[0].release();
    item->process(&mPool);
    delete item;

    DirectoryPtr indexDir = directory->GetDirectory(INDEX_DIR_NAME, false);
    DirectoryPtr dumpDir = indexDir->GetDirectory(mKkvConfig->GetIndexName(), false);
    ASSERT_TRUE(dumpDir);
    ASSERT_TRUE(dumpDir->IsExist(PREFIX_KEY_FILE_NAME));
    ASSERT_TRUE(dumpDir->IsExist(SUFFIX_KEY_FILE_NAME));
    ASSERT_TRUE(dumpDir->IsExist(KKV_VALUE_FILE_NAME));
}

void KKVSegmentWriterTest::TestNeedForceDumpForDenseTable()
{
    string docStr = "cmd=add,single_int8=-8,single_int16=-16,pkey=1,skey=2,ts=1000000;"
                    "cmd=add,single_int8=-9,single_int16=-17,pkey=2,skey=3,ts=2000000;";

    IndexPartitionOptions options;
    KKVSegmentWriter<uint64_t> segWriter(mSchema, options);
    BuildingSegmentData segmentData;
    segmentData.SetSegmentId(0);
    segmentData.SetSegmentDirName("test_segment");
    MAKE_SEGMENT_DIRECTORY(0);
    segmentData.Init(GET_SEGMENT_DIRECTORY(), false);

    std::shared_ptr<indexlib::framework::SegmentMetrics> metrics(new framework::SegmentMetrics);
    string groupName = SegmentWriter::GetMetricsGroupName(0);
    metrics->Set<size_t>(groupName, index::KKV_SEGMENT_MEM_USE, 1000);
    metrics->Set<size_t>(groupName, index::KKV_PKEY_MEM_USE, 1);

    metrics->Set<size_t>(groupName, index::KKV_SKEY_MEM_USE, 0);
    metrics->Set<size_t>(groupName, index::KKV_VALUE_MEM_USE, 0);
    metrics->Set<double>(groupName, index::KKV_SKEY_VALUE_MEM_RATIO, 0);

    size_t pkeySize = ClosedHashPrefixKeyTable<SKeyListInfo, PKT_DENSE>::CalculateMemoryUse(PKOT_RW, 2, 50);
    size_t total = pkeySize * 1000 + 1024;
    mQuotaControl.reset(new util::QuotaControl(total));

    segWriter.Init(segmentData, metrics, mQuotaControl);
    BuildDocs(segWriter, docStr);

    bool needForceDump = (GET_CASE_PARAM() == PKT_DENSE);
    ASSERT_EQ(needForceDump, segWriter.NeedForceDump());
}

void KKVSegmentWriterTest::TestNeedDumpWhenNoSpace()
{
    string fields = "single_int8:int8;single_int16:int16;mstr:string;pkey:string;skey:int32";
    mSchema = SchemaMaker::MakeKKVSchema(fields, "pkey", "skey", "single_int8;single_int16;mstr");
    const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
    mKkvConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());

    PKeyTableType tableType = GET_CASE_PARAM();
    KKVIndexPreference::HashDictParam param(PrefixKeyTableType2Str(tableType));
    mKkvConfig->GetIndexPreference().SetHashDictParam(param);

    mFormatter.Init(mKkvConfig->GetValueConfig()->CreatePackAttributeConfig());
    mQuotaControl.reset(new util::QuotaControl(1024 * 1024 * 1024));

    string longValue(800 * 1000, 'a');
    string docString = "cmd=add,single_int8=-8,single_int16=-16,mstr=" + longValue + ",pkey=1,skey=2,ts=1000000;";

    IndexPartitionOptions options;
    options.SetIsOnline(true);
    options.GetOnlineConfig().useSwapMmapFile = true;
    assert(options.GetOnlineConfig().useSwapMmapFile);
    options.GetOnlineConfig().maxSwapMmapFileSize = 1;

    IndexFormatVersion indexVersion(INDEX_FORMAT_VERSION);
    SchemaRewriter::RewriteForKV(mSchema, options, indexVersion);
    options.SetIsOnline(true);
    PartitionStateMachine psm;
    psm.Init(mSchema, options, GET_TEMP_DATA_PATH());

    ASSERT_TRUE(psm.Transfer(BUILD_RT, docString, "", ""));
    OnlinePartitionPtr onlinePartition = DYNAMIC_POINTER_CAST(OnlinePartition, psm.GetIndexPartition());
    InMemorySegmentPtr inMemorySegment1 = onlinePartition->GetPartitionData()->GetInMemorySegment();
    segmentid_t id1 = inMemorySegment1->GetSegmentData().GetSegmentId();

    ASSERT_TRUE(psm.Transfer(BUILD_RT, docString, "", ""));
    InMemorySegmentPtr inMemorySegment2 = onlinePartition->GetPartitionData()->GetInMemorySegment();
    segmentid_t id2 = inMemorySegment2->GetSegmentData().GetSegmentId();
    ASSERT_FALSE(id1 == id2);
    ASSERT_TRUE(id1 + 1 == id2);
    ASSERT_FALSE(psm.Transfer(QUERY, "", "1", ""));

    longValue += longValue;
    docString = "cmd=add,single_int8=-8,single_int16=-16,mstr=" + longValue + ",pkey=2,skey=2,ts=1000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", ""));
}

void KKVSegmentWriterTest::TestMmapFileDump()
{
    string fields = "single_int8:int8;single_int16:int16;mstr:string;pkey:string;skey:int32";
    mSchema = SchemaMaker::MakeKKVSchema(fields, "pkey", "skey", "single_int8;single_int16;mstr");
    const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
    mKkvConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());

    PKeyTableType tableType = GET_CASE_PARAM();
    KKVIndexPreference::HashDictParam param(PrefixKeyTableType2Str(tableType));
    mKkvConfig->GetIndexPreference().SetHashDictParam(param);

    string longValue(800 * 1000, 'a');
    string docString = "cmd=add,single_int8=-8,single_int16=-16,mstr=" + longValue + ",pkey=1,skey=2,ts=1000000;";

    IndexPartitionOptions options;
    options.SetIsOnline(true);
    options.GetOnlineConfig().useSwapMmapFile = true;
    assert(options.GetOnlineConfig().useSwapMmapFile);
    options.GetOnlineConfig().maxSwapMmapFileSize = 1;
    options.GetOnlineConfig().enableAsyncCleanResource = false;
    IndexFormatVersion indexVersion(INDEX_FORMAT_VERSION);
    SchemaRewriter::RewriteForKV(mSchema, options, indexVersion);
    options.SetIsOnline(true);
    PartitionStateMachine psm;
    psm.Init(mSchema, options, GET_TEMP_DATA_PATH());

    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, docString, "", ""));
    RESET_FILE_SYSTEM();
    string path = "rt_index_partition/segment_1073741824_level_0/index/pkey/value";
    ASSERT_TRUE(GET_PARTITION_DIRECTORY()->IsExist(path));

    OnlinePartitionPtr onlinePart = DYNAMIC_POINTER_CAST(OnlinePartition, psm.GetIndexPartition());
    assert(onlinePart);

    DirectoryPtr rootDir = onlinePart->GetRootDirectory();
    assert(rootDir);

    FileStat fileStat;
    ASSERT_EQ(FSEC_OK, rootDir->GetFileSystem()->TEST_GetFileStat(path, fileStat));
    ASSERT_EQ(FSFT_MMAP, fileStat.fileType);
}

void KKVSegmentWriterTest::TestConfigWorkForSuffixKeyWriter()
{
    IndexPartitionOptions options;
    KKVSegmentWriter<uint64_t> segWriter(mSchema, options);
    BuildingSegmentData segmentData;
    segmentData.SetSegmentId(0);
    segmentData.SetSegmentDirName("test_segment");
    MAKE_SEGMENT_DIRECTORY(0);
    segmentData.Init(GET_SEGMENT_DIRECTORY(), false);

    std::shared_ptr<indexlib::framework::SegmentMetrics> metrics(new framework::SegmentMetrics);
    string groupName = SegmentWriter::GetMetricsGroupName(0);
    metrics->Set<size_t>(groupName, index::KKV_SEGMENT_MEM_USE, 1000);
    metrics->Set<size_t>(groupName, index::KKV_PKEY_MEM_USE, 1);

    metrics->Set<size_t>(groupName, index::KKV_SKEY_MEM_USE, 0);
    metrics->Set<size_t>(groupName, index::KKV_VALUE_MEM_USE, 0);
    metrics->Set<double>(groupName, index::KKV_SKEY_VALUE_MEM_RATIO, 0);

    size_t pkeySize = ClosedHashPrefixKeyTable<SKeyListInfo, PKT_DENSE>::CalculateMemoryUse(PKOT_RW, 2, 100);
    size_t total = pkeySize * 1000 + 1024;
    mQuotaControl.reset(new util::QuotaControl(total));
    segWriter.Init(segmentData, metrics, mQuotaControl);
    KKVSegmentWriter<uint64_t>::SKeyWriterPtr mSKeyWriter = segWriter.mSKeyWriter;
    ASSERT_EQ((size_t)200, mSKeyWriter->GetLongTailThreshold());
}

void KKVSegmentWriterTest::TestSwapMmapFileWriter()
{
    string docStr = "cmd=add,single_int8=-8,single_int16=-16,pkey=1,skey=2,ts=1000000;"
                    "cmd=add,single_int8=-9,single_int16=-17,pkey=1,skey=3,ts=2000000;";
    IndexPartitionOptions options;
    options.SetIsOnline(true);
    // options.GetOnlineConfig().buildConfig.buildTotalMemory = 1024 * 1024;
    options.GetOnlineConfig().useSwapMmapFile = true;
    assert(options.GetOnlineConfig().useSwapMmapFile);
    IndexFormatVersion indexVersion(INDEX_FORMAT_VERSION);
    SchemaRewriter::RewriteForKV(mSchema, options, indexVersion);
    config::KKVIndexConfigPtr kkvConfig =
        DYNAMIC_POINTER_CAST(config::KKVIndexConfig, mSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    ASSERT_TRUE(kkvConfig->GetUseSwapMmapFile());
    KKVSegmentWriter<uint64_t> segWriter(mSchema, options);
    BuildingSegmentData segmentData;
    segmentData.SetSegmentId(0);
    segmentData.SetSegmentDirName("test_segment");
    FileSystemOptions fsOptions;
    fsOptions.outputStorage = FSST_MEM;
    RESET_FILE_SYSTEM("ut", true, fsOptions);
    MAKE_SEGMENT_DIRECTORY(0);
    segmentData.Init(GET_SEGMENT_DIRECTORY(), false);

    std::shared_ptr<indexlib::framework::SegmentMetrics> metrics(new framework::SegmentMetrics);
    segWriter.Init(segmentData, metrics, mQuotaControl);
    ASSERT_EQ(CalcucateSize(0, 0, options, metrics, true), segWriter.GetTotalMemoryUse());
    BuildDocs(segWriter, docStr);
    ASSERT_EQ(CalcucateSize(2, 1, options, metrics, true), segWriter.GetTotalMemoryUse());

    SegmentInfoPtr segmentInfo = segWriter.GetSegmentInfo();
    ASSERT_EQ((size_t)2, segmentInfo->docCount);
}

void KKVSegmentWriterTest::TestUserTimestamp()
{
    string docStr = "cmd=add,single_int8=-8,single_int16=-16,pkey=1,skey=2,ts=1000000,ha_reserved_timestamp=2000000;"
                    "cmd=add,single_int8=-9,single_int16=-17,pkey=1,skey=3,ts=2000000,ha_reserved_timestamp=3000000;";
    IndexPartitionOptions options;
    KKVSegmentWriter<uint64_t> segWriter(mSchema, options);
    BuildingSegmentData segmentData;
    segmentData.SetSegmentId(0);
    segmentData.SetSegmentDirName("segment");
    MAKE_SEGMENT_DIRECTORY(0);
    segmentData.Init(GET_SEGMENT_DIRECTORY(), false);
    std::shared_ptr<indexlib::framework::SegmentMetrics> metrics(new framework::SegmentMetrics);

    segWriter.Init(segmentData, metrics, mQuotaControl);
    ASSERT_EQ(CalcucateSize(0, 0, options, metrics), segWriter.GetTotalMemoryUse());
    BuildDocs(segWriter, docStr);
    InMemorySegmentReaderPtr reader = segWriter.CreateInMemSegmentReader();
    CheckReader(reader, "1", "2;3", "-16;-17", "1;2");

    OfflineConfig offlineConfig;
    offlineConfig.buildConfig.useUserTimestamp = true;
    options.SetOfflineConfig(offlineConfig);
    KKVSegmentWriter<uint64_t> segWriterUTS(mSchema, options);
    BuildingSegmentData segmentDataUTS;
    segmentDataUTS.SetSegmentId(0);
    segmentDataUTS.SetSegmentDirName("segment");
    MAKE_SEGMENT_DIRECTORY(0);
    segmentDataUTS.Init(GET_SEGMENT_DIRECTORY(), false);

    std::shared_ptr<indexlib::framework::SegmentMetrics> metricsUTS(new framework::SegmentMetrics);
    segWriterUTS.Init(segmentDataUTS, metricsUTS, mQuotaControl);
    ASSERT_EQ(CalcucateSize(0, 0, options, metricsUTS), segWriterUTS.GetTotalMemoryUse());
    BuildDocs(segWriterUTS, docStr);
    InMemorySegmentReaderPtr readerUTS = segWriterUTS.CreateInMemSegmentReader();
    CheckReader(readerUTS, "1", "2;3", "-16;-17", "2;3");
}
void KKVSegmentWriterTest::TestCalculateMemoryRatio()
{
    typedef KKVSegmentWriter<uint32_t> Writer;
    ASSERT_DOUBLE_EQ(0.01, Writer::CalculateMemoryRatio(std::shared_ptr<framework::SegmentGroupMetrics>(), 0));

    indexlib::framework::SegmentMetrics metrics;
    string groupName = "group";

    metrics.Set<size_t>(groupName, KKV_SKEY_MEM_USE, 0);
    metrics.Set<size_t>(groupName, KKV_VALUE_MEM_USE, 0);
    metrics.Set<double>(groupName, KKV_SKEY_VALUE_MEM_RATIO, 0);
    std::shared_ptr<framework::SegmentGroupMetrics> groupMetrics = metrics.GetSegmentGroupMetrics(groupName);

    ASSERT_DOUBLE_EQ(0.01, Writer::CalculateMemoryRatio(groupMetrics, 0));

    metrics.Set<size_t>(groupName, KKV_SKEY_MEM_USE, 0);
    metrics.Set<size_t>(groupName, KKV_VALUE_MEM_USE, 1);
    ASSERT_DOUBLE_EQ(0.0001, Writer::CalculateMemoryRatio(groupMetrics, 0));

    metrics.Set<size_t>(groupName, KKV_SKEY_MEM_USE, 100);
    metrics.Set<size_t>(groupName, KKV_VALUE_MEM_USE, 0);
    metrics.Set<double>(groupName, KKV_SKEY_VALUE_MEM_RATIO, 1);
    ASSERT_DOUBLE_EQ(0.90, Writer::CalculateMemoryRatio(groupMetrics, 0));

    metrics.Set<size_t>(groupName, KKV_SKEY_MEM_USE, 8);
    metrics.Set<size_t>(groupName, KKV_VALUE_MEM_USE, 192);
    metrics.Set<double>(groupName, KKV_SKEY_VALUE_MEM_RATIO, 0);
    ASSERT_DOUBLE_EQ(0.04 * 0.7, Writer::CalculateMemoryRatio(groupMetrics, 0));

    metrics.Set<size_t>(groupName, KKV_SKEY_MEM_USE, 8);
    metrics.Set<size_t>(groupName, KKV_VALUE_MEM_USE, 192);
    metrics.Set<double>(groupName, KKV_SKEY_VALUE_MEM_RATIO, 1);
    ASSERT_DOUBLE_EQ(0.04 * 0.7 + 0.3, Writer::CalculateMemoryRatio(groupMetrics, 0));
}

void KKVSegmentWriterTest::TestBuildSKeyFull()
{
    IndexPartitionOptions options;
    options.GetBuildConfig().buildTotalMemory = 1;
    KKVSegmentWriter<uint64_t> segWriter(mSchema, options);
    BuildingSegmentData segmentData;
    segmentData.SetSegmentId(0);
    segmentData.SetSegmentDirName("test_segment");
    MAKE_SEGMENT_DIRECTORY(0);
    segmentData.Init(GET_SEGMENT_DIRECTORY(), false);

    std::shared_ptr<indexlib::framework::SegmentMetrics> metrics(new framework::SegmentMetrics);
    segWriter.Init(segmentData, metrics, mQuotaControl);

    string docStr = "cmd=delete,pkey=1,skey=2,ts=1000000;"
                    "cmd=add,single_int8=-9,single_int16=-17,pkey=2,skey=3,ts=2000000;";
    auto docs = test::DocumentCreator::CreateKVDocuments(mSchema, docStr);
    while (true) {
        ASSERT_TRUE(segWriter.AddDocument(docs[0]));
        ASSERT_FALSE(segWriter.NeedForceDump());
        if (segWriter.mSKeyWriter->GetSKeyNodes().Size() + 1 == segWriter.mSKeyWriter->GetSKeyNodes().Capacity()) {
            break;
        }
    }
    ASSERT_TRUE(segWriter.AddDocument(docs[1]));
    ASSERT_TRUE(segWriter.NeedForceDump());
}
}} // namespace indexlib::partition
