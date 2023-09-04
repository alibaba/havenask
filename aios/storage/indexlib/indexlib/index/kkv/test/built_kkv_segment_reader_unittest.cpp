#include "indexlib/index/kkv/test/built_kkv_segment_reader_unittest.h"

#include "indexlib/config/kkv_index_config.h"
#include "indexlib/document/document_rewriter/pack_attribute_rewriter.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/common/KeyHasherWrapper.h"
#include "indexlib/index/kkv/kkv_built_segment_doc_iterator.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/region_schema_maker.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::util;
using namespace indexlib::test;
using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::partition;
using namespace indexlib::index_base;
using namespace indexlib::document;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, BuiltKKVSegmentReaderTest);

BuiltKKVSegmentReaderTest::BuiltKKVSegmentReaderTest() : mEx(1) {}

BuiltKKVSegmentReaderTest::~BuiltKKVSegmentReaderTest() {}

void BuiltKKVSegmentReaderTest::CaseSetUp()
{
    mQuotaControl.reset(new util::QuotaControl(1024 * 1024 * 1024));
    mPool.reset(new Pool());
}

void BuiltKKVSegmentReaderTest::CaseTearDown() {}

void BuiltKKVSegmentReaderTest::TestSimpleProcess()
{
    string fields = "single_int8:int8;single_int16:int16;pkey:uint64;skey:uint64";
    auto schema = SchemaMaker::MakeKKVSchema(fields, "pkey", "skey", "single_int8;single_int16;skey");

    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    auto kkvIndexConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());

    PKeyTableType tableType = GET_CASE_PARAM();
    KKVIndexPreference::HashDictParam param(PrefixKeyTableType2Str(tableType));
    kkvIndexConfig->GetIndexPreference().SetHashDictParam(param);

    string docStr = "cmd=add,single_int8=-8,single_int16=-16,pkey=1,skey=2,ts=1000000;"
                    "cmd=add,single_int8=-9,single_int16=-17,pkey=1,skey=3,ts=2000000;"
                    "cmd=add,single_int8=9,single_int16=17,pkey=1,skey=3,ts=3000000;"
                    "cmd=add,single_int8=9,single_int16=16,pkey=2,skey=2,ts=3000000;"
                    "cmd=add,single_int8=9,single_int16=18,pkey=1,skey=4,ts=3000000;";

    IndexPartitionOptions options;
    options.SetIsOnline(false);

    KKVSegmentWriter<uint64_t> segWriter(schema, options);
    BuildingSegmentData segmentData;
    segmentData.SetSegmentId(0);
    std::shared_ptr<indexlib::framework::SegmentMetrics> metrics(new framework::SegmentMetrics);
    segWriter.Init(segmentData, metrics, mQuotaControl);
    SegmentInfoPtr segmentInfo = segWriter.GetSegmentInfo();

    BuildDocs(segWriter, schema, docStr);

    file_system::DirectoryPtr directory = MAKE_SEGMENT_DIRECTORY(0);
    segWriter.EndSegment();
    vector<std::unique_ptr<common::DumpItem>> dumpItems;
    segWriter.CreateDumpItems(directory, dumpItems);
    ASSERT_EQ((size_t)1, dumpItems.size());
    auto dumpItem = dumpItems[0].release();
    dumpItem->process(mPool.get());
    delete dumpItem;

    segmentData.SetDirectory(directory);
    segmentData.SetSegmentInfo(*segmentInfo);

    KKVIndexOptions kkvOptions;
    kkvOptions.kkvConfig = kkvIndexConfig;
    kkvOptions.kvConfig = kkvIndexConfig;
    CheckData(segmentData, kkvIndexConfig, kkvOptions, "1", "2;3;4", "-16;17;18", "1;3;3");
    CheckData(segmentData, kkvIndexConfig, kkvOptions, "2", "2", "16", "3");

    // OnDiskSKeyOffset is 6 bytes
    ASSERT_EQ((size_t)10, sizeof(NormalOnDiskSKeyNode<uint32_t>));
}

void BuiltKKVSegmentReaderTest::TestMultiRegion()
{
    string fields = "single_int8:int8;single_int16:int16;pkey:uint64;skey:uint64";
    RegionSchemaMaker maker(fields, "kkv");
    maker.AddRegion("region1", "pkey", "skey", "single_int8;single_int16;skey;", "");
    maker.AddRegion("region2", "skey", "pkey", "single_int8;single_int16;pkey;", "");
    IndexPartitionSchemaPtr schema = maker.GetSchema();
    KKVIndexConfigPtr firstKKVConfig =
        DYNAMIC_POINTER_CAST(config::KKVIndexConfig, schema->GetIndexSchema(0)->GetPrimaryKeyIndexConfig());

    KKVIndexConfigPtr secondKKVConfig =
        DYNAMIC_POINTER_CAST(config::KKVIndexConfig, schema->GetIndexSchema(1)->GetPrimaryKeyIndexConfig());

    string fullDocs = "cmd=add,pkey=100,skey=0,single_int16=1,single_int8=3,ts=1000000,region_name=region1;"
                      "cmd=add,pkey=101,skey=1,single_int16=2,single_int8=4,ts=2000000,region_name=region2;"
                      "cmd=add,pkey=100,skey=1,single_int16=3,single_int8=5,ts=3000000,region_name=region1;"
                      "cmd=add,pkey=102,skey=3,single_int16=4,single_int8=6,ts=4000000,region_name=region1;"
                      "cmd=add,pkey=100,skey=3,single_int16=5,single_int8=7,ts=5000000,region_name=region1;"
                      "cmd=add,pkey=104,skey=1,single_int16=6,single_int8=8,ts=6000000,region_name=region2;";
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    MultiRegionKKVSegmentWriter segWriter(schema, options);
    BuildingSegmentData segmentData;
    segmentData.SetSegmentId(0);
    std::shared_ptr<indexlib::framework::SegmentMetrics> metrics(new framework::SegmentMetrics);
    segWriter.Init(segmentData, metrics, mQuotaControl);
    SegmentInfoPtr segmentInfo = segWriter.GetSegmentInfo();
    BuildDocs(segWriter, schema, fullDocs);

    file_system::DirectoryPtr directory = MAKE_SEGMENT_DIRECTORY(0);
    segWriter.EndSegment();
    vector<std::unique_ptr<common::DumpItem>> dumpItems;
    segWriter.CreateDumpItems(directory, dumpItems);
    ASSERT_EQ((size_t)1, dumpItems.size());
    auto dumpItem = dumpItems[0].release();
    dumpItem->process(mPool.get());
    delete dumpItem;
    segmentData.SetDirectory(directory);
    segmentData.SetSegmentInfo(*segmentInfo);

    KKVIndexOptions kkvOptions1;
    KKVIndexOptions kkvOptions2;
    kkvOptions1.kvConfig = firstKKVConfig;
    kkvOptions1.kkvConfig = firstKKVConfig;
    kkvOptions1.hasMultiRegion = true;
    kkvOptions2.kvConfig = secondKKVConfig;
    kkvOptions2.kkvConfig = secondKKVConfig;
    kkvOptions2.hasMultiRegion = true;
    KKVIndexConfigPtr dataKKVConfig = CreateKKVIndexConfigForMultiRegionData(schema);

    CheckData(segmentData, dataKKVConfig, kkvOptions1, "100", "0;1;3", "1;3;5", "1;3;5");
    CheckData(segmentData, dataKKVConfig, kkvOptions1, "102", "3", "4", "4");
    CheckData(segmentData, dataKKVConfig, kkvOptions2, "1", "101;104", "2;6", "2;6");

    { // test regionId mismatch
        typedef uint64_t SKeyType;
        BuiltKKVSegmentReader<SKeyType> segReader;
        segReader.Open(dataKKVConfig, segmentData);

        string pkeyStr = "100";
        uint64_t pkey;
        EXPECT_TRUE(KeyHasherWrapper::GetHashKeyByFieldType(ft_uint64, pkeyStr.c_str(), pkeyStr.size(), pkey));
        pkey = kkvOptions1.GetLookupKeyHash(pkey);
        typedef KKVBuiltSegmentDocIterator<SKeyType> SegmentIterator;
        auto kkvIter = future_lite::interface::syncAwait(
            segReader.template Lookup<SegmentIterator>(pkey, mPool.get(), &kkvOptions2), &mEx);
        ASSERT_FALSE(kkvIter);
    }
}

void BuiltKKVSegmentReaderTest::CheckData(const SegmentData& segData, const KKVIndexConfigPtr& kkvConfig,
                                          const KKVIndexOptions& kkvOptions, const string& pkeyStr,
                                          const string& skeyStr, const string& expectedValuesStr,
                                          const string& expectedTsStr)
{
    typedef uint64_t SKeyType;
    BuiltKKVSegmentReader<SKeyType> segReader;
    segReader.Open(kkvConfig, segData);

    uint64_t pkey;
    EXPECT_TRUE(KeyHasherWrapper::GetHashKeyByFieldType(ft_uint64, pkeyStr.c_str(), pkeyStr.size(), pkey));
    pkey = kkvOptions.GetLookupKeyHash(pkey);
    typedef KKVBuiltSegmentDocIterator<SKeyType> SegmentIterator;
    auto kkvIter = future_lite::interface::syncAwait(
        segReader.template Lookup<SegmentIterator>(pkey, mPool.get(), &kkvOptions), &mEx);
    ASSERT_TRUE(kkvIter);

    vector<string> skeys;
    StringUtil::fromString(skeyStr, skeys, ";");
    vector<string> expectedValues;
    StringUtil::fromString(expectedValuesStr, expectedValues, ";");
    vector<string> expectedTs;
    StringUtil::fromString(expectedTsStr, expectedTs, ";");

    vector<pair<SKeyType, pair<int16_t, uint32_t>>> nodes;
    for (size_t i = 0; i < skeys.size(); ++i) {
        SKeyType skey;
        EXPECT_TRUE(KeyHasherWrapper::GetHashKeyByFieldType(ft_uint64, skeys[i].c_str(), skeys[i].size(), skey));
        int16_t value;
        StringUtil::fromString(expectedValues[i], value);
        uint32_t ts;
        StringUtil::fromString(expectedTs[i], ts);
        nodes.push_back(make_pair(skey, make_pair(value, ts)));
    }
    sort(nodes.begin(), nodes.end());

    assert(skeys.size() == expectedValues.size());
    assert(skeys.size() == expectedTs.size());
    PackAttributeFormatter formatter;
    formatter.Init(kkvOptions.kkvConfig->GetValueConfig()->CreatePackAttributeConfig());

    string attrName = "single_int16";
    AttributeReferenceTyped<int16_t>* attrRef = formatter.GetAttributeReferenceTyped<int16_t>(attrName);
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
    IE_POOL_COMPATIBLE_DELETE_CLASS(mPool.get(), kkvIter);
}

void BuiltKKVSegmentReaderTest::BuildDocs(KKVSegmentWriter<uint64_t>& writer, const IndexPartitionSchemaPtr& schema,
                                          const string& docStr)
{
    // TODO(hanyao)
    auto docs = DocumentCreator::CreateKVDocuments(schema, docStr);
    for (size_t i = 0; i < docs.size(); i++) {
        writer.AddDocument(docs[i]);
    }
}
}} // namespace indexlib::index
