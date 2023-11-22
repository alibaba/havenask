#include "autil/Log.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/document/kv/KVDocumentBatch.h"
#include "indexlib/document/raw_document/test/RawDocumentMaker.h"
#include "indexlib/document/test/KVDocumentBatchMaker.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/kv/Common.h"
#include "indexlib/index/kv/FieldValueExtractor.h"
#include "indexlib/index/kv/IKVIterator.h"
#include "indexlib/index/kv/KVDiskIndexer.h"
#include "indexlib/index/kv/TTLFilter.h"
#include "indexlib/index/kv/VarLenKVMemIndexer.h"
#include "indexlib/index/kv/VarLenKVMerger.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/kv/config/ValueConfig.h"
#include "indexlib/table/kv_table/test/KVTabletSchemaMaker.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;

// kv merge integration test (including fixed len and var len test) is in kv_table/index_task/test

namespace indexlibv2 { namespace index {

class KVMockSegment : public framework::Segment
{
public:
    KVMockSegment(const std::shared_ptr<IIndexer>& indexer, SegmentStatus segmentStatus)
        : framework::Segment(segmentStatus)
        , _indexer(indexer)
    {
    }
    std::pair<Status, std::shared_ptr<indexlibv2::index::IIndexer>> GetIndexer(const std::string& type,
                                                                               const std::string& indexName) override
    {
        if (_indexer) {
            return std::make_pair(Status::OK(), _indexer);
        }
        return std::make_pair(Status::NotFound(), nullptr);
    }
    std::pair<Status, size_t> EstimateMemUsed(const std::shared_ptr<config::ITabletSchema>& schema) override
    {
        return {Status::OK(), 0};
    }
    size_t EvaluateCurrentMemUsed() override { return 0; }

private:
    std::shared_ptr<IIndexer> _indexer;
};

class KVMergerTest : public TESTBASE
{
public:
    KVMergerTest() {}
    ~KVMergerTest() {}

public:
    void setUp() override {}
    void tearDown() override {}

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, KVMergerTest);

TEST_F(KVMergerTest, TestInit)
{
    {
        std::string field = "key:string;value1:int32;value2:int64;";
        auto tabletSchema = table::KVTabletSchemaMaker::Make(field, "key", "value1;value2");
        map<string, any> params = {{CURRENT_TIME_IN_SECOND, std::string("100")}};
        VarLenKVMerger varLenKVMerger;
        Status s = varLenKVMerger.Init(tabletSchema->GetIndexConfig("kv", "key"), params);
        ASSERT_FALSE(s.IsOK());
    }
    {
        std::string field = "key:string;value1:int32;value2:int64;";
        auto tabletSchema = table::KVTabletSchemaMaker::Make(field, "key", "value1;value2");
        std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(tabletSchema->GetIndexConfig("kv", "key"))
            ->SetTTL(100);
        map<string, any> params = {{DROP_DELETE_KEY, std::string("true")},
                                   {CURRENT_TIME_IN_SECOND, std::string("100")}};
        VarLenKVMerger varLenKVMerger;
        Status s = varLenKVMerger.Init(tabletSchema->GetIndexConfig("kv", "key"), params);
        ASSERT_TRUE(s.IsOK());
        ASSERT_TRUE(varLenKVMerger.TEST_GetDropDeleteKey());
        ASSERT_TRUE(dynamic_cast<TTLFilter*>(varLenKVMerger.TEST_GetRecordFilter()));
    }
}

TEST_F(KVMergerTest, TestMergeWithValueAdapter)
{
    // prepare fs
    auto testRoot = GET_TEMP_DATA_PATH();
    auto fs = indexlib::file_system::FileSystemCreator::Create("test", testRoot).GetOrThrow();
    auto rootDir = indexlib::file_system::Directory::Get(fs);
    indexlib::file_system::DirectoryOption option;

    // prepare schema & indexer data
    std::string field = "key:string;value1:int32;value2:int64;";
    auto tabletSchema = table::KVTabletSchemaMaker::Make(field, "key", "value1;value2");
    auto kvIndexConfig =
        std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(tabletSchema->GetIndexConfig("kv", "key"));

    VarLenKVMemIndexer indexer(true, 1024 * 1024);
    ASSERT_TRUE(indexer.Init(kvIndexConfig, nullptr).IsOK());
    std::string docStr = "cmd=add,key=1,value1=10,value2=11,ts=101000000;"
                         "cmd=add,key=2,value1=20,value2=22,ts=102000000;";
    auto rawDocs = document::RawDocumentMaker::MakeBatch(docStr);
    for (const auto& rawDoc : rawDocs) {
        auto docBatch = document::KVDocumentBatchMaker::Make(tabletSchema, {rawDoc});
        ASSERT_TRUE(docBatch);
        auto s = indexer.Build(docBatch.get());
        ASSERT_TRUE(s.IsOK()) << s.ToString();
    }
    autil::mem_pool::Pool pool;
    auto dir0 = rootDir->MakeDirectory("segment_0_level_0/index/", option);
    auto s = indexer.Dump(&pool, dir0, nullptr);
    ASSERT_TRUE(s.IsOK()) << s.ToString();

    // prepare new schema
    field = "key:string;value1:int32;value2:int64;value3:uint64";
    auto newTabletSchema = table::KVTabletSchemaMaker::Make(field, "key", "value1;value2;value3");
    auto newFieldConfig = newTabletSchema->GetFieldConfig("value3");
    newFieldConfig->SetDefaultValue("100");
    auto newKvIndexConfig =
        std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(newTabletSchema->GetIndexConfig("kv", "key"));
    newTabletSchema->SetSchemaId(1);

    // prepare merge segment infos
    auto metrics = std::make_shared<indexlib::framework::SegmentMetrics>();
    indexer.FillStatistics(metrics);
    std::shared_ptr<KVDiskIndexer> diskIndexer = std::make_shared<KVDiskIndexer>();
    s = diskIndexer->Open(kvIndexConfig, dir0->GetIDirectory());
    ASSERT_TRUE(s.IsOK()) << s.ToString();

    std::shared_ptr<framework::Segment> segment0 =
        std::make_shared<KVMockSegment>(diskIndexer, framework::Segment::SegmentStatus::ST_BUILT);
    framework::SegmentMeta meta0(0);
    meta0.segmentMetrics = metrics;
    meta0.schema = tabletSchema;
    segment0->TEST_SetSegmentMeta(meta0);

    std::shared_ptr<framework::SegmentMeta> meta1(new framework::SegmentMeta(1));
    meta1->segmentDir = rootDir->MakeDirectory("segment_1_level_0", option);
    meta1->schema = newTabletSchema;
    IIndexMerger::SourceSegment sourceSegment0;
    sourceSegment0.segment = segment0;

    index::IIndexMerger::SegmentMergeInfos segMergeInfos;
    segMergeInfos.srcSegments.emplace_back(sourceSegment0);
    segMergeInfos.targetSegments.push_back(meta1);

    // use new schema triger merge
    map<string, any> params = {{DROP_DELETE_KEY, std::string("true")}};
    VarLenKVMerger varLenKVMerger;
    ASSERT_TRUE(varLenKVMerger.Init(newKvIndexConfig, params).IsOK());
    std::shared_ptr<framework::IndexTaskResourceManager> taskResourceManager;
    ASSERT_TRUE(varLenKVMerger.Merge(segMergeInfos, taskResourceManager).IsOK());

    // check merge data value
    KVDiskIndexer newIndexer;
    auto dir1 = rootDir->GetDirectory("segment_1_level_0/index/", true);
    ASSERT_TRUE(newIndexer.Open(newKvIndexConfig, dir1->GetIDirectory()).IsOK());
    auto iter = newIndexer.GetReader()->CreateIterator();

    auto checkValues = [&](const autil::StringView& value, const std::string& expectValues) {
        vector<vector<string>> expectInfos;
        autil::StringUtil::fromString(expectValues, expectInfos, "=", ";");
        auto valueConfig = newKvIndexConfig->GetValueConfig();
        PackAttributeFormatter formatter;
        auto [status, packAttrConfig] = valueConfig->CreatePackAttributeConfig();
        ASSERT_TRUE(status.IsOK());
        ASSERT_TRUE(formatter.Init(packAttrConfig));

        autil::StringView packValue = value;
        if (valueConfig->GetFixedLength() == -1) {
            size_t countLen = 0;
            autil::MultiValueFormatter::decodeCount(value.data(), countLen);
            packValue = autil::StringView(value.data() + countLen, value.size() - countLen);
        }
        FieldValueExtractor valueExtractor(&formatter, packValue, &pool);
        for (size_t i = 0; i < expectInfos.size(); i++) {
            assert(expectInfos[i].size() == 2);
            FieldType type;
            bool isMulti;
            size_t idx;
            string valueStr;
            ASSERT_TRUE(valueExtractor.GetValueMeta(expectInfos[i][0], idx, type, isMulti));
            ASSERT_TRUE(valueExtractor.GetStringValue(idx, valueStr));
            ASSERT_EQ(expectInfos[i][1], valueStr);
        }
    };

    Record record;
    ASSERT_TRUE(iter->HasNext());
    ASSERT_TRUE(iter->Next(&pool, record).IsOK());
    checkValues(record.value, "value1=10;value2=11;value3=100");
    ASSERT_TRUE(iter->HasNext());
    ASSERT_TRUE(iter->Next(&pool, record).IsOK());
    checkValues(record.value, "value1=20;value2=22;value3=100");
    ASSERT_FALSE(iter->HasNext());
}

}} // namespace indexlibv2::index
