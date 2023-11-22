#include "indexlib/index/kkv/merge/OnDiskKKVIterator.h"

#include "indexlib/config/ITabletSchema.h"
#include "indexlib/document/raw_document/test/RawDocumentMaker.h"
#include "indexlib/document/test/KVDocumentBatchMaker.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/IIndexer.h"
#include "indexlib/index/IMemIndexer.h"
#include "indexlib/index/common/FieldTypeTraits.h"
#include "indexlib/index/kkv/building/KKVMemIndexer.h"
#include "indexlib/index/kkv/built/KKVDiskIndexer.h"
#include "indexlib/index/kkv/common/Trait.h"
#include "indexlib/table/kkv_table/test/KKVTabletSchemaMaker.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace indexlib::file_system;
using namespace indexlib::util;
using namespace indexlibv2::framework;
using namespace indexlib::config;

namespace indexlibv2::index {

class KKVMockSegment : public framework::Segment
{
public:
    KKVMockSegment(const std::shared_ptr<IIndexer>& indexer, SegmentStatus segmentStatus)
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

class OnDiskKKVIteratorTest : public TESTBASE
{
private:
    using OnDiskKKVIteratorTyped = OnDiskKKVIterator<int8_t>;

public:
    OnDiskKKVIteratorTest() {}
    ~OnDiskKKVIteratorTest() {}

public:
    void setUp() override
    {
        // prepare fs
        auto testRoot = GET_TEMP_DATA_PATH();
        auto fs = indexlib::file_system::FileSystemCreator::Create("test", testRoot).GetOrThrow();
        _rootDir = indexlib::file_system::IDirectory::Get(fs);
    }
    void tearDown() override {}

    std::shared_ptr<IMemIndexer> CreateKKVMemIndexer(FieldType skeyType, size_t pkeyMemUse, size_t skeyMemUse,
                                                     size_t valueMemUse, double skeyCompressRatio,
                                                     double valueCompressRatio);

private:
    void InnerTest(const std::string& docStrs, const std::string& dataInfoStr, PKeyTableType tableType,
                   size_t expectPkeyCount);

    void CreateIterator(const std::vector<std::string>& docStrVec, PKeyTableType tableType,
                        std::shared_ptr<OnDiskKKVIteratorTyped>& result);

    void CheckIterator(const shared_ptr<OnDiskKKVIteratorTyped>& iter, const std::string& dataInfoStr,
                       size_t expectPkeyCount);

private:
    std::shared_ptr<indexlib::file_system::IDirectory> _rootDir;
    std::shared_ptr<config::KKVIndexConfig> _kkvIndexConfig;
    std::shared_ptr<config::ITabletSchema> _tabletSchema;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, OnDiskKKVIteratorTest);

TEST_F(OnDiskKKVIteratorTest, TestSimpleProcess)
{
    string docStrInSegments = "cmd=add,pkey=1,skey=0,value=1,ts=1000000;"
                              "cmd=add,pkey=2,skey=1,value=2,ts=1000000;"
                              "cmd=add,pkey=1,skey=2,value=3,ts=2000000;#"
                              "cmd=add,pkey=3,skey=2,value=4,ts=2000000;"
                              "cmd=delete,pkey=1,skey=2,ts=3000000;"
                              "cmd=delete,pkey=2,ts=4000000;#"
                              "cmd=add,pkey=2,skey=5,value=5,ts=5000000;";

    string expectData = "1:0:1:1;"
                        "1:2:del_skey:3;"
                        "2:0:del_pkey:4;"
                        "2:5:5:5;"
                        "3:2:4:2";
    InnerTest(docStrInSegments, expectData, PKeyTableType::DENSE, 3);
    InnerTest(docStrInSegments, expectData, PKeyTableType::CUCKOO, 3);
    InnerTest(docStrInSegments, expectData, PKeyTableType::SEPARATE_CHAIN, 3);
}

TEST_F(OnDiskKKVIteratorTest, TestSamePKeyInMultiSegments)
{
    string docStrInSegments = "cmd=add,pkey=1,skey=0,value=1,ts=1000000;#"
                              "cmd=add,pkey=1,skey=2,value=3,ts=2000000;#"
                              "cmd=delete,pkey=1,ts=3000000;#"
                              "cmd=add,pkey=1,skey=2,value=4,ts=4000000;#"
                              "cmd=add,pkey=1,skey=3,value=5,ts=5000000;#"
                              "cmd=add,pkey=1,skey=1,value=6,ts=6000000;";

    string expectData = "1:0:del_pkey:3;"
                        "1:1:6:6;"
                        "1:2:4:4;"
                        "1:3:5:5";
    InnerTest(docStrInSegments, expectData, PKeyTableType::DENSE, 1);
    InnerTest(docStrInSegments, expectData, PKeyTableType::CUCKOO, 1);
    InnerTest(docStrInSegments, expectData, PKeyTableType::SEPARATE_CHAIN, 1);
}

TEST_F(OnDiskKKVIteratorTest, TestDuplicateSeyInMultiSegments)
{
    string docStrInSegments = "cmd=add,pkey=1,skey=0,value=1,ts=1000000;#"
                              "cmd=add,pkey=1,skey=2,value=3,ts=2000000;#"
                              "cmd=delete,pkey=1,skey=2,ts=3000000;#"
                              "cmd=add,pkey=1,skey=2,value=4,ts=4000000;#"
                              "cmd=add,pkey=1,skey=0,value=5,ts=5000000;";

    string expectData = "1:0:5:5;"
                        "1:2:4:4";
    InnerTest(docStrInSegments, expectData, PKeyTableType::DENSE, 1);
    InnerTest(docStrInSegments, expectData, PKeyTableType::CUCKOO, 1);
    InnerTest(docStrInSegments, expectData, PKeyTableType::SEPARATE_CHAIN, 1);
}

TEST_F(OnDiskKKVIteratorTest, TestDuplicateSeyInMultiSegments_13227243)
{
    string docStrInSegments = "cmd=add,pkey=1,skey=0,value=1,ts=1000000;cmd=add,pkey=1,skey=1,value=2,ts=2000000;#"
                              "cmd=add,pkey=1,skey=-1,value=3,ts=3000000;cmd=add,pkey=1,skey=1,value=3,ts=3000000;"
                              "cmd=delete,pkey=1,skey=0,ts=3000000;#"
                              "cmd=add,pkey=1,skey=2,value=4,ts=4000000;";

    string expectData = "1:-1:3:3;"
                        "1:0:del_skey:3;"
                        "1:1:3:3;"
                        "1:2:4:4";
    InnerTest(docStrInSegments, expectData, PKeyTableType::DENSE, 1);
    InnerTest(docStrInSegments, expectData, PKeyTableType::CUCKOO, 1);
    InnerTest(docStrInSegments, expectData, PKeyTableType::SEPARATE_CHAIN, 1);
}

void OnDiskKKVIteratorTest::InnerTest(const string& docStrs, const string& dataInfoStr, PKeyTableType tableType,
                                      size_t expectPkeyCount)
{
    tearDown();
    setUp();
    vector<string> docStrVec;
    StringUtil::fromString(docStrs, docStrVec, "#");
    std::shared_ptr<OnDiskKKVIteratorTest::OnDiskKKVIteratorTyped> iter;
    CreateIterator(docStrVec, tableType, iter);
    ASSERT_TRUE(iter);
    CheckIterator(iter, dataInfoStr, expectPkeyCount);
}

std::shared_ptr<IMemIndexer> OnDiskKKVIteratorTest::CreateKKVMemIndexer(FieldType skeyType, size_t pkeyMemUse,
                                                                        size_t skeyMemUse, size_t valueMemUse,
                                                                        double skeyCompressRatio,
                                                                        double valueCompressRatio)
{
    const std::string& tabletName = "test";
    FieldType skeyDictType = skeyType == ft_string ? ft_uint64 : skeyType;
    switch (skeyDictType) {
#define CASE_MACRO(type)                                                                                               \
    case type: {                                                                                                       \
        using SKeyType = indexlib::index::FieldTypeTraits<type>::AttrItemType;                                         \
        return std::make_shared<KKVMemIndexer<SKeyType>>(tabletName, pkeyMemUse, skeyMemUse, valueMemUse,              \
                                                         skeyCompressRatio, valueCompressRatio, false, true);          \
    }
        KKV_SKEY_DICT_TYPE_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
    default: {
        AUTIL_LOG(ERROR, "unsupported skey field_type:[%s]", config::FieldConfig::FieldTypeToStr(skeyDictType));
        return nullptr;
    }
    }
}

void OnDiskKKVIteratorTest::CreateIterator(const vector<string>& docStrVec, PKeyTableType tableType,
                                           std::shared_ptr<OnDiskKKVIteratorTest::OnDiskKKVIteratorTyped>& result)
{
    string field = "pkey:uint32;skey:int8;value:uint32;";
    _tabletSchema = table::KKVTabletSchemaMaker::Make(field, "pkey", "skey", "value");
    _kkvIndexConfig =
        std::dynamic_pointer_cast<indexlibv2::config::KKVIndexConfig>(_tabletSchema->GetIndexConfig("kkv", "pkey"));

    KKVIndexPreference::HashDictParam param(PrefixKeyTableType2Str(tableType));
    _kkvIndexConfig->GetIndexPreference().SetHashDictParam(param);
    _kkvIndexConfig->GetValueConfig()->EnableCompactFormat(true);

    vector<IIndexMerger::SourceSegment> segments;
    for (size_t i = 0; i < docStrVec.size(); i++) {
        auto indexer = this->CreateKKVMemIndexer(ft_int8, 1024, 1024, 1024, 1.0, 1.0);
        ASSERT_TRUE(indexer->Init(_kkvIndexConfig, nullptr).IsOK());

        auto rawDocs = document::RawDocumentMaker::MakeBatch(docStrVec[i]);
        for (const auto& rawDoc : rawDocs) {
            auto docBatch = document::KVDocumentBatchMaker::Make(_tabletSchema, {rawDoc});
            ASSERT_TRUE(docBatch);
            auto s = indexer->Build(docBatch.get());
            ASSERT_TRUE(s.IsOK());
        }
        autil::mem_pool::Pool pool;
        indexlib::file_system::DirectoryOption option;
        auto segmentDir =
            _rootDir->MakeDirectory("segment_" + StringUtil::toString(i) + "_level_0/", option).GetOrThrow();
        ASSERT_TRUE(segmentDir);
        indexlib::file_system::DirectoryOption dirOption;
        auto indexDir0 = segmentDir->MakeDirectory("index", dirOption).GetOrThrow();
        auto indexDir1 = indexlib::file_system::IDirectory::ToLegacyDirectory(indexDir0);
        auto s = indexer->Dump(&pool, indexDir1, nullptr);
        ASSERT_TRUE(s.IsOK());

        auto metrics = std::make_shared<indexlib::framework::SegmentMetrics>();
        indexer->FillStatistics(metrics);
        std::shared_ptr<KKVDiskIndexer<int8_t>> diskIndexer = std::make_shared<KKVDiskIndexer<int8_t>>(0, true);
        auto status = diskIndexer->Open(_kkvIndexConfig, indexDir0);
        ASSERT_TRUE(status.IsOK());

        std::shared_ptr<framework::Segment> segment0 =
            std::make_shared<KKVMockSegment>(diskIndexer, framework::Segment::SegmentStatus::ST_BUILT);
        framework::SegmentMeta meta0(0);
        meta0.segmentDir = indexlib::file_system::IDirectory::ToLegacyDirectory(segmentDir);
        meta0.segmentMetrics = metrics;
        meta0.schema = _tabletSchema;
        segment0->TEST_SetSegmentMeta(meta0);
        IIndexMerger::SourceSegment sourceSegment {0, segment0};

        segments.push_back(sourceSegment);
    }

    result.reset(new OnDiskKKVIteratorTyped(_kkvIndexConfig));
    ASSERT_TRUE(result->Init(segments).IsOK());
}

// dataInfoStr : pkey:skey:[del_pkey|del_skey|value]:ts;
void OnDiskKKVIteratorTest::CheckIterator(const std::shared_ptr<OnDiskKKVIteratorTyped>& iter,
                                          const string& dataInfoStr, size_t expectPkeyCount)
{
    ASSERT_EQ(expectPkeyCount, iter->GetEstimatePkeyCount());
    vector<string> dataInfo;
    StringUtil::fromString(dataInfoStr, dataInfo, ";");

    size_t count = 0;
    while (iter->IsValid()) {
        auto singlePkeyIter = iter->GetCurrentIterator();
        ASSERT_TRUE(singlePkeyIter);

        uint64_t pkey = singlePkeyIter->GetPKeyHash();
        if (singlePkeyIter->HasPKeyDeleted()) {
            string str = StringUtil::toString<uint64_t>(pkey) + ":" + StringUtil::toString<uint64_t>(0) + ":" +
                         "del_pkey:" + StringUtil::toString<uint64_t>(singlePkeyIter->GetPKeyDeletedTs());
            ASSERT_EQ(dataInfo[count++], str);
        }

        while (singlePkeyIter->IsValid()) {
            uint64_t skey = singlePkeyIter->GetCurrentSkey();
            bool isDelete = singlePkeyIter->CurrentSkeyDeleted();
            uint32_t timestamp = singlePkeyIter->GetCurrentTs();

            string typeValue = "del_skey";
            if (!isDelete) {
                autil::StringView value;
                singlePkeyIter->GetCurrentValue(value);
                typeValue = StringUtil::toString<uint32_t>(*(uint32_t*)value.data());
            }
            string str = StringUtil::toString<uint64_t>(pkey) + ":" + StringUtil::toString<int64_t>((int64_t)skey) +
                         ":" + typeValue + ":" + StringUtil::toString<uint64_t>(timestamp);
            ASSERT_EQ(dataInfo[count++], str);
            singlePkeyIter->MoveToNext();
        }
        iter->MoveToNext();
    }
    ASSERT_EQ(dataInfo.size(), count);
}

} // namespace indexlibv2::index
