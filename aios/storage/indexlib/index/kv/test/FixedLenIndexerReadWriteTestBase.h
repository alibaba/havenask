#include "autil/mem_pool/Pool.h"
#include "future_lite/CoroInterface.h"
#include "future_lite/executors/SimpleExecutor.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/document/kv/KVDocumentBatch.h"
#include "indexlib/document/raw_document/test/RawDocumentMaker.h"
#include "indexlib/document/test/KVDocumentBatchMaker.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/MountOption.h"
#include "indexlib/file_system/load_config/CacheLoadStrategy.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/common/FieldTypeTraits.h"
#include "indexlib/index/kv/FixedLenKVLeafReader.h"
#include "indexlib/index/kv/FixedLenKVMemIndexer.h"
#include "indexlib/index/kv/IKVIterator.h"
#include "indexlib/index/kv/IKVSegmentReader.h"
#include "indexlib/index/kv/KVCommonDefine.h"
#include "indexlib/index/kv/KVDiskIndexer.h"
#include "indexlib/index/kv/KVFormatOptions.h"
#include "indexlib/index/kv/KVTypeId.h"
#include "indexlib/index/kv/SegmentStatistics.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/kv/config/KVIndexPreference.h"
#include "unittest/unittest.h"

namespace indexlibv2::index {

class FixedLenIndexerReadWriteTestBase : public TESTBASE
{
public:
    void setUp() override
    {
        _pool = std::make_unique<autil::mem_pool::Pool>();
        _testDataPath = GET_TEMPLATE_DATA_PATH();
        auto [s, fs] = indexlib::file_system::FileSystemCreator::CreateForWrite("ut", _testDataPath).StatusWith();
        ASSERT_TRUE(s.IsOK());
        _directory = indexlib::file_system::Directory::Get(fs);
        ASSERT_TRUE(_directory);
    }

protected:
    virtual void MakeSchema(const std::string& fieldNames, const std::string& keyName, const std::string& valueNames,
                            std::string compressTypeStr = "") = 0;

    std::shared_ptr<indexlib::file_system::Directory> PrepareOnlineDirectory(bool inMemory)
    {
        indexlib::file_system::LoadConfigList loadConfigList;
        if (inMemory) {
            loadConfigList.PushBack(indexlib::file_system::LoadConfigListCreator::MakeMmapLoadConfig(false, false));
        } else {
            loadConfigList.PushBack(indexlib::file_system::LoadConfigListCreator::MakeBlockLoadConfig(
                "lru", 1024, 16, indexlib::file_system::CacheLoadStrategy::DEFAULT_CACHE_IO_BATCH_SIZE, false, false));
        }
        indexlib::file_system::FileSystemOptions options;
        options.loadConfigList = loadConfigList;
        options.enableAsyncFlush = false;
        options.needFlush = true;
        options.useCache = true;
        options.useRootLink = false;
        options.isOffline = true;
        auto fs = indexlib::file_system::FileSystemCreator::Create("ut", _testDataPath, options).GetOrThrow();
        if (fs) {
            auto ec = fs->MountDir(_testDataPath, "", "", indexlib::file_system::FSMT_READ_WRITE, true);
            if (ec != indexlib::file_system::FSEC_OK) {
                return nullptr;
            }
            return indexlib::file_system::Directory::Get(fs);
        } else {
            return nullptr;
        }
    }

    template <FieldType ft>
    void CheckReader(IKVSegmentReader* reader, bool hasIterator)
    {
        future_lite::executors::SimpleExecutor ex(1);
#define GetSync(key, value, ts)                                                                                        \
    future_lite::interface::syncAwait(reader->Get(key, value, ts, _pool.get(), nullptr, nullptr), &ex)

        using Type = typename indexlib::index::FieldTypeTraits<ft>::AttrItemType;

        // check iterator
        std::map<key_t, Record> keyStatus;
        if (hasIterator) {
            auto iter = reader->CreateIterator();
            ASSERT_TRUE(iter.get() != nullptr);
            Record record;
            while (iter->HasNext()) {
                auto s = iter->Next(_pool.get(), record);
                ASSERT_TRUE(s.IsOK()) << s.ToString();
                keyStatus[record.key] = record;
            }
        }

        uint64_t ts;
        if (hasIterator) {
            ASSERT_EQ(keyStatus.size(), 3u);
            Record record;
            Type typeValue;

            auto mapIter = keyStatus.find(2);
            ASSERT_TRUE(mapIter != keyStatus.end());
            record = mapIter->second;
            ASSERT_EQ(2, record.key);
            ASSERT_TRUE(record.deleted);
            if (_indexConfig->TTLEnabled()) {
                ASSERT_EQ(104, record.timestamp);
            }

            mapIter = keyStatus.find(1);
            ASSERT_TRUE(mapIter != keyStatus.end());
            record = mapIter->second;
            ASSERT_EQ(1, record.key);
            ASSERT_FALSE(record.deleted);
            typeValue = *(Type*)record.value.data();
            ASSERT_EQ(11, typeValue);
            if (_indexConfig->TTLEnabled()) {
                ASSERT_EQ(101, record.timestamp);
            }

            mapIter = keyStatus.find(3);
            ASSERT_TRUE(mapIter != keyStatus.end());
            record = mapIter->second;
            ASSERT_EQ(3, record.key);
            ASSERT_FALSE(record.deleted);
            typeValue = *(Type*)record.value.data();
            ASSERT_EQ(33, typeValue);
            if (_indexConfig->TTLEnabled()) {
                ASSERT_EQ(103, record.timestamp);
            }
        }

        // check reader
        autil::StringView value;
        Type v1 = 0;
        ASSERT_EQ(GetSync(1, value, ts), indexlib::util::OK);
        v1 = *(Type*)value.data();
        ASSERT_EQ(11, v1);
        if (_indexConfig->TTLEnabled()) {
            ASSERT_EQ(101, ts);
        }

        ASSERT_EQ(GetSync(2, value, ts), indexlib::util::DELETED);
        if (_indexConfig->TTLEnabled()) {
            ASSERT_EQ(104, ts);
        }

        ASSERT_EQ(GetSync(3, value, ts), indexlib::util::OK);
        v1 = *(Type*)value.data();
        ASSERT_EQ(33, v1);
        if (_indexConfig->TTLEnabled()) {
            ASSERT_EQ(103, ts);
        }

        ASSERT_EQ(GetSync(4, value, ts), indexlib::util::NOT_FOUND);
        ASSERT_EQ(GetSync(100, value, ts), indexlib::util::NOT_FOUND);
#undef GetSync
    }

    template <FieldType ft>
    void CheckMemoryReader(FixedLenKVMemIndexer* indexer)
    {
        auto reader = indexer->CreateInMemoryReader();
        ASSERT_TRUE(reader);
        CheckReader<ft>(reader.get(), false);
    }

    template <FieldType ft>
    void CheckDiskReader(bool loadToMemory)
    {
        auto dir = PrepareOnlineDirectory(loadToMemory);
        ASSERT_TRUE(dir);
        auto diskIndexer = std::make_unique<KVDiskIndexer>();
        auto s = diskIndexer->Open(_indexConfig, dir->GetIDirectory());
        ASSERT_TRUE(s.IsOK()) << s.ToString();
        auto reader = diskIndexer->GetReader();
        ASSERT_TRUE(reader);
        ASSERT_TRUE(std::dynamic_pointer_cast<FixedLenKVLeafReader>(reader));
        CheckReader<ft>(reader.get(), false);
        CheckReader<ft>(reader.get(), true);
    }

    void CheckBuildMemoryFull()
    {
        std::string fields = "key:uint32;f1:int8";
        MakeSchema(fields, "key", "f1");

        // 16 byte is for hash table header, 24 byte hash bucket
        FixedLenKVMemIndexer indexer(16 + 24);
        ASSERT_TRUE(indexer.Init(_indexConfig, nullptr).IsOK());

        auto& kvIndexPreference = _indexConfig->GetIndexPreference();
        auto dictParam = kvIndexPreference.GetHashDictParam();
        // occupancyPct is 50, so hash table will return full
        dictParam.SetOccupancyPct(50);
        kvIndexPreference.SetHashDictParam(dictParam);

        // build
        std::string docStr = "cmd=add,key=1,f1=11,ts=101000000;"
                             "cmd=add,key=1,f1=12,ts=101000000;";
        auto rawDocs = document::RawDocumentMaker::MakeBatch(docStr);
        for (const auto& rawDoc : rawDocs) {
            auto docBatch = document::KVDocumentBatchMaker::Make(_schema, {rawDoc});
            ASSERT_TRUE(docBatch);
            auto s = indexer.Build(docBatch.get());
            ASSERT_TRUE(s.IsNeedDump()) << s.ToString();
        }
    }

    template <FieldType ft>
    void DoTestBuildAndRead(std::string compressTypeStr = "", std::string compressFieldTypeStr = "")
    {
        auto f1TypeStr = indexlibv2::config::FieldConfig::FieldTypeToStr(ft);
        std::string fields = "key:uint32;f1:" + std::string(f1TypeStr) + ":false";
        MakeSchema(fields, "key", "f1" + compressFieldTypeStr, compressTypeStr);

        FixedLenKVMemIndexer indexer(DEFAULT_MEMORY_USE_IN_BYTES);
        ASSERT_TRUE(indexer.Init(_indexConfig, nullptr).IsOK());

        // build
        std::string docStr = "cmd=add,key=1,f1=11,ts=101000000;"
                             "cmd=add,key=1,f1=12,ts=101000000;"
                             "cmd=add,key=2,f1=22,ts=102000000;"
                             "cmd=add,key=1,f1=11,ts=101000000;"
                             "cmd=add,key=3,f1=33,ts=103000000;"
                             "cmd=delete,key=2,ts=104000000;"
                             "cmd=add,key=3,f1=33,ts=103000000;"
                             "cmd=delete,key=3,ts=103000000;"
                             "cmd=add,key=3,f1=33,ts=103000000;"
                             "cmd=add,key=2,f1=22,ts=102000000;"
                             "cmd=delete,key=2,ts=104000000;";
        auto rawDocs = document::RawDocumentMaker::MakeBatch(docStr);
        for (const auto& rawDoc : rawDocs) {
            auto docBatch = document::KVDocumentBatchMaker::Make(_schema, {rawDoc});
            ASSERT_TRUE(docBatch);
            auto s = indexer.Build(docBatch.get());
            ASSERT_TRUE(s.IsOK()) << s.ToString();
        }

        // fill and check statistics
        auto metrics = std::make_shared<indexlib::framework::SegmentMetrics>();
        indexer.FillStatistics(metrics);
        auto groupMetrics = metrics->GetSegmentGroupMetrics("key");
        ASSERT_TRUE(groupMetrics);
        ASSERT_EQ(3, groupMetrics->Get<size_t>(SegmentStatistics::KV_KEY_COUNT));
        ASSERT_EQ(1, groupMetrics->Get<size_t>(SegmentStatistics::KV_KEY_DELETE_COUNT));

        ASSERT_LT(0, groupMetrics->Get<float>(SegmentStatistics::KV_KEY_VALUE_MEM_RATIO));
        ASSERT_LT(0, groupMetrics->Get<size_t>(SegmentStatistics::KV_SEGMENT_MEM_USE));
        ASSERT_LT(0, groupMetrics->Get<size_t>(SegmentStatistics::KV_HASH_MEM_USE));
        ASSERT_EQ(0, groupMetrics->Get<size_t>(SegmentStatistics::KV_VALUE_MEM_USE));
        ASSERT_EQ(groupMetrics->Get<size_t>(SegmentStatistics::KV_SEGMENT_MEM_USE),
                  groupMetrics->Get<size_t>(SegmentStatistics::KV_HASH_MEM_USE));

        CheckMemoryReader<ft>(&indexer);

        // dump
        auto s = indexer.Dump(_pool.get(), _directory, nullptr);
        ASSERT_TRUE(s.IsOK()) << s.ToString();
        ASSERT_TRUE(_directory->IsExist("key/key"));
        ASSERT_TRUE(_directory->IsExist("key/format_option"));

        CheckDiskReader<ft>(/*loadToMemory*/ true);
        CheckDiskReader<ft>(/*loadToMemory*/ false);
    }

protected:
    std::string _testDataPath;
    std::shared_ptr<config::ITabletSchema> _schema;
    std::shared_ptr<indexlibv2::config::KVIndexConfig> _indexConfig;
    std::unique_ptr<autil::mem_pool::Pool> _pool;
    std::shared_ptr<indexlib::file_system::Directory> _directory;

protected:
    static constexpr int64_t DEFAULT_MEMORY_USE_IN_BYTES = 1024 * 1024;
};

} // namespace indexlibv2::index
