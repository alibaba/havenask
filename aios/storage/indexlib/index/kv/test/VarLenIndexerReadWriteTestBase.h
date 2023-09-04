#include "autil/TimeoutTerminator.h"
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
#include "indexlib/framework/mem_reclaimer/EpochBasedMemReclaimer.h"
#include "indexlib/index/common/FieldTypeTraits.h"
#include "indexlib/index/common/field_format/pack_attribute/PackAttributeFormatter.h"
#include "indexlib/index/kv/IKVIterator.h"
#include "indexlib/index/kv/IKVSegmentReader.h"
#include "indexlib/index/kv/KVCommonDefine.h"
#include "indexlib/index/kv/KVDiskIndexer.h"
#include "indexlib/index/kv/KVFormatOptions.h"
#include "indexlib/index/kv/KVTypeId.h"
#include "indexlib/index/kv/SegmentStatistics.h"
#include "indexlib/index/kv/VarLenKVCompressedLeafReader.h"
#include "indexlib/index/kv/VarLenKVLeafReader.h"
#include "indexlib/index/kv/VarLenKVMemIndexer.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/kv/config/KVIndexPreference.h"
#include "indexlib/index/kv/config/ValueConfig.h"
#include "unittest/unittest.h"
namespace indexlibv2::index {

class VarLenIndexerReadWriteTestBase : public TESTBASE
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
    virtual void MakeSchema(const std::string& fieldNames, const std::string& keyName,
                            const std::string& valueNames) = 0;

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
    void CheckReader(IKVSegmentReader* reader, bool valueSort, bool checkIterator)
    {
        future_lite::executors::SimpleExecutor ex(1);
        autil::TimeoutTerminator timeoutTerminator;
#define GetSync(key, value, ts)                                                                                        \
    future_lite::interface::syncAwait(reader->Get(key, value, ts, _pool.get(), nullptr, &timeoutTerminator), &ex)

        using Type = typename indexlib::index::FieldTypeTraits<ft>::AttrItemType;
        PackAttributeFormatter formatter;
        auto [status, packAttrConfig] = _indexConfig->GetValueConfig()->CreatePackAttributeConfig();
        ASSERT_TRUE(status.IsOK());
        ASSERT_TRUE(formatter.Init(packAttrConfig));
        auto* ref1 = formatter.GetAttributeReferenceTyped<int64_t>("f1");
        ASSERT_TRUE(ref1 != nullptr);
        using Type = typename indexlib::index::FieldTypeTraits<ft>::AttrItemType;
        auto* ref2 = formatter.GetAttributeReferenceTyped<Type>("f2");
        ASSERT_TRUE(ref2 != nullptr);

        // check iterator
        if (checkIterator) {
            auto iter = reader->CreateIterator();
            ASSERT_TRUE(iter.get() != nullptr);
            int64_t offset = iter->GetOffset();
            CheckIterator(iter.get(), valueSort);

            ASSERT_TRUE(iter->Seek(offset).IsOK());
            CheckIterator(iter.get(), valueSort);

            ASSERT_TRUE(iter->Seek(std::numeric_limits<uint32_t>::max()).IsOutOfRange());
        }

        // check reader
        autil::StringView value;
        uint64_t ts;
        int64_t v1;
        Type v2;
        ASSERT_EQ(GetSync(1, value, ts), indexlib::util::OK);
        ASSERT_TRUE(ref1->GetValue(value.data(), v1));
        ASSERT_TRUE(ref2->GetValue(value.data(), v2));
        ASSERT_EQ(111, v1);
        if constexpr (ft != ft_string) {
            ASSERT_EQ(1, v2);
        } else {
            ASSERT_EQ("1", std::string(v2.data(), v2.size()));
        }
        if (_indexConfig->TTLEnabled()) {
            ASSERT_EQ(101, ts);
        }

        ASSERT_EQ(GetSync(2, value, ts), indexlib::util::DELETED);
        if (_indexConfig->TTLEnabled()) {
            ASSERT_EQ(104, ts);
        }

        ASSERT_EQ(GetSync(3, value, ts), indexlib::util::OK);
        ASSERT_TRUE(ref1->GetValue(value.data(), v1));
        ASSERT_TRUE(ref2->GetValue(value.data(), v2));
        ASSERT_EQ(333, v1);
        if constexpr (ft != ft_string) {
            ASSERT_EQ(3, v2);
        } else {
            ASSERT_EQ("3", std::string(v2.data(), v2.size()));
        }
        if (_indexConfig->TTLEnabled()) {
            ASSERT_EQ(103, ts);
        }

        ASSERT_EQ(GetSync(4, value, ts), indexlib::util::NOT_FOUND);
        ASSERT_EQ(GetSync(100, value, ts), indexlib::util::NOT_FOUND);
#undef GetSync
    }

    void CheckIterator(IKVIterator* iter, bool valueSort)
    {
        Record record;
        ASSERT_TRUE(iter->HasNext());
        auto s = iter->Next(_pool.get(), record);
        ASSERT_TRUE(s.IsOK()) << s.ToString();
        ASSERT_EQ(2, record.key);
        ASSERT_TRUE(record.deleted);
        ASSERT_TRUE(iter->HasNext());
        s = iter->Next(_pool.get(), record);
        ASSERT_TRUE(s.IsOK()) << s.ToString();
        if (!valueSort) {
            ASSERT_EQ(1, record.key);
        } else {
            ASSERT_EQ(3, record.key);
        }
        ASSERT_FALSE(record.deleted);
        ASSERT_TRUE(iter->HasNext());
        s = iter->Next(_pool.get(), record);
        ASSERT_TRUE(s.IsOK()) << s.ToString();
        if (!valueSort) {
            ASSERT_EQ(3, record.key);
        } else {
            ASSERT_EQ(1, record.key);
        }
        ASSERT_FALSE(record.deleted);
        ASSERT_FALSE(iter->HasNext());
    }

    template <FieldType ft>
    void CheckMemoryReader(VarLenKVMemIndexer* indexer)
    {
        auto reader = indexer->CreateInMemoryReader();
        ASSERT_TRUE(reader);
        CheckReader<ft>(reader.get(), false, false);
    }

    template <FieldType ft>
    void CheckDiskReader(bool loadToMemory, bool sort)
    {
        auto dir = PrepareOnlineDirectory(loadToMemory);
        ASSERT_TRUE(dir);
        auto diskIndexer = std::make_unique<KVDiskIndexer>();
        auto s = diskIndexer->Open(_indexConfig, dir->GetIDirectory());
        ASSERT_TRUE(s.IsOK()) << s.ToString();
        auto reader = diskIndexer->GetReader();
        ASSERT_TRUE(reader);
        if (_indexConfig->GetIndexPreference().GetValueParam().EnableFileCompress()) {
            ASSERT_TRUE(std::dynamic_pointer_cast<VarLenKVCompressedLeafReader>(reader));
        } else {
            ASSERT_TRUE(std::dynamic_pointer_cast<VarLenKVLeafReader>(reader));
        }
        CheckReader<ft>(reader.get(), sort, true);
    }

    void CheckBuildMemoryFull()
    {
        std::string fields = "key:uint32;f1:int64;f2:int8";
        MakeSchema(fields, "key", "f1;f2");

        // 16 byte is for hash table header, 32 byte is for two hash bucket, 0.01 is keyValueSizeRatio.
        std::unique_ptr<VarLenKVMemIndexer> indexerPtr = std::make_unique<VarLenKVMemIndexer>((16 + 32) * 100, 0.01);
        VarLenKVMemIndexer& indexer = *indexerPtr;
        ASSERT_TRUE(indexer.Init(_indexConfig, nullptr).IsOK());

        auto& kvIndexPreference = _indexConfig->GetIndexPreference();
        auto dictParam = kvIndexPreference.GetHashDictParam();
        // occupancyPct is 50, so hash table will return full after insert one key
        dictParam.SetOccupancyPct(50);
        kvIndexPreference.SetHashDictParam(dictParam);

        // build
        std::string docStr = "cmd=add,key=1,f1=101,f2=998,ts=101000000;"
                             "cmd=add,key=1,f1=102,f2=999,ts=101000000;";
        auto rawDocs = document::RawDocumentMaker::MakeBatch(docStr);
        uint32_t i = 0;
        for (const auto& rawDoc : rawDocs) {
            auto docBatch = document::KVDocumentBatchMaker::Make(_schema, {rawDoc});
            ASSERT_TRUE(docBatch);
            auto s = indexer.Build(docBatch.get());
            if (i == 0) {
                ASSERT_TRUE(s.IsOK()) << s.ToString();
            }
            if (i == 1) {
                ASSERT_TRUE(s.IsNeedDump()) << s.ToString();
            }
            i++;
        }
    }

    template <FieldType ft>
    void DoTestBuildAndRead(bool sortValue = false)
    {
        auto f2TypeStr = indexlibv2::config::FieldConfig::FieldTypeToStr(ft);
        std::string fields = "key:uint32;f1:int64;f2:" + std::string(f2TypeStr);
        MakeSchema(fields, "key", "f1;f2");

        std::shared_ptr<indexlibv2::framework::EpochBasedMemReclaimer> memReclaimer;
        if (_enableMemoryReclaim) {
            int reclaimFreq = 0;
            memReclaimer = std::make_shared<indexlibv2::framework::EpochBasedMemReclaimer>(reclaimFreq, nullptr);
        }

        std::unique_ptr<VarLenKVMemIndexer> indexerPtr;
        if (sortValue) {
            std::shared_ptr<KVSortDataCollector> sortDataCollector(new KVSortDataCollector);
            config::SortDescriptions sortDescriptions;
            config::SortDescription sortDescription("f1", config::sp_desc);
            sortDescriptions.push_back(sortDescription);
            indexerPtr = std::make_unique<VarLenKVMemIndexer>(
                DEFAULT_MEMORY_USE_IN_BYTES, VarLenKVMemIndexer::DEFAULT_KEY_VALUE_MEM_RATIO,
                VarLenKVMemIndexer::DEFAULT_VALUE_COMPRESSION_RATIO, sortDescriptions, memReclaimer.get());
        } else {
            indexerPtr = std::make_unique<VarLenKVMemIndexer>(
                DEFAULT_MEMORY_USE_IN_BYTES, VarLenKVMemIndexer::DEFAULT_KEY_VALUE_MEM_RATIO,
                VarLenKVMemIndexer::DEFAULT_VALUE_COMPRESSION_RATIO, memReclaimer.get());
        }
        VarLenKVMemIndexer& indexer = *indexerPtr;
        ASSERT_TRUE(indexer.Init(_indexConfig, nullptr).IsOK());

        // build
        std::string docStr = "cmd=add,key=1,f1=101,f2=998,ts=101000000;"
                             "cmd=add,key=1,f1=102,f2=999,ts=101000000;"
                             "cmd=add,key=2,f1=222,f2=2,ts=102000000;"
                             "cmd=add,key=1,f1=111,f2=1,ts=101000000;"
                             "cmd=add,key=3,f1=333,f2=888,ts=103000000;"
                             "cmd=delete,key=2,ts=104000000;"
                             "cmd=add,key=3,f1=333,f2=777,ts=103000000;"
                             "cmd=delete,key=3,ts=103000000;"
                             "cmd=add,key=3,f1=333,f2=3,ts=103000000;"
                             "cmd=add,key=2,f1=222,f2=666,ts=102000000;"
                             "cmd=delete,key=2,ts=104000000;";
        auto rawDocs = document::RawDocumentMaker::MakeBatch(docStr);
        for (const auto& rawDoc : rawDocs) {
            auto docBatch = document::KVDocumentBatchMaker::Make(_schema, {rawDoc});
            ASSERT_TRUE(docBatch);
            auto s = indexer.Build(docBatch.get());
            if (memReclaimer) {
                memReclaimer->TryReclaim();
            }
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
        ASSERT_LT(0, groupMetrics->Get<size_t>(SegmentStatistics::KV_VALUE_MEM_USE));
        ASSERT_EQ(groupMetrics->Get<size_t>(SegmentStatistics::KV_SEGMENT_MEM_USE),
                  groupMetrics->Get<size_t>(SegmentStatistics::KV_HASH_MEM_USE) +
                      groupMetrics->Get<size_t>(SegmentStatistics::KV_VALUE_MEM_USE));

        CheckMemoryReader<ft>(&indexer);

        // dump
        auto s = indexer.Dump(_pool.get(), _directory, nullptr);
        ASSERT_TRUE(s.IsOK()) << s.ToString();
        ASSERT_TRUE(_directory->IsExist("key/key"));
        ASSERT_TRUE(_directory->IsExist("key/value"));
        ASSERT_TRUE(_directory->IsExist("key/format_option"));

        CheckDiskReader<ft>(/*loadToMemory*/ true, sortValue);
        CheckDiskReader<ft>(/*loadToMemory*/ false, sortValue);
    }

protected:
    std::string _testDataPath;
    std::shared_ptr<config::ITabletSchema> _schema;
    std::shared_ptr<indexlibv2::config::KVIndexConfig> _indexConfig;
    std::unique_ptr<autil::mem_pool::Pool> _pool;
    std::shared_ptr<indexlib::file_system::Directory> _directory;
    bool _enableMemoryReclaim = false;

protected:
    static constexpr int64_t DEFAULT_MEMORY_USE_IN_BYTES = 1024 * 1024;
};

} // namespace indexlibv2::index
