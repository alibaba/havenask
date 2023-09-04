#include "indexlib/index/kv/test/MultiSegmentKVIteratorTestBase.h"

#include "indexlib/config/UnresolvedSchema.h"
#include "indexlib/document/raw_document/test/RawDocumentMaker.h"
#include "indexlib/document/test/KVDocumentBatchMaker.h"
#include "indexlib/file_system/MountOption.h"
#include "indexlib/framework/mock/FakeSegment.h"

namespace indexlibv2::index {

void MultiSegmentKVIteratorTestBase::setUp()
{
    // build directory
    _rootDir = GET_TEMPLATE_DATA_PATH();
    auto [s, fs] = indexlib::file_system::FileSystemCreator::CreateForWrite("ut", _rootDir).StatusWith();
    ASSERT_TRUE(s.IsOK()) << s.ToString();
    _indexRootDir = indexlib::file_system::Directory::Get(fs);
    ASSERT_TRUE(_indexRootDir);

    // schema
    auto fieldNames = "key:uint32;f1:int64;f2:int8;f3:string";
    auto result = KVIndexConfigBuilder(fieldNames, "key", "f1;f2;f3").Finalize();
    _schema = std::move(result.first);
    _config = std::move(result.second);
    ASSERT_TRUE(_schema);
    ASSERT_TRUE(_config);

    _formatter = std::make_unique<PackAttributeFormatter>();
    auto [status, packAttrConfig] = _config->GetValueConfig()->CreatePackAttributeConfig();
    ASSERT_TRUE(status.IsOK()) << status.ToString();
    ASSERT_TRUE(_formatter->Init(packAttrConfig));

    _indexerParam.maxMemoryUseInBytes = 64 * 1024 * 1024; // 64M
    _indexerParam.sortDescriptions.push_back(config::SortDescription("f1", config::sp_desc));
    _indexerParam.sortDescriptions.push_back(config::SortDescription("f2", config::sp_asc));
    ASSERT_TRUE(_schema->TEST_GetImpl()->SetRuntimeSetting("sort_descriptions", _indexerParam.sortDescriptions, true));

    _pool = std::make_unique<autil::mem_pool::Pool>();
}

void MultiSegmentKVIteratorTestBase::PrepareSegments(const std::vector<std::string>& segmentDocsVec,
                                                     std::vector<std::shared_ptr<framework::Segment>>& segments)
{
    BuildIndex(segmentDocsVec);

    indexlib::file_system::LoadConfigList loadConfigList;
    loadConfigList.PushBack(indexlib::file_system::LoadConfigListCreator::MakeBlockLoadConfig(
        "lru", 1024, 16, indexlib::file_system::CacheLoadStrategy::DEFAULT_CACHE_IO_BATCH_SIZE, false, false));
    indexlib::file_system::FileSystemOptions options;
    options.loadConfigList = loadConfigList;
    options.enableAsyncFlush = false;
    options.needFlush = true;
    options.useCache = true;
    options.useRootLink = false;
    options.isOffline = true;
    auto fs = indexlib::file_system::FileSystemCreator::Create("ut", _rootDir, options).GetOrThrow();
    ASSERT_TRUE(fs);
    auto ec = fs->MountDir(_rootDir, "", "", indexlib::file_system::FSMT_READ_WRITE, true);
    ASSERT_EQ(indexlib::file_system::FSEC_OK, ec);
    _onlineDirectory = indexlib::file_system::Directory::Get(fs);
    ASSERT_TRUE(_onlineDirectory);

    for (size_t i = 0; i < segmentDocsVec.size(); ++i) {
        auto segment = LoadSegment(_onlineDirectory, i);
        ASSERT_TRUE(segment) << "Load Segment " << i << " failed";
        segments.emplace_back(std::move(segment));
    }
}

void MultiSegmentKVIteratorTestBase::BuildIndex(const std::vector<std::string>& segmentDocsVec) const
{
    for (size_t i = 0; i < segmentDocsVec.size(); ++i) {
        BuildOneSegment(segmentDocsVec[i], i);
    }
}

void MultiSegmentKVIteratorTestBase::BuildOneSegment(const std::string& docsStr, size_t segmentId) const
{
    KVIndexFactory factory;
    auto indexer = factory.CreateMemIndexer(_config, _indexerParam);
    ASSERT_TRUE(indexer);
    auto s = indexer->Init(_config, nullptr);
    ASSERT_TRUE(s.IsOK()) << s.ToString();

    auto rawDocs = document::RawDocumentMaker::MakeBatch(docsStr);
    for (const auto& rawDoc : rawDocs) {
        auto docBatch = document::KVDocumentBatchMaker::Make(_schema, {rawDoc});
        ASSERT_TRUE(docBatch);
        auto s = indexer->Build(docBatch.get());
        ASSERT_TRUE(s.IsOK()) << s.ToString();
    }

    auto segmentDir = MakeSegmentDirectory(segmentId);
    ASSERT_TRUE(segmentDir) << segmentId;

    s = indexer->Dump(_pool.get(), segmentDir, nullptr);
    ASSERT_TRUE(s.IsOK()) << s.ToString();
}

std::shared_ptr<indexlib::file_system::Directory> MultiSegmentKVIteratorTestBase::MakeSegmentDirectory(size_t id) const
{
    return _indexRootDir->MakeDirectory("segment_" + std::to_string(id));
}

std::shared_ptr<framework::Segment>
MultiSegmentKVIteratorTestBase::LoadSegment(const std::shared_ptr<indexlib::file_system::Directory>& dir, size_t id,
                                            bool addIndex) const
{
    auto dirName = "segment_" + std::to_string(id);
    auto segDir = dir->GetDirectory(dirName, false);
    if (!segDir) {
        return nullptr;
    }
    if (addIndex) {
        segDir = segDir->GetDirectory("index", false);
    }
    if (!segDir) {
        return nullptr;
    }
    auto indexer = std::make_shared<KVDiskIndexer>();
    auto s = indexer->Open(_config, segDir->GetIDirectory());
    if (!s.IsOK()) {
        return nullptr;
    }
    auto segment = std::make_shared<framework::FakeSegment>(id, _schema);
    segment->AddIndexer(_config->GetIndexType(), _config->GetIndexName(), std::move(indexer));
    return segment;
}

void MultiSegmentKVIteratorTestBase::CheckIterator(MultiSegmentKVIterator* iter,
                                                   const std::vector<std::pair<keytype_t, bool>>& keys,
                                                   const std::vector<std::vector<std::string>>& values) const
{
    ASSERT_EQ(keys.size(), values.size());
    for (auto i = 0; i < keys.size(); ++i) {
        ASSERT_TRUE(iter->HasNext());
        Record r;
        auto s = iter->Next(_pool.get(), r);
        ASSERT_TRUE(s.IsOK()) << s.ToString();
        ASSERT_EQ(keys[i].first, r.key);
        ASSERT_EQ(keys[i].second, r.deleted);
        if (r.deleted) {
            continue;
        }
        size_t countLen = 0;
        auto count = autil::MultiValueFormatter::decodeCount(r.value.data(), countLen);
        auto packData = autil::StringView(r.value.data() + countLen, count);
        FieldValueExtractor extractor(_formatter.get(), std::move(packData), _pool.get());
        const auto& valueR = values[i];
        for (size_t j = 0; j < valueR.size(); ++j) {
            std::string v;
            ASSERT_TRUE(extractor.GetStringValue(j, v));
            ASSERT_EQ(valueR[j], v) << keys[i].first;
        }
    }
}

} // namespace indexlibv2::index
