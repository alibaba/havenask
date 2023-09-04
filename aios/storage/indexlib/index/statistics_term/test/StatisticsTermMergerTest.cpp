#include "indexlib/index/statistics_term/StatisticsTermMerger.h"

#include "indexlib/document/normal/IndexDocument.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/load_config/MmapLoadStrategy.h"
#include "indexlib/framework/DumpParams.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/statistics_term/Constant.h"
#include "indexlib/index/statistics_term/StatisticsTermDiskIndexer.h"
#include "indexlib/index/statistics_term/StatisticsTermIndexConfig.h"
#include "indexlib/index/statistics_term/StatisticsTermMemIndexer.h"
#include "unittest/unittest.h"

namespace indexlibv2::index {

class FakeSegment final : public framework::Segment
{
public:
    FakeSegment(const framework::SegmentMeta& segMeta, const std::shared_ptr<StatisticsTermDiskIndexer>& indexer)
        : framework::Segment(framework::Segment::SegmentStatus::ST_BUILT)
        , _indexer(indexer)
    {
        _segmentMeta = segMeta;
    }

public:
    std::pair<Status, std::shared_ptr<IIndexer>> GetIndexer(const std::string& type,
                                                            const std::string& indexName) override
    {
        return std::make_pair(Status::OK(), _indexer);
    }
    size_t EstimateMemUsed(const std::shared_ptr<config::ITabletSchema>& schema) override { return 0; }
    size_t EvaluateCurrentMemUsed() override { return 0; }

private:
    std::shared_ptr<StatisticsTermDiskIndexer> _indexer;
};

class StatisticsTermMergerTest : public TESTBASE
{
public:
    void setUp() override;

private:
    void PrepareSrcData(IIndexMerger::SegmentMergeInfos& segMergeInfos);
    void PrepareMemIndexerData(const std::shared_ptr<indexlib::file_system::Directory>& indexDir);
    void CheckHashMap(const std::string& invertIndexName,
                      const std::shared_ptr<indexlib::file_system::Directory>& segDir,
                      const std::unordered_map<size_t, std::string>& expectHashMap);

private:
    std::string _rootPath;
    std::shared_ptr<indexlib::file_system::Directory> _rootDir;
    std::shared_ptr<StatisticsTermIndexConfig> _statConfig;
    autil::mem_pool::Pool _pool;
};

void StatisticsTermMergerTest::setUp()
{
    _rootPath = GET_TEMP_DATA_PATH() + "/";
    if (!_rootPath.empty()) {
        indexlib::file_system::FslibWrapper::DeleteDirE(_rootPath, indexlib::file_system::DeleteOption::NoFence(true));
    }

    indexlib::file_system::FileSystemOptions fsOptions;
    fsOptions.needFlush = true;

    auto loadStrategy =
        std::make_shared<indexlib::file_system::MmapLoadStrategy>(/*lock*/ true, false, 4 * 1024 * 1024, 0);
    indexlib::file_system::LoadConfig::FilePatternStringVector pattern;
    pattern.push_back(".*");

    indexlib::file_system::LoadConfig loadConfig;
    loadConfig.SetLoadStrategyPtr(loadStrategy);
    loadConfig.SetFilePatternString(pattern);
    loadConfig.SetLoadStrategyPtr(loadStrategy);
    loadConfig.SetName("__OFFLINE_ATTRIBUTE__");

    indexlib::file_system::LoadConfigList loadConfigList;
    loadConfigList.PushFront(loadConfig);

    fsOptions.loadConfigList = loadConfigList;
    auto fs = indexlib::file_system::FileSystemCreator::Create("ut", _rootPath, fsOptions).GetOrThrow();
    _rootDir = indexlib::file_system::Directory::Get(fs);

    auto jsonStr = R"(
{
    "index_name" : "statistics_term",
    "index_type" : "statistics_term",
    "associated_inverted_index" : ["content_id1", "content_id2"]
}
)";
    _statConfig = std::make_unique<StatisticsTermIndexConfig>();
    ASSERT_NO_THROW(FromJsonString(*_statConfig, jsonStr));
}

void StatisticsTermMergerTest::PrepareMemIndexerData(const std::shared_ptr<indexlib::file_system::Directory>& indexDir)
{
    auto params = std::make_shared<framework::DumpParams>();
    std::map<std::string, size_t> emptyInitialKeyCount;
    auto indexer = std::make_unique<StatisticsTermMemIndexer>(emptyInitialKeyCount);
    ASSERT_TRUE(indexer->Init(_statConfig, /*docInfoExtractorFactory*/ nullptr).IsOK());

    static size_t idx = 0;
    auto getTermHash = [&]() -> size_t { return 1000000 + idx; };
    auto getTermStr = [&]() -> std::string { return std::to_string(1000000 + idx); };
    auto getNextTermData = [&]() -> std::unordered_map<size_t, std::string> {
        std::unordered_map<size_t, std::string> hashMap;
        for (size_t j = 0; j < 3; ++j) {
            auto termHash = getTermHash();
            auto termStr = getTermStr();
            hashMap[termHash] = termStr;
            ++idx;
        }
        return hashMap;
    };
    for (size_t i = 0; i < 1; ++i) {
        auto indexDoc = std::make_shared<indexlib::document::IndexDocument>(&_pool);
        indexlib::document::IndexDocument::TermOriginValueMap termMap;
        termMap["content_id1"] = getNextTermData();
        termMap["content_id2"] = getNextTermData();
        indexDoc->_termOriginValueMap = termMap;
        ASSERT_TRUE(indexer->AddDocument(indexDoc).IsOK());
    }

    ASSERT_TRUE(indexer->Dump(/*dumpPool*/ nullptr, indexDir, params).IsOK());
}

void StatisticsTermMergerTest::PrepareSrcData(IIndexMerger::SegmentMergeInfos& segMergeInfos)
{
    auto partDir = _rootDir->MakeDirectory("partition_0_65535");
    auto segDir1 = partDir->MakeDirectory("segment_50001_level_0");
    auto indexDir1 = segDir1->MakeDirectory(index::STATISTICS_TERM_INDEX_PATH);

    auto segDir2 = partDir->MakeDirectory("segment_50002_level_0");
    auto indexDir2 = segDir2->MakeDirectory(index::STATISTICS_TERM_INDEX_PATH);

    PrepareMemIndexerData(indexDir1);
    PrepareMemIndexerData(indexDir2);
    [[maybe_unused]] auto ret = partDir->Sync(/*waitFinish*/ true);

    auto diskIndexer1 = std::make_shared<StatisticsTermDiskIndexer>();
    auto diskIndexer2 = std::make_shared<StatisticsTermDiskIndexer>();
    ASSERT_TRUE(diskIndexer1->Open(_statConfig, indexDir1->GetIDirectory()).IsOK());
    ASSERT_TRUE(diskIndexer2->Open(_statConfig, indexDir2->GetIDirectory()).IsOK());

    framework::SegmentMeta segMeta1;
    segMeta1.segmentId = 50001;
    segMeta1.segmentDir = segDir1;

    framework::SegmentMeta segMeta2;
    segMeta2.segmentId = 50002;
    segMeta2.segmentDir = segDir2;

    auto seg1 = std::make_shared<FakeSegment>(segMeta1, diskIndexer1);
    auto seg2 = std::make_shared<FakeSegment>(segMeta2, diskIndexer2);

    segMergeInfos.srcSegments.push_back({0, seg1});
    segMergeInfos.srcSegments.push_back({0, seg2});

    // prepare target
    auto segMeta3 = std::make_shared<framework::SegmentMeta>();
    segMeta3->segmentId = 50003;
    segMeta3->segmentDir = partDir->MakeDirectory("segment_50003_level_0");
    auto segMeta4 = std::make_shared<framework::SegmentMeta>();
    segMeta4->segmentId = 50004;
    segMeta4->segmentDir = partDir->MakeDirectory("segment_50004_level_0");
    segMergeInfos.targetSegments = {segMeta3, segMeta4};
}

void StatisticsTermMergerTest::CheckHashMap(const std::string& invertIndexName,
                                            const std::shared_ptr<indexlib::file_system::Directory>& segDir,
                                            const std::unordered_map<size_t, std::string>& expectHashMap)
{
    auto indexDir = segDir->GetDirectory(index::STATISTICS_TERM_INDEX_PATH, /*throw if not exist*/ true);
    auto diskIndexer = std::make_shared<StatisticsTermDiskIndexer>();
    ASSERT_TRUE(diskIndexer->Open(_statConfig, indexDir->GetIDirectory()).IsOK());
    auto leafReader = diskIndexer->GetReader();
    ASSERT_EQ(2, leafReader->_termMetas.size());

    std::unordered_map<size_t, std::string> termStatistics;
    ASSERT_TRUE(leafReader->GetTermStatistics(invertIndexName, termStatistics).IsOK());
    ASSERT_EQ(expectHashMap.size(), termStatistics.size());
    for (const auto& [hash, str] : expectHashMap) {
        auto it = termStatistics.find(hash);
        ASSERT_TRUE(it != termStatistics.end());
        ASSERT_EQ(str, it->second);
    }
}

/*
  seg1: "content_id1" -> {{1000000, "1000000"}, {1000001, "1000001"}, {1000002, "1000002"}}
        "content_id2" -> {{1000003, "1000003"}, {1000004, "1000004"}, {1000005, "1000005"}}
  seg2: "content_id1" -> {{1000006, "1000006"}, {1000007, "1000007"}, {1000008, "1000008"}}
        "content_id2" -> {{1000009, "1000009"}, {1000010, "1000010"}, {1000011, "1000011"}}
 */
TEST_F(StatisticsTermMergerTest, testSimple)
{
    IIndexMerger::SegmentMergeInfos segMergeInfos;
    PrepareSrcData(segMergeInfos);
    auto indexMerger = std::make_unique<StatisticsTermMerger>();
    std::map<std::string, std::any> params;
    ASSERT_TRUE(indexMerger->Init(_statConfig, params).IsOK());
    ASSERT_TRUE(indexMerger->Merge(segMergeInfos, /*task resouce*/ nullptr).IsOK());

    std::unordered_map<size_t, std::string> termStatistics;
    // check src segment1
    termStatistics = {{1000000, "1000000"}, {1000001, "1000001"}, {1000002, "1000002"}};
    CheckHashMap("content_id1", segMergeInfos.srcSegments[0].segment->_segmentMeta.segmentDir, termStatistics);

    termStatistics = {{1000003, "1000003"}, {1000004, "1000004"}, {1000005, "1000005"}};
    CheckHashMap("content_id2", segMergeInfos.srcSegments[0].segment->_segmentMeta.segmentDir, termStatistics);

    // check src segment2
    termStatistics = {{1000006, "1000006"}, {1000007, "1000007"}, {1000008, "1000008"}};
    CheckHashMap("content_id1", segMergeInfos.srcSegments[1].segment->_segmentMeta.segmentDir, termStatistics);

    termStatistics = {{1000009, "1000009"}, {1000010, "1000010"}, {1000011, "1000011"}};
    CheckHashMap("content_id2", segMergeInfos.srcSegments[1].segment->_segmentMeta.segmentDir, termStatistics);

    // check target segment1
    termStatistics = {{1000000, "1000000"}, {1000001, "1000001"}, {1000002, "1000002"},
                      {1000006, "1000006"}, {1000007, "1000007"}, {1000008, "1000008"}};
    CheckHashMap("content_id1", segMergeInfos.targetSegments[0]->segmentDir, termStatistics);

    termStatistics = {{1000003, "1000003"}, {1000004, "1000004"}, {1000005, "1000005"},
                      {1000009, "1000009"}, {1000010, "1000010"}, {1000011, "1000011"}};
    CheckHashMap("content_id2", segMergeInfos.targetSegments[0]->segmentDir, termStatistics);

    auto targetIndexDir2 = segMergeInfos.targetSegments[1]->segmentDir->GetDirectory(index::STATISTICS_TERM_INDEX_PATH,
                                                                                     /*throw if not exist*/ false);
    ASSERT_TRUE(targetIndexDir2 == nullptr);
}

} // namespace indexlibv2::index
