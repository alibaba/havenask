#include "indexlib/table/plain/MultiShardMemSegment.h"

#include "indexlib/base/MemoryQuotaController.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/document/DocumentBatch.h"
#include "indexlib/document/extractor/IDocumentInfoExtractorFactory.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/framework/MetricsManager.h"
#include "indexlib/framework/SegmentDescriptions.h"
#include "indexlib/framework/mock/MockTabletSchema.h"
#include "indexlib/index/BuildingIndexMemoryUseUpdater.h"
#include "indexlib/index/IIndexFactory.h"
#include "indexlib/index/IIndexReader.h"
#include "indexlib/index/common/ShardPartitioner.h"
#include "indexlib/index/mock/MockIndexConfig.h"
#include "indexlib/index/mock/MockIndexFactory.h"
#include "indexlib/index/mock/MockMemIndexer.h"
#include "indexlib/table/plain/MultiShardSegmentMetrics.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2 { namespace plain {

class MultiShardMemSegmentTest : public TESTBASE
{
public:
    MultiShardMemSegmentTest() = default;
    ~MultiShardMemSegmentTest() = default;

    void setUp() override
    {
        indexlib::file_system::FileSystemOptions fsOptions;
        std::string rootPath = GET_TEMP_DATA_PATH();
        auto fs = indexlib::file_system::FileSystemCreator::Create("online", rootPath, fsOptions).GetOrThrow();
        _rootDir = indexlib::file_system::Directory::Get(fs);
    }
    void tearDown() override {}

private:
    std::shared_ptr<indexlib::file_system::Directory> _rootDir;
};

TEST_F(MultiShardMemSegmentTest, TestOpen)
{
    auto tabletOptions = std::make_shared<config::TabletOptions>();
    tabletOptions->SetIsOnline(true);

    std::vector<std::shared_ptr<config::IIndexConfig>> indexConfigs;
    std::string mockKV = "mock_kv";
    const std::string indexName = "kv";
    auto indexConfig = std::make_shared<config::MockIndexConfig>();
    EXPECT_CALL(*indexConfig, GetIndexType).WillRepeatedly(testing::ReturnRef(mockKV));
    EXPECT_CALL(*indexConfig, GetIndexName).WillRepeatedly(testing::ReturnRef(indexName));
    indexConfigs.push_back(indexConfig);

    auto schema = std::make_shared<framework::MockTabletSchema>();
    auto indexer = std::make_shared<index::MockMemIndexer>();
    EXPECT_CALL(*indexer, UpdateMemUse).WillRepeatedly(Return());
    EXPECT_CALL(*indexer, Init).WillOnce(Return(Status::InternalError())).WillRepeatedly(Return(Status::OK()));
    framework::BuildResource buildResource;
    buildResource.memController = std::make_shared<MemoryQuotaController>("name", 1024);
    buildResource.buildResourceMetrics = std::make_shared<indexlib::util::BuildResourceMetrics>();
    buildResource.buildResourceMetrics->Init();
    auto tabletFactoryCreator = index::IndexFactoryCreator::GetInstance();

    {
        EXPECT_CALL(*schema, GetIndexConfigs()).WillRepeatedly(testing::Return(indexConfigs));
        auto mockFactory = std::make_unique<index::MockIndexFactory>();
        tabletFactoryCreator->_factorysMap[mockKV] = std::make_pair(Status::OK(), mockFactory.get());
        EXPECT_CALL(*mockFactory, CreateMemIndexer).WillRepeatedly(Return(indexer));
        framework::SegmentMeta segmentMeta;
        segmentMeta.segmentDir = _rootDir->MakeDirectory("segment_0_level_0");
        segmentMeta.segmentId = 0;
        segmentMeta.segmentInfo = std::make_shared<framework::SegmentInfo>();
        segmentMeta.segmentInfo->SetShardCount(1);
        auto memSegment = std::make_unique<plain::MultiShardMemSegment>(
            /*shardPartitioner*/ nullptr, /*levelNum*/ 2, tabletOptions.get(), schema, segmentMeta);
        auto status = memSegment->Open(buildResource, /*lastSegMetrics=*/nullptr);
        ASSERT_TRUE(status.IsInternalError());
        tabletFactoryCreator->_factorysMap.erase(mockKV);
    }
    {
        EXPECT_CALL(*schema, GetIndexConfigs()).WillRepeatedly(testing::Return(indexConfigs));
        auto mockFactory = std::make_unique<index::MockIndexFactory>();
        tabletFactoryCreator->_factorysMap[mockKV] = std::make_pair(Status::OK(), mockFactory.get());
        EXPECT_CALL(*mockFactory, CreateMemIndexer).WillRepeatedly(Return(indexer));
        framework::SegmentMeta segmentMeta;
        segmentMeta.segmentDir = _rootDir->MakeDirectory("segment_1_level_0");
        segmentMeta.segmentId = 1;
        segmentMeta.segmentInfo = std::make_shared<framework::SegmentInfo>();
        segmentMeta.segmentInfo->SetShardCount(1);
        auto memSegment = std::make_unique<plain::MultiShardMemSegment>(
            /*shardPartitioner*/ nullptr, /*levelNum*/ 2, tabletOptions.get(), schema, segmentMeta);
        auto status = memSegment->Open(buildResource, /*lastSegMetrics=*/nullptr);
        ASSERT_TRUE(status.IsOK());
        tabletFactoryCreator->_factorysMap.erase(mockKV);
    }
    {
        EXPECT_CALL(*schema, GetIndexConfigs()).WillRepeatedly(testing::Return(indexConfigs));
        auto mockFactory = std::make_unique<index::MockIndexFactory>();
        tabletFactoryCreator->_factorysMap[mockKV] = std::make_pair(Status::OK(), mockFactory.get());
        EXPECT_CALL(*mockFactory, CreateMemIndexer).WillRepeatedly(Return(indexer));
        auto partitioner = std::make_shared<index::ShardPartitioner>();
        uint32_t shardNum = 2;
        uint32_t levelNum = 2;
        ASSERT_TRUE(partitioner->Init(indexlib::HashFunctionType::hft_murmur, shardNum).IsOK());
        framework::SegmentMeta segmentMeta;
        segmentMeta.segmentDir = _rootDir->MakeDirectory("segment_0_level_1");
        segmentMeta.segmentId = 0;
        segmentMeta.segmentInfo = std::make_shared<framework::SegmentInfo>();
        segmentMeta.segmentInfo->SetShardCount(shardNum);
        auto memSegment = std::make_unique<plain::MultiShardMemSegment>(partitioner, levelNum, tabletOptions.get(),
                                                                        schema, segmentMeta);
        auto status = memSegment->Open(buildResource, /*lastSegMetrics=*/nullptr);
        ASSERT_TRUE(status.IsOK());
        tabletFactoryCreator->_factorysMap.erase(mockKV);
    }
    {
        auto partitioner = std::make_shared<index::ShardPartitioner>();
        ASSERT_TRUE(partitioner->Init(indexlib::HashFunctionType::hft_murmur, 3).IsInvalidArgs());
    }
}

TEST_F(MultiShardMemSegmentTest, TestBuild)
{
    auto tabletOptions = std::make_shared<config::TabletOptions>();
    tabletOptions->SetIsOnline(true);

    std::vector<std::shared_ptr<config::IIndexConfig>> indexConfigs;
    auto indexConfig = std::make_shared<config::MockIndexConfig>();
    const std::string indexName = "mock_kv";
    EXPECT_CALL(*indexConfig, GetIndexType).WillRepeatedly(testing::ReturnRef(indexName));
    EXPECT_CALL(*indexConfig, GetIndexName).WillRepeatedly(testing::ReturnRef(indexName));
    indexConfigs.push_back(indexConfig);

    auto schema = std::make_shared<framework::MockTabletSchema>();
    EXPECT_CALL(*schema, GetIndexConfigs()).WillRepeatedly(testing::Return(indexConfigs));
    auto indexer = std::make_shared<index::MockMemIndexer>();
    EXPECT_CALL(*indexer, UpdateMemUse).WillRepeatedly(Return());
    EXPECT_CALL(*indexer, Init).WillRepeatedly(Return(Status::OK()));

    framework::BuildResource buildResource;
    buildResource.memController = std::make_shared<MemoryQuotaController>("name", 1024);
    buildResource.buildResourceMetrics = std::make_shared<indexlib::util::BuildResourceMetrics>();
    buildResource.buildResourceMetrics->Init();
    auto tabletFactoryCreator = index::IndexFactoryCreator::GetInstance();
    {
        auto mockFactory = std::make_unique<index::MockIndexFactory>();
        tabletFactoryCreator->_factorysMap[indexName] = std::make_pair(Status::OK(), mockFactory.get());
        EXPECT_CALL(*mockFactory, CreateMemIndexer).WillRepeatedly(Return(indexer));
        uint32_t shardNum = 2;
        uint32_t levelNum = 2;
        auto partitioner = std::make_shared<index::ShardPartitioner>();
        ASSERT_TRUE(partitioner->Init(indexlib::HashFunctionType::hft_murmur, shardNum).IsOK());
        framework::SegmentMeta segmentMeta;
        segmentMeta.segmentDir = _rootDir->MakeDirectory("segment_0_level_0");
        segmentMeta.segmentId = 0;
        segmentMeta.segmentInfo = std::make_shared<framework::SegmentInfo>();
        segmentMeta.segmentInfo->SetShardCount(shardNum);

        auto memSegment = std::make_unique<plain::MultiShardMemSegment>(partitioner, levelNum, tabletOptions.get(),
                                                                        schema, segmentMeta);
        auto status = memSegment->Open(buildResource, /*lastSegMetrics=*/nullptr);

        auto segmentMetrics = memSegment->GetSegmentMetrics();
        auto multiShardSegMetrics =
            std::dynamic_pointer_cast<indexlibv2::plain::MultiShardSegmentMetrics>(segmentMetrics);
        ASSERT_NE(multiShardSegMetrics, nullptr);

        ASSERT_TRUE(status.IsOK());
        std::shared_ptr<document::IDocumentBatch> docBatch(new document::DocumentBatch());
        status = memSegment->Build(docBatch.get());
        ASSERT_TRUE(status.IsOK());
        tabletFactoryCreator->_factorysMap.erase(indexName);
    }
}

TEST_F(MultiShardMemSegmentTest, TestCollectSegmentDescription)
{
    auto tabletOptions = std::make_shared<config::TabletOptions>();
    tabletOptions->SetIsOnline(true);

    std::vector<std::shared_ptr<config::IIndexConfig>> indexConfigs;
    auto indexConfig = std::make_shared<config::MockIndexConfig>();
    const std::string indexName = "mock_kv";
    EXPECT_CALL(*indexConfig, GetIndexType).WillRepeatedly(testing::ReturnRef(indexName));
    EXPECT_CALL(*indexConfig, GetIndexName).WillRepeatedly(testing::ReturnRef(indexName));
    indexConfigs.push_back(indexConfig);

    auto schema = std::make_shared<framework::MockTabletSchema>();
    EXPECT_CALL(*schema, GetIndexConfigs()).WillRepeatedly(testing::Return(indexConfigs));
    auto indexer = std::make_shared<index::MockMemIndexer>();
    EXPECT_CALL(*indexer, UpdateMemUse).WillRepeatedly(Return());
    EXPECT_CALL(*indexer, Init).WillRepeatedly(Return(Status::OK()));

    framework::BuildResource buildResource;
    buildResource.memController = std::make_shared<MemoryQuotaController>("name", 1024);
    buildResource.buildResourceMetrics = std::make_shared<indexlib::util::BuildResourceMetrics>();
    buildResource.buildResourceMetrics->Init();
    auto tabletFactoryCreator = index::IndexFactoryCreator::GetInstance();
    auto segDescs = std::make_shared<framework::SegmentDescriptions>();
    uint32_t shardNum = 2;
    uint32_t levelNum = 2;

    {
        auto mockFactory = std::make_unique<index::MockIndexFactory>();
        tabletFactoryCreator->_factorysMap[indexName] = std::make_pair(Status::OK(), mockFactory.get());
        EXPECT_CALL(*mockFactory, CreateMemIndexer).WillRepeatedly(Return(indexer));
        auto partitioner = std::make_shared<index::ShardPartitioner>();
        ASSERT_TRUE(partitioner->Init(indexlib::HashFunctionType::hft_murmur, shardNum).IsOK());
        framework::SegmentMeta segmentMeta;
        segmentid_t segmentId = 0;
        std::string segmentName = "segment_" + std::to_string(segmentId) + "_level_0";
        segmentMeta.segmentDir = _rootDir->MakeDirectory(segmentName);
        segmentMeta.segmentId = segmentId;
        segmentMeta.segmentInfo = std::make_shared<framework::SegmentInfo>();
        segmentMeta.segmentInfo->SetShardCount(shardNum);
        auto memSegment = std::make_unique<plain::MultiShardMemSegment>(partitioner, levelNum, tabletOptions.get(),
                                                                        schema, segmentMeta);
        auto status = memSegment->Open(buildResource, /*lastSegMetrics=*/nullptr);
        ASSERT_TRUE(status.IsOK());

        auto levelInfo = segDescs->GetLevelInfo();
        ASSERT_TRUE(levelInfo == nullptr);
        memSegment->CollectSegmentDescription(segDescs);
        levelInfo = segDescs->GetLevelInfo();
        ASSERT_TRUE(levelInfo != nullptr);
        auto segmentName2 = segDescs->GetSegmentDir(segmentId);
        ASSERT_EQ(segmentName, segmentName2);
        const auto& levelMetas = levelInfo->levelMetas;
        ASSERT_EQ(levelMetas.size(), 2);
        ASSERT_EQ(levelMetas[0].levelIdx, 0);
        ASSERT_EQ(levelMetas[0].topology, framework::topo_sequence);
        ASSERT_EQ(levelMetas[0].segments.size(), 1);
        ASSERT_EQ(levelMetas[0].segments[0], segmentId);
        ASSERT_EQ(levelMetas[1].topology, framework::topo_hash_mod);
        tabletFactoryCreator->_factorysMap.erase(indexName);
    }
    {
        auto mockFactory = std::make_unique<index::MockIndexFactory>();
        tabletFactoryCreator->_factorysMap[indexName] = std::make_pair(Status::OK(), mockFactory.get());
        EXPECT_CALL(*mockFactory, CreateMemIndexer).WillRepeatedly(Return(indexer));
        auto partitioner = std::make_shared<index::ShardPartitioner>();
        ASSERT_TRUE(partitioner->Init(indexlib::HashFunctionType::hft_murmur, shardNum).IsOK());
        framework::SegmentMeta segmentMeta;
        segmentid_t segmentId = 1;
        std::string segmentName = "segment_" + std::to_string(segmentId) + "_level_0";
        segmentMeta.segmentDir = _rootDir->MakeDirectory(segmentName);
        segmentMeta.segmentId = segmentId;
        segmentMeta.segmentInfo = std::make_shared<framework::SegmentInfo>();
        segmentMeta.segmentInfo->SetShardCount(shardNum);
        auto memSegment = std::make_unique<plain::MultiShardMemSegment>(partitioner, levelNum, tabletOptions.get(),
                                                                        schema, segmentMeta);
        auto status = memSegment->Open(buildResource, /*lastSegMetrics=*/nullptr);
        ASSERT_TRUE(status.IsOK());

        auto levelInfo = segDescs->GetLevelInfo();
        ASSERT_TRUE(levelInfo != nullptr);
        memSegment->CollectSegmentDescription(segDescs);
        auto segmentName2 = segDescs->GetSegmentDir(segmentId);
        ASSERT_EQ(segmentName, segmentName2);
        const auto& levelMetas = levelInfo->levelMetas;
        ASSERT_EQ(levelMetas.size(), 2);
        ASSERT_EQ(levelMetas[0].levelIdx, 0);
        ASSERT_EQ(levelMetas[0].topology, framework::topo_sequence);
        ASSERT_EQ(levelMetas[0].segments.size(), 2);
        ASSERT_EQ(levelMetas[0].segments[1], segmentId);
        ASSERT_EQ(levelMetas[1].topology, framework::topo_hash_mod);
        tabletFactoryCreator->_factorysMap.erase(indexName);
    }
}

TEST_F(MultiShardMemSegmentTest, TestEvaluateMemUsed)
{
    framework::SegmentMeta segmentMeta;
    auto tabletOptions = std::make_shared<config::TabletOptions>();
    auto schema = std::make_shared<framework::MockTabletSchema>();
    auto memSegment = std::make_shared<plain::PlainMemSegment>(tabletOptions.get(), schema, segmentMeta);
    memSegment->_segmentMemController = std::make_unique<MemoryQuotaController>("name", 1024);
    auto indexer = std::make_shared<index::MockMemIndexer>();
    auto fn = [](index::BuildingIndexMemoryUseUpdater* memUpdater) {
        memUpdater->UpdateCurrentMemUse(233);
        return;
    };
    EXPECT_CALL(*indexer, UpdateMemUse(_)).WillRepeatedly(Invoke(std::move(fn)));
    memSegment->TEST_AddIndexer("moc_kv", "kv", indexer);
    memSegment->UpdateMemUse();
    ASSERT_EQ(memSegment->EvaluateCurrentMemUsed(), 233);
    auto partitioner = std::make_shared<index::ShardPartitioner>();
    auto multiShardMemSegment =
        std::make_unique<plain::MultiShardMemSegment>(partitioner, 2, tabletOptions.get(), schema, segmentMeta);
    multiShardMemSegment->TEST_AddSegment(memSegment);
    multiShardMemSegment->TEST_AddSegment(memSegment);
    ASSERT_EQ(multiShardMemSegment->EvaluateCurrentMemUsed(), 466);
}

}} // namespace indexlibv2::plain
