#include "indexlib/table/plain/MultiShardDiskSegment.h"

#include "indexlib/base/MemoryQuotaController.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/framework/MetricsManager.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/framework/mock/MockTabletSchema.h"
#include "indexlib/index/IIndexFactory.h"
#include "indexlib/index/IIndexReader.h"
#include "indexlib/index/IndexFactoryCreator.h"
#include "indexlib/index/mock/MockDiskIndexer.h"
#include "indexlib/index/mock/MockIndexConfig.h"
#include "indexlib/index/mock/MockIndexFactory.h"
#include "indexlib/table/plain/MultiShardSegmentMetrics.h"
#include "unittest/unittest.h"

namespace indexlibv2 { namespace table {

class MultiShardDiskSegmentTest : public TESTBASE
{
public:
    MultiShardDiskSegmentTest() = default;
    ~MultiShardDiskSegmentTest() = default;

    void setUp() override;
    void tearDown() override;

private:
    void CreateDirectory(const std::shared_ptr<indexlib::file_system::Directory>& dir, const std::string& path)
    {
        dir->MakeDirectory(path);
    }

    std::shared_ptr<indexlib::file_system::Directory> _rootDir;
};

void MultiShardDiskSegmentTest::setUp()
{
    indexlib::file_system::FileSystemOptions fsOptions;
    std::string rootPath = GET_TEMP_DATA_PATH();
    auto fs = indexlib::file_system::FileSystemCreator::Create("online", rootPath, fsOptions).GetOrThrow();
    _rootDir = indexlib::file_system::Directory::Get(fs);
}

void MultiShardDiskSegmentTest::tearDown() {}

TEST_F(MultiShardDiskSegmentTest, TestOpen)
{
    std::vector<std::shared_ptr<config::IIndexConfig>> indexConfigs;
    auto metricsManager = std::make_shared<framework::MetricsManager>("", nullptr);
    framework::BuildResource buildResource;
    buildResource.metricsManager = metricsManager.get();

    auto indexConfig = std::make_shared<config::MockIndexConfig>();
    std::string indexType = "mock_kv";
    EXPECT_CALL(*indexConfig, GetIndexType).WillRepeatedly(testing::ReturnRef(indexType));
    const std::string indexName = "kv";
    EXPECT_CALL(*indexConfig, GetIndexName).WillRepeatedly(testing::ReturnRef(indexName));
    indexConfigs.push_back(indexConfig);

    auto schema = std::make_shared<framework::MockTabletSchema>();

    EXPECT_CALL(*schema, GetIndexConfigs()).WillRepeatedly(testing::Return(indexConfigs));
    auto indexer = std::make_shared<index::MockDiskIndexer>();
    EXPECT_CALL(*indexer, Open).WillOnce(Return(Status::InvalidArgs())).WillRepeatedly(Return(Status::OK()));
    framework::SegmentMeta segmentMeta;
    std::string segmentDirStr = "segment_0_level_0";
    CreateDirectory(_rootDir, segmentDirStr);
    segmentMeta.segmentDir = _rootDir->GetDirectory(segmentDirStr, /*throwExceptionIfNotExist=*/false);
    auto tabletFactoryCreator = index::IndexFactoryCreator::GetInstance();
    {
        auto mockFactory = std::make_unique<index::MockIndexFactory>();
        tabletFactoryCreator->_factorysMap[indexType] = std::make_pair(Status::OK(), mockFactory.get());
        EXPECT_CALL(*mockFactory, CreateDiskIndexer).WillRepeatedly(Return(indexer));
        EXPECT_CALL(*mockFactory, GetIndexPath).WillRepeatedly(Return("kv"));
        // index dir not exist
        auto diskSegment = std::make_unique<plain::MultiShardDiskSegment>(schema, segmentMeta, buildResource);
        auto status = diskSegment->Open(/*memoryQuotaController=*/nullptr, framework::DiskSegment::OpenMode::NORMAL);
        ASSERT_TRUE(status.IsInternalError());
        tabletFactoryCreator->_factorysMap.erase(indexType);
    }
    {
        auto mockFactory = std::make_unique<index::MockIndexFactory>();
        tabletFactoryCreator->_factorysMap[indexType] = std::make_pair(Status::OK(), mockFactory.get());
        EXPECT_CALL(*mockFactory, CreateDiskIndexer).WillRepeatedly(Return(indexer));
        EXPECT_CALL(*mockFactory, GetIndexPath).WillRepeatedly(Return("kv"));
        auto indexPath = index::IndexFactoryCreator::GetInstance()->Create(indexType).second->GetIndexPath();
        segmentMeta.segmentDir->MakeDirectory(indexPath);
        auto diskSegment = std::make_unique<plain::MultiShardDiskSegment>(schema, segmentMeta, buildResource);
        auto status = diskSegment->Open(/*memoryQuotaController=*/nullptr, framework::DiskSegment::OpenMode::NORMAL);
        ASSERT_TRUE(status.IsInvalidArgs());
        tabletFactoryCreator->_factorysMap.erase(indexType);
    }
    {
        auto mockFactory = std::make_unique<index::MockIndexFactory>();
        tabletFactoryCreator->_factorysMap[indexType] = std::make_pair(Status::OK(), mockFactory.get());
        EXPECT_CALL(*mockFactory, CreateDiskIndexer).WillRepeatedly(Return(indexer));
        EXPECT_CALL(*mockFactory, GetIndexPath).WillRepeatedly(Return("kv"));
        auto diskSegment = std::make_unique<plain::MultiShardDiskSegment>(schema, segmentMeta, buildResource);
        auto status = diskSegment->Open(/*memoryQuotaController=*/nullptr, framework::DiskSegment::OpenMode::NORMAL);
        ASSERT_TRUE(status.IsOK());
        tabletFactoryCreator->_factorysMap.erase(indexType);
    }
}

TEST_F(MultiShardDiskSegmentTest, TestCollectSegmentDescription)
{
    std::vector<std::shared_ptr<config::IIndexConfig>> indexConfigs;
    auto metricsManager = std::make_shared<framework::MetricsManager>("", nullptr);
    framework::BuildResource buildResource;
    buildResource.metricsManager = metricsManager.get();

    auto indexConfig = std::make_shared<config::MockIndexConfig>();
    std::string indexType = "mock_kv";
    EXPECT_CALL(*indexConfig, GetIndexType).WillRepeatedly(testing::ReturnRef(indexType));
    const std::string indexName = "kv";
    EXPECT_CALL(*indexConfig, GetIndexName).WillRepeatedly(testing::ReturnRef(indexName));
    indexConfigs.push_back(indexConfig);

    auto schema = std::make_shared<framework::MockTabletSchema>();
    EXPECT_CALL(*schema, GetIndexConfigs()).WillRepeatedly(testing::Return(indexConfigs));
    auto indexer = std::make_shared<index::MockDiskIndexer>();
    EXPECT_CALL(*indexer, Open).WillRepeatedly(Return(Status::OK()));
    auto tabletFactoryCreator = index::IndexFactoryCreator::GetInstance();

    auto segDescs = std::make_shared<framework::SegmentDescriptions>();
    uint32_t shardNum = 2;
    uint32_t levelNum = 2;

    {
        auto mockFactory = std::make_unique<index::MockIndexFactory>();
        tabletFactoryCreator->_factorysMap[indexType] = std::make_pair(Status::OK(), mockFactory.get());
        EXPECT_CALL(*mockFactory, CreateDiskIndexer).WillRepeatedly(Return(indexer));
        EXPECT_CALL(*mockFactory, GetIndexPath).WillRepeatedly(Return("kv"));
        auto indexPath = index::IndexFactoryCreator::GetInstance()->Create(indexType).second->GetIndexPath();
        framework::SegmentMeta segmentMeta;

        segmentid_t segmentId = 0;
        std::string segmentName = "segment_" + std::to_string(segmentId) + "_level_0";
        CreateDirectory(_rootDir, segmentName);
        auto segmentDir = _rootDir->GetDirectory(segmentName, /*throwExceptionIfNotExist=*/true);
        CreateDirectory(segmentDir, "shard_0");
        auto shard0Dir = segmentDir->GetDirectory("shard_0", /*throwExceptionIfNotExist=*/true);
        CreateDirectory(shard0Dir, indexPath);

        CreateDirectory(segmentDir, "shard_1");
        auto shard1Dir = segmentDir->GetDirectory("shard_1", /*throwExceptionIfNotExist=*/true);
        CreateDirectory(shard1Dir, indexPath);

        segmentMeta.segmentDir = _rootDir->GetDirectory(segmentName, /*throwExceptionIfNotExist=*/false);
        segmentMeta.segmentId = segmentId;
        segmentMeta.segmentInfo->shardCount = shardNum;
        auto diskSegment = std::make_unique<plain::MultiShardDiskSegment>(schema, segmentMeta, buildResource);
        auto status = diskSegment->Open(/*memoryQuotaController=*/nullptr, framework::DiskSegment::OpenMode::NORMAL);
        ASSERT_TRUE(status.IsOK());

        auto segmentMetrics = diskSegment->GetSegmentMetrics();
        auto multiShardSegMetrics =
            std::dynamic_pointer_cast<indexlibv2::plain::MultiShardSegmentMetrics>(segmentMetrics);
        ASSERT_NE(multiShardSegMetrics, nullptr);

        auto levelInfo = std::make_shared<indexlibv2::framework::LevelInfo>();
        levelInfo->Init(framework::topo_hash_mod, levelNum, shardNum);
        segDescs->SetLevelInfo(levelInfo);
        diskSegment->CollectSegmentDescription(segDescs);
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
        ASSERT_EQ(levelMetas[1].segments.size(), 2);
        ASSERT_EQ(levelMetas[1].topology, framework::topo_hash_mod);
        tabletFactoryCreator->_factorysMap.erase(indexName);
    }

    {
        auto mockFactory = std::make_unique<index::MockIndexFactory>();
        tabletFactoryCreator->_factorysMap[indexType] = std::make_pair(Status::OK(), mockFactory.get());
        EXPECT_CALL(*mockFactory, CreateDiskIndexer).WillRepeatedly(Return(indexer));
        EXPECT_CALL(*mockFactory, GetIndexPath).WillRepeatedly(Return("kv"));
        auto indexPath = index::IndexFactoryCreator::GetInstance()->Create(indexType).second->GetIndexPath();
        framework::SegmentMeta segmentMeta;
        segmentid_t segmentId = 1;
        uint32_t shardId = 1;
        std::string segmentName = "segment_" + std::to_string(segmentId) + "_level_1";
        CreateDirectory(_rootDir, segmentName);
        segmentMeta.segmentDir = _rootDir->GetDirectory(segmentName, /*throwExceptionIfNotExist=*/false);
        segmentMeta.segmentDir->MakeDirectory(indexPath);
        segmentMeta.segmentId = segmentId;
        segmentMeta.segmentInfo->shardCount = shardNum;
        segmentMeta.segmentInfo->shardId = shardId;
        auto diskSegment = std::make_unique<plain::MultiShardDiskSegment>(schema, segmentMeta, buildResource);
        auto status = diskSegment->Open(/*memoryQuotaController=*/nullptr, framework::DiskSegment::OpenMode::NORMAL);
        ASSERT_TRUE(status.IsOK());

        diskSegment->CollectSegmentDescription(segDescs);
        auto segmentName2 = segDescs->GetSegmentDir(segmentId);
        ASSERT_EQ(segmentName, segmentName2);
        const auto& levelMetas = segDescs->GetLevelInfo()->levelMetas;
        ASSERT_EQ(levelMetas.size(), 2);
        ASSERT_EQ(levelMetas[0].levelIdx, 0);
        ASSERT_EQ(levelMetas[0].topology, framework::topo_sequence);
        ASSERT_EQ(levelMetas[0].segments.size(), 1);
        ASSERT_EQ(levelMetas[1].levelIdx, 1);
        ASSERT_EQ(levelMetas[1].topology, framework::topo_hash_mod);
        ASSERT_EQ(levelMetas[1].segments.size(), 2);
        ASSERT_EQ(levelMetas[1].segments[shardId], segmentId);
        tabletFactoryCreator->_factorysMap.erase(indexName);
    }
}

TEST_F(MultiShardDiskSegmentTest, TestEvaluateMemUsed)
{
    auto indexer = std::make_shared<index::MockDiskIndexer>();
    EXPECT_CALL(*indexer, EvaluateCurrentMemUsed).WillRepeatedly((Return(233)));
    auto schema = std::make_shared<framework::MockTabletSchema>();
    framework::SegmentMeta segmentMeta;
    framework::BuildResource buildResource;
    auto diskSegment = std::make_shared<plain::PlainDiskSegment>(schema, segmentMeta, buildResource);
    diskSegment->AddIndexer("moc_kv", "kv", indexer);
    diskSegment->AddIndexer("moc_kv", "kv2", indexer);
    auto multiShardDiskSegment = std::make_unique<plain::MultiShardDiskSegment>(schema, segmentMeta, buildResource);
    multiShardDiskSegment->TEST_AddSegment(diskSegment);
    multiShardDiskSegment->TEST_AddSegment(diskSegment);
    ASSERT_EQ(multiShardDiskSegment->EvaluateCurrentMemUsed(), 932);
}

}} // namespace indexlibv2::table
