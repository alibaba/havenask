#include "indexlib/table/plain/PlainDiskSegment.h"

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
#include "unittest/unittest.h"

namespace indexlibv2 { namespace table {

class PlainDiskSegmentTest : public TESTBASE
{
public:
    PlainDiskSegmentTest() = default;
    ~PlainDiskSegmentTest() = default;

    void setUp() override;
    void tearDown() override;

private:
    void CreateDirectory(const std::string& dir) { _rootDir->MakeDirectory(dir); }

    std::shared_ptr<indexlib::file_system::Directory> _rootDir;
};

void PlainDiskSegmentTest::setUp()
{
    indexlib::file_system::FileSystemOptions fsOptions;
    std::string rootPath = GET_TEMP_DATA_PATH();
    auto fs = indexlib::file_system::FileSystemCreator::Create("online", rootPath, fsOptions).GetOrThrow();
    _rootDir = indexlib::file_system::Directory::Get(fs);
}

void PlainDiskSegmentTest::tearDown() {}

TEST_F(PlainDiskSegmentTest, TestOpen)
{
    std::vector<std::shared_ptr<config::IIndexConfig>> indexConfigs;
    auto metricsManager = std::make_shared<framework::MetricsManager>("", nullptr);
    framework::BuildResource buildResource;
    buildResource.metricsManager = metricsManager.get();

    auto indexConfig = std::make_shared<config::MockIndexConfig>();
    std::string indexType = "mock_orc";
    EXPECT_CALL(*indexConfig, GetIndexType).WillRepeatedly(testing::ReturnRef(indexType));
    const std::string indexName = "orc";
    EXPECT_CALL(*indexConfig, GetIndexName).WillRepeatedly(testing::ReturnRef(indexName));
    indexConfigs.push_back(indexConfig);

    auto schema = std::make_shared<framework::MockTabletSchema>();

    EXPECT_CALL(*schema, GetIndexConfigs()).WillRepeatedly(testing::Return(indexConfigs));
    auto indexer = std::make_shared<index::MockDiskIndexer>();
    EXPECT_CALL(*indexer, Open).Times(2).WillOnce(Return(Status::InvalidArgs())).WillOnce(Return(Status::OK()));
    framework::SegmentMeta segmentMeta;
    std::string segmentDirStr = "segment_0_level_0";
    CreateDirectory(segmentDirStr);
    segmentMeta.segmentDir = _rootDir->GetDirectory(segmentDirStr, /*throwExceptionIfNotExist=*/false);
    auto tabletFactoryCreator = index::IndexFactoryCreator::GetInstance();
    {
        auto mockFactory = std::make_unique<index::MockIndexFactory>();
        tabletFactoryCreator->_factorysMap[indexType] = std::make_pair(Status::OK(), mockFactory.get());
        EXPECT_CALL(*mockFactory, CreateDiskIndexer).WillRepeatedly(Return(indexer));
        EXPECT_CALL(*mockFactory, GetIndexPath).WillRepeatedly(Return("orc"));
        // index dir not exist
        auto diskSegment = std::make_unique<plain::PlainDiskSegment>(schema, segmentMeta, buildResource);
        auto status = diskSegment->Open(/*memoryQuotaController=*/nullptr, framework::DiskSegment::OpenMode::NORMAL);
        EXPECT_TRUE(status.IsInternalError());
        tabletFactoryCreator->_factorysMap.erase(indexType);
    }
    {
        auto mockFactory = std::make_unique<index::MockIndexFactory>();
        tabletFactoryCreator->_factorysMap[indexType] = std::make_pair(Status::OK(), mockFactory.get());
        EXPECT_CALL(*mockFactory, CreateDiskIndexer).WillRepeatedly(Return(indexer));
        EXPECT_CALL(*mockFactory, GetIndexPath).WillRepeatedly(Return("orc"));
        auto indexPath = index::IndexFactoryCreator::GetInstance()->Create(indexType).second->GetIndexPath();
        segmentMeta.segmentDir->MakeDirectory(indexPath);
        auto diskSegment = std::make_unique<plain::PlainDiskSegment>(schema, segmentMeta, buildResource);
        auto status = diskSegment->Open(/*memoryQuotaController=*/nullptr, framework::DiskSegment::OpenMode::NORMAL);
        EXPECT_TRUE(status.IsInvalidArgs());
        tabletFactoryCreator->_factorysMap.erase(indexType);
    }
    {
        auto mockFactory = std::make_unique<index::MockIndexFactory>();
        tabletFactoryCreator->_factorysMap[indexType] = std::make_pair(Status::OK(), mockFactory.get());
        EXPECT_CALL(*mockFactory, CreateDiskIndexer).WillRepeatedly(Return(indexer));
        EXPECT_CALL(*mockFactory, GetIndexPath).WillRepeatedly(Return("orc"));
        auto diskSegment = std::make_unique<plain::PlainDiskSegment>(schema, segmentMeta, buildResource);
        auto status = diskSegment->Open(/*memoryQuotaController=*/nullptr, framework::DiskSegment::OpenMode::NORMAL);
        EXPECT_TRUE(status.IsOK());
        tabletFactoryCreator->_factorysMap.erase(indexType);
    }
}

TEST_F(PlainDiskSegmentTest, TestEvaluateMemUsed)
{
    auto indexer = std::make_shared<index::MockDiskIndexer>();
    EXPECT_CALL(*indexer, EvaluateCurrentMemUsed).WillRepeatedly((Return(233)));
    framework::BuildResource buildResource;
    framework::SegmentMeta segmentMeta;
    auto schema = std::make_shared<framework::MockTabletSchema>();
    auto diskSegment = std::make_unique<plain::PlainDiskSegment>(schema, segmentMeta, buildResource);
    diskSegment->AddIndexer("moc_kv", "kv", indexer);
    diskSegment->AddIndexer("moc_kv", "kv2", indexer);
    ASSERT_EQ(diskSegment->EvaluateCurrentMemUsed(), 466);
}

}} // namespace indexlibv2::table
