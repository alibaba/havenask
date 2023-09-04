#include "indexlib/table/plain/PlainMemSegment.h"

#include "autil/Scope.h"
#include "indexlib/base/MemoryQuotaController.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/document/DocumentBatch.h"
#include "indexlib/document/extractor/IDocumentInfoExtractorFactory.h"
#include "indexlib/framework/MetricsManager.h"
#include "indexlib/framework/mock/MockTabletSchema.h"
#include "indexlib/index/BuildingIndexMemoryUseUpdater.h"
#include "indexlib/index/IIndexFactory.h"
#include "indexlib/index/IIndexReader.h"
#include "indexlib/index/mock/MockIndexConfig.h"
#include "indexlib/index/mock/MockIndexFactory.h"
#include "indexlib/index/mock/MockMemIndexer.h"
#include "unittest/unittest.h"
using namespace std;

namespace indexlibv2::plain {

class PlainMemSegmentTest : public TESTBASE
{
public:
    PlainMemSegmentTest() = default;
    ~PlainMemSegmentTest() = default;

    void setUp() override {}
    void tearDown() override {}
};

TEST_F(PlainMemSegmentTest, TestOpen)
{
    auto tabletOptions = std::make_shared<config::TabletOptions>();
    tabletOptions->SetIsOnline(true);

    std::vector<std::shared_ptr<config::IIndexConfig>> indexConfigs;
    auto indexConfig = std::make_shared<config::MockIndexConfig>();
    std::string mockOrc = "mock_orc";
    EXPECT_CALL(*indexConfig, GetIndexType).WillRepeatedly(testing::ReturnRef(mockOrc));
    const std::string indexName = "orc";
    EXPECT_CALL(*indexConfig, GetIndexName).WillRepeatedly(testing::ReturnRef(indexName));
    indexConfigs.push_back(indexConfig);

    auto schema = std::make_shared<framework::MockTabletSchema>();
    auto indexer = std::make_shared<index::MockMemIndexer>();
    EXPECT_CALL(*indexer, UpdateMemUse).WillRepeatedly(Return());
    EXPECT_CALL(*indexer, Init).Times(2).WillOnce(Return(Status::InternalError())).WillOnce(Return(Status::OK()));
    framework::BuildResource buildResource;
    buildResource.memController = std::make_shared<MemoryQuotaController>("name", 1024);
    auto tabletFactoryCreator = index::IndexFactoryCreator::GetInstance();

    {
        EXPECT_CALL(*schema, GetIndexConfigs()).WillRepeatedly(testing::Return(indexConfigs));
        auto mockFactory = std::make_unique<index::MockIndexFactory>();
        tabletFactoryCreator->_factorysMap[mockOrc] = std::make_pair(Status::OK(), mockFactory.get());
        EXPECT_CALL(*mockFactory, CreateMemIndexer).WillRepeatedly(Return(indexer));
        auto memSegment =
            std::make_unique<plain::PlainMemSegment>(tabletOptions.get(), schema, framework::SegmentMeta());
        auto status = memSegment->Open(buildResource, /*lastSegMetrics=*/nullptr);
        EXPECT_TRUE(status.IsInternalError());
        tabletFactoryCreator->_factorysMap.erase(mockOrc);
    }
    {
        EXPECT_CALL(*schema, GetIndexConfigs()).WillRepeatedly(testing::Return(indexConfigs));
        auto mockFactory = std::make_unique<index::MockIndexFactory>();
        tabletFactoryCreator->_factorysMap[mockOrc] = std::make_pair(Status::OK(), mockFactory.get());
        EXPECT_CALL(*mockFactory, CreateMemIndexer).WillRepeatedly(Return(indexer));
        auto memSegment =
            std::make_unique<plain::PlainMemSegment>(tabletOptions.get(), schema, framework::SegmentMeta());
        auto status = memSegment->Open(buildResource, /*lastSegMetrics=*/nullptr);
        EXPECT_TRUE(status.IsOK());
        tabletFactoryCreator->_factorysMap.erase(mockOrc);
    }
}

TEST_F(PlainMemSegmentTest, TestPrepareIndexers)
{
    auto metricsManager = std::make_shared<framework::MetricsManager>("", nullptr);
    framework::BuildResource buildResource;
    buildResource.metricsManager = metricsManager.get();
    buildResource.memController = std::make_shared<MemoryQuotaController>("name", 1024);
    auto tabletOptions = std::make_shared<config::TabletOptions>();
    tabletOptions->SetIsOnline(true);

    auto indexConfig = std::make_shared<config::MockIndexConfig>();
    const std::string indexName = "mock_orc";
    EXPECT_CALL(*indexConfig, GetIndexType).WillRepeatedly(testing::ReturnRef(indexName));
    EXPECT_CALL(*indexConfig, GetIndexName).WillRepeatedly(testing::ReturnRef(indexName));

    auto indexConfig2 = std::make_shared<config::MockIndexConfig>();
    const std::string indexName2 = "mock_pk";
    EXPECT_CALL(*indexConfig2, GetIndexType).WillRepeatedly(testing::ReturnRef(indexName2));
    EXPECT_CALL(*indexConfig2, GetIndexName).WillRepeatedly(testing::ReturnRef(indexName2));

    std::vector<std::shared_ptr<config::IIndexConfig>> indexConfigs;
    indexConfigs.push_back(indexConfig);
    indexConfigs.push_back(indexConfig2);

    auto schema = std::make_shared<framework::MockTabletSchema>();
    auto indexer = std::make_shared<index::MockMemIndexer>();
    EXPECT_CALL(*indexer, UpdateMemUse).WillRepeatedly(Return());
    EXPECT_CALL(*indexer, Init).WillOnce(Return(Status::InternalError())).WillRepeatedly(Return(Status::OK()));

    auto tabletFactoryCreator = index::IndexFactoryCreator::GetInstance();
    {
        auto mockFactory1 = std::make_unique<index::MockIndexFactory>();
        auto mockFactory2 = std::make_unique<index::MockIndexFactory>();
        tabletFactoryCreator->_factorysMap[indexName] = std::make_pair(Status::OK(), mockFactory1.get());
        tabletFactoryCreator->_factorysMap[indexName2] = std::make_pair(Status::OK(), mockFactory2.get());
        EXPECT_CALL(*mockFactory1, CreateMemIndexer).WillRepeatedly(Return(indexer));
        EXPECT_CALL(*mockFactory2, CreateMemIndexer).WillRepeatedly(Return(indexer));
        std::vector<std::shared_ptr<config::IIndexConfig>> emptyIndexConfigs;
        EXPECT_CALL(*schema, GetIndexConfigs()).WillRepeatedly(testing::Return(emptyIndexConfigs));
        auto memSegment =
            std::make_unique<plain::PlainMemSegment>(tabletOptions.get(), schema, framework::SegmentMeta());
        auto status = memSegment->Open(buildResource, nullptr);
        EXPECT_TRUE(status.IsInvalidArgs());
        tabletFactoryCreator->_factorysMap.erase(indexName);
        tabletFactoryCreator->_factorysMap.erase(indexName2);
    }
    {
        EXPECT_CALL(*schema, GetIndexConfigs()).WillRepeatedly(testing::Return(indexConfigs));
        auto mockFactory1 = std::make_unique<index::MockIndexFactory>();
        auto mockFactory2 = std::make_unique<index::MockIndexFactory>();
        tabletFactoryCreator->_factorysMap[indexName] = std::make_pair(Status::OK(), mockFactory1.get());
        tabletFactoryCreator->_factorysMap[indexName2] = std::make_pair(Status::OK(), mockFactory2.get());
        EXPECT_CALL(*mockFactory1, CreateMemIndexer).WillRepeatedly(Return(indexer));
        EXPECT_CALL(*mockFactory2, CreateMemIndexer).WillRepeatedly(Return(indexer));
        auto memSegment =
            std::make_unique<plain::PlainMemSegment>(tabletOptions.get(), schema, framework::SegmentMeta());
        auto status = memSegment->Open(buildResource, nullptr);
        EXPECT_TRUE(status.IsInternalError());
        tabletFactoryCreator->_factorysMap.erase(indexName);
        tabletFactoryCreator->_factorysMap.erase(indexName2);
    }
    {
        EXPECT_CALL(*schema, GetIndexConfigs()).WillRepeatedly(testing::Return(indexConfigs));
        auto mockFactory1 = std::make_unique<index::MockIndexFactory>();
        auto mockFactory2 = std::make_unique<index::MockIndexFactory>();
        tabletFactoryCreator->_factorysMap[indexName] = std::make_pair(Status::OK(), mockFactory1.get());
        tabletFactoryCreator->_factorysMap[indexName2] = std::make_pair(Status::OK(), mockFactory2.get());
        EXPECT_CALL(*mockFactory1, CreateMemIndexer).WillRepeatedly(Return(indexer));
        EXPECT_CALL(*mockFactory2, CreateMemIndexer).WillRepeatedly(Return(indexer));
        auto memSegment =
            std::make_unique<plain::PlainMemSegment>(tabletOptions.get(), schema, framework::SegmentMeta());
        auto status = memSegment->Open(buildResource, nullptr);
        EXPECT_TRUE(status.IsOK());
        tabletFactoryCreator->_factorysMap.erase(indexName);
        tabletFactoryCreator->_factorysMap.erase(indexName2);
    }
}

TEST_F(PlainMemSegmentTest, TestBuild)
{
    auto tabletOptions = std::make_shared<config::TabletOptions>();
    tabletOptions->SetIsOnline(true);

    std::vector<std::shared_ptr<config::IIndexConfig>> indexConfigs;
    auto indexConfig = std::make_shared<config::MockIndexConfig>();
    const std::string indexName = "mock_orc";
    EXPECT_CALL(*indexConfig, GetIndexType).WillRepeatedly(testing::ReturnRef(indexName));
    EXPECT_CALL(*indexConfig, GetIndexName).WillRepeatedly(testing::ReturnRef(indexName));
    indexConfigs.push_back(indexConfig);

    auto schema = std::make_shared<framework::MockTabletSchema>();
    EXPECT_CALL(*schema, GetIndexConfigs()).WillRepeatedly(testing::Return(indexConfigs));
    auto indexer = std::make_shared<index::MockMemIndexer>();
    EXPECT_CALL(*indexer, UpdateMemUse).WillRepeatedly(Return());
    EXPECT_CALL(*indexer, Init).Times(1).WillRepeatedly(Return(Status::OK()));
    EXPECT_CALL(*indexer, Build(testing::_)).Times(1);

    framework::BuildResource buildResource;
    buildResource.memController = std::make_shared<MemoryQuotaController>("name", 1024);
    auto tabletFactoryCreator = index::IndexFactoryCreator::GetInstance();
    {
        auto mockFactory = std::make_unique<index::MockIndexFactory>();
        tabletFactoryCreator->_factorysMap[indexName] = std::make_pair(Status::OK(), mockFactory.get());
        EXPECT_CALL(*mockFactory, CreateMemIndexer).Times(1).WillOnce(Return(indexer));
        auto memSegment =
            std::make_unique<plain::PlainMemSegment>(tabletOptions.get(), schema, framework::SegmentMeta());
        auto status = memSegment->Open(buildResource, /*lastSegMetrics=*/nullptr);
        EXPECT_TRUE(status.IsOK());
        auto docBatchPtr = new document::DocumentBatch();
        std::shared_ptr<document::IDocumentBatch> docBatch(docBatchPtr);

        status = memSegment->Build(docBatch.get());
        EXPECT_TRUE(status.IsOK());
        tabletFactoryCreator->_factorysMap.erase(indexName);
    }
}

TEST_F(PlainMemSegmentTest, TestFillStatisticsWhenSeal)
{
    auto tabletOptions = std::make_shared<config::TabletOptions>();
    tabletOptions->SetIsOnline(true);

    std::vector<std::shared_ptr<config::IIndexConfig>> indexConfigs;
    auto indexConfig = std::make_shared<config::MockIndexConfig>();
    const std::string indexName = "mock_orc";
    EXPECT_CALL(*indexConfig, GetIndexType).WillRepeatedly(testing::ReturnRef(indexName));
    EXPECT_CALL(*indexConfig, GetIndexName).WillRepeatedly(testing::ReturnRef(indexName));
    indexConfigs.push_back(indexConfig);

    auto schema = std::make_shared<framework::MockTabletSchema>();
    EXPECT_CALL(*schema, GetIndexConfigs()).WillRepeatedly(testing::Return(indexConfigs));
    auto indexer = std::make_shared<index::MockMemIndexer>();
    EXPECT_CALL(*indexer, UpdateMemUse).WillRepeatedly(Return());
    EXPECT_CALL(*indexer, Init).Times(1).WillRepeatedly(Return(Status::OK()));
    EXPECT_CALL(*indexer, FillStatistics).Times(1);
    EXPECT_CALL(*indexer, Seal).Times(1);

    framework::BuildResource buildResource;
    buildResource.memController = std::make_shared<MemoryQuotaController>("name", 1024);

    auto tabletFactoryCreator = index::IndexFactoryCreator::GetInstance();
    auto mockFactory = std::make_unique<index::MockIndexFactory>();
    tabletFactoryCreator->_factorysMap[indexName] = std::make_pair(Status::OK(), mockFactory.get());

    autil::ScopeGuard _([&tabletFactoryCreator, &indexName]() { tabletFactoryCreator->_factorysMap.erase(indexName); });

    EXPECT_CALL(*mockFactory, CreateMemIndexer).Times(1).WillOnce(Return(indexer));
    auto memSegment = std::make_unique<plain::PlainMemSegment>(tabletOptions.get(), schema, framework::SegmentMeta());
    EXPECT_TRUE(memSegment->Open(buildResource, /*lastSegMetrics=*/nullptr).IsOK());
    memSegment->Seal();
}

TEST_F(PlainMemSegmentTest, TestEvaluateMemUsed)
{
    framework::SegmentMeta segmentMeta;
    auto tabletOptions = std::make_shared<config::TabletOptions>();
    auto schema = std::make_shared<framework::MockTabletSchema>();
    auto memSegment = std::make_shared<plain::PlainMemSegment>(tabletOptions.get(), schema, segmentMeta);
    memSegment->_segmentMemController = std::make_unique<MemoryQuotaController>("name", 1024);
    auto indexer1 = std::make_shared<index::MockMemIndexer>();
    auto fn1 = [](index::BuildingIndexMemoryUseUpdater* memUpdater) {
        memUpdater->UpdateCurrentMemUse(233);
        return;
    };
    EXPECT_CALL(*indexer1, UpdateMemUse(_)).WillRepeatedly(Invoke(std::move(fn1)));
    auto indexer2 = std::make_shared<index::MockMemIndexer>();
    auto fn2 = [](index::BuildingIndexMemoryUseUpdater* memUpdater) {
        memUpdater->UpdateCurrentMemUse(322);
        return;
    };
    EXPECT_CALL(*indexer2, UpdateMemUse(_)).WillRepeatedly(Invoke(std::move(fn2)));
    memSegment->TEST_AddIndexer("moc_kv", "kv", indexer1);
    memSegment->TEST_AddIndexer("moc_kv", "kv2", indexer2);
    memSegment->UpdateMemUse();
    ASSERT_EQ(memSegment->EvaluateCurrentMemUsed(), 555);
}

class MockMemSegment : public PlainMemSegment
{
public:
    MockMemSegment(const config::TabletOptions* options, const std::shared_ptr<config::ITabletSchema>& schema,
                   const framework::SegmentMeta& segmentMeta)
        : PlainMemSegment(options, schema, segmentMeta)
    {
    }
    ~MockMemSegment() = default;

    MOCK_METHOD((std::pair<Status, size_t>), GetLastSegmentMemUse,
                (const std::shared_ptr<config::IIndexConfig>&, const indexlib::framework::SegmentMetrics*),
                (const, override));
};

TEST_F(PlainMemSegmentTest, TestGenerateIndexerResource)
{
    auto tabletOptions = std::make_shared<config::TabletOptions>();
    tabletOptions->SetIsOnline(true);
    std::vector<std::shared_ptr<config::IIndexConfig>> indexConfigs;
    auto indexConfig = std::make_shared<config::MockIndexConfig>();
    string indexType = "mock_kv";
    EXPECT_CALL(*indexConfig, GetIndexType).WillRepeatedly(testing::ReturnRef(indexType));
    EXPECT_CALL(*indexConfig, GetIndexName).WillRepeatedly(testing::ReturnRef("kv1"));
    indexConfigs.push_back(indexConfig);
    indexConfig = std::make_shared<config::MockIndexConfig>();
    EXPECT_CALL(*indexConfig, GetIndexType).WillRepeatedly(testing::ReturnRef(indexType));
    EXPECT_CALL(*indexConfig, GetIndexName).WillRepeatedly(testing::ReturnRef("kv2"));
    indexConfigs.push_back(indexConfig);

    auto schema = std::make_shared<framework::MockTabletSchema>();
    auto tabletFactoryCreator = index::IndexFactoryCreator::GetInstance();
    framework::BuildResource buildResource;
    buildResource.buildingMemLimit = 5000;
    EXPECT_CALL(*schema, GetIndexConfigs()).WillRepeatedly(testing::Return(indexConfigs));
    auto mockFactory = std::make_unique<index::MockIndexFactory>();
    tabletFactoryCreator->_factorysMap[indexType] = std::make_pair(Status::OK(), mockFactory.get());
    auto memSegment = std::make_unique<MockMemSegment>(tabletOptions.get(), schema, framework::SegmentMeta());
    {
        EXPECT_CALL(*memSegment, GetLastSegmentMemUse)
            .Times(2)
            .WillOnce(Return(std::make_pair(Status::OK(), 1)))
            .WillOnce(Return(std::make_pair(Status::OK(), 5)));
        vector<plain::PlainMemSegment::IndexerResource> indexerResources;
        ASSERT_TRUE(memSegment->GenerateIndexerResource(buildResource, indexerResources).IsOK());
        ASSERT_EQ(2, indexerResources.size());
        ASSERT_EQ(833, indexerResources[0].indexerParam.maxMemoryUseInBytes);
        ASSERT_EQ(4166, indexerResources[1].indexerParam.maxMemoryUseInBytes);
    }
    {
        EXPECT_CALL(*memSegment, GetLastSegmentMemUse)
            .Times(2)
            .WillOnce(Return(std::make_pair(Status::NotFound(), 1)))
            .WillOnce(Return(std::make_pair(Status::OK(), 5)));
        vector<plain::PlainMemSegment::IndexerResource> indexerResources;
        ASSERT_TRUE(memSegment->GenerateIndexerResource(buildResource, indexerResources).IsOK());
        ASSERT_EQ(2, indexerResources.size());
        ASSERT_EQ(2500, indexerResources[0].indexerParam.maxMemoryUseInBytes);
        ASSERT_EQ(2500, indexerResources[1].indexerParam.maxMemoryUseInBytes);
    }
    tabletFactoryCreator->_factorysMap.erase(indexType);
}

} // namespace indexlibv2::plain
