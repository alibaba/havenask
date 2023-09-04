#include "indexlib/index/attribute/AttributeIndexFactory.h"

#include "indexlib/framework/MetricsManager.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/IIndexReader.h"
#include "indexlib/index/IndexerParameter.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/attribute/merger/UniqEncodedMultiValueAttributeMerger.h"
#include "indexlib/index/attribute/test/AttributeTestUtil.h"
#include "unittest/unittest.h"

namespace indexlibv2::index {
class AttributeIndexFactoryTest : public TESTBASE
{
public:
    AttributeIndexFactoryTest() = default;
    ~AttributeIndexFactoryTest() = default;

    void setUp() override;
    void tearDown() override {}
};

void AttributeIndexFactoryTest::setUp() {}

TEST_F(AttributeIndexFactoryTest, TestSimpleProcess)
{
    std::shared_ptr<config::IIndexConfig> config = AttributeTestUtil::CreateAttrConfig<uint32_t>(true, "equal");
    auto indexFactoryCreator = IndexFactoryCreator::GetInstance();
    auto [status, indexFactory] = indexFactoryCreator->Create(config->GetIndexType());
    ASSERT_TRUE(status.IsOK());
    ASSERT_EQ(typeid(AttributeIndexFactory), typeid(*indexFactory));
    IndexerParameter indexParam;
    std::shared_ptr<kmonitor::MetricsReporter> metricsReporter;
    std::shared_ptr<framework::MetricsManager> metricsManager(new framework::MetricsManager("", metricsReporter));
    indexParam.metricsManager = metricsManager.get();
    ASSERT_TRUE(indexFactory->CreateDiskIndexer(config, indexParam));
    ASSERT_TRUE(indexFactory->CreateMemIndexer(config, indexParam));
    ASSERT_TRUE(indexFactory->CreateIndexReader(config, indexParam));
    ASSERT_TRUE(indexFactory->CreateIndexMerger(config));
    ASSERT_TRUE(indexFactory->CreateIndexConfig({}));
    ASSERT_EQ(std::string("attribute"), indexFactory->GetIndexPath());
}

TEST_F(AttributeIndexFactoryTest, TestCreateMergerWithUniqEncoded)
{
    std::shared_ptr<config::IIndexConfig> config = AttributeTestUtil::CreateAttrConfig<autil::MultiChar>(false, "uniq");
    auto indexFactoryCreator = IndexFactoryCreator::GetInstance();
    auto [status, indexFactory] = indexFactoryCreator->Create(config->GetIndexType());
    ASSERT_TRUE(status.IsOK());
    ASSERT_EQ(typeid(AttributeIndexFactory), typeid(*indexFactory));
    IndexerParameter indexParam;
    std::shared_ptr<kmonitor::MetricsReporter> metricsReporter;
    std::shared_ptr<framework::MetricsManager> metricsManager(new framework::MetricsManager("", metricsReporter));
    indexParam.metricsManager = metricsManager.get();
    ASSERT_TRUE(indexFactory->CreateDiskIndexer(config, indexParam));
    ASSERT_TRUE(indexFactory->CreateMemIndexer(config, indexParam));
    ASSERT_TRUE(indexFactory->CreateIndexReader(config, indexParam));

    auto indexMerger = indexFactory->CreateIndexMerger(config);
    std::shared_ptr<IIndexMerger> res = std::move(indexMerger);

    ASSERT_TRUE(std::dynamic_pointer_cast<UniqEncodedMultiValueAttributeMerger<char>>(res));

    ASSERT_TRUE(indexFactory->CreateIndexConfig({}));
    ASSERT_EQ(std::string("attribute"), indexFactory->GetIndexPath());
}

} // namespace indexlibv2::index
