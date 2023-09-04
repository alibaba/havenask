#include "indexlib/table/kkv_table/KKVTabletFactory.h"

#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/DiskSegment.h"
#include "indexlib/framework/MemSegment.h"
#include "indexlib/framework/MetricsManager.h"
#include "indexlib/table/common/LSMTabletLoader.h"
#include "indexlib/table/kkv_table/test/KKVTableTestHelper.h"
#include "indexlib/table/kkv_table/test/KKVTabletSchemaMaker.h"
#include "indexlib/table/plain/MultiShardDiskSegment.h"
#include "indexlib/table/plain/MultiShardMemSegment.h"
#include "unittest/unittest.h"

using namespace indexlibv2::framework;
using namespace indexlibv2::plain;

namespace indexlibv2 { namespace table {

class KKVTabletFactoryTest : public TESTBASE
{
public:
    void setUp() override
    {
        _schema = KKVTableTestHelper::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "kkv_schema.json");
        _tabletOptions = std::make_shared<config::TabletOptions>();
    }

private:
    std::shared_ptr<config::ITabletSchema> _schema;
    std::shared_ptr<config::TabletOptions> _tabletOptions;
};

TEST_F(KKVTabletFactoryTest, TestCreateTabletLoader)
{
    KKVTabletFactory factory;
    factory.Init(_tabletOptions, nullptr);
    std::unique_ptr<ITabletLoader> tabletLoader = factory.CreateTabletLoader(std::string("test"));
    LSMTabletLoader* lsmTabletLoader = dynamic_cast<LSMTabletLoader*>(tabletLoader.get());
    ASSERT_TRUE(lsmTabletLoader);
}

TEST_F(KKVTabletFactoryTest, TestCreateDiskSegment)
{
    KKVTabletFactory factory;
    factory.Init(_tabletOptions, nullptr);
    BuildResource buildResource;
    std::shared_ptr<framework::MetricsManager> metricsManager =
        std::make_shared<framework::MetricsManager>("", nullptr);
    buildResource.metricsManager = metricsManager.get();

    std::unique_ptr<DiskSegment> builtSeg = factory.CreateDiskSegment(SegmentMeta(), buildResource);
    MultiShardDiskSegment* multiShardBuiltSeg = dynamic_cast<MultiShardDiskSegment*>(builtSeg.get());
    ASSERT_TRUE(multiShardBuiltSeg);
}

TEST_F(KKVTabletFactoryTest, TestCreateMemSegment)
{
    {
        auto options = std::make_shared<config::TabletOptions>();
        KKVTabletFactory factory;
        factory.Init(options, nullptr);
        std::unique_ptr<MemSegment> buildingSeg = factory.CreateMemSegment(SegmentMeta());
        MultiShardMemSegment* multiShardBuildingSeg = dynamic_cast<MultiShardMemSegment*>(buildingSeg.get());
        ASSERT_NE(nullptr, multiShardBuildingSeg);
    }
    {
        std::string jsonStr = R"({
            "online_index_config": {
                "build_config": {
                    "sharding_column_num" : 2,
                    "level_num" : 3
                }
            }
        })";
        auto tabletOptions = KKVTableTestHelper::CreateTableOptions(jsonStr);
        KKVTabletFactory factory;
        factory.Init(tabletOptions, nullptr);
        SegmentMeta segmentMeta;
        segmentMeta.schema = _schema;
        segmentMeta.segmentInfo = std::make_shared<framework::SegmentInfo>();
        segmentMeta.segmentInfo->SetShardCount(framework::SegmentInfo::INVALID_SHARDING_COUNT);
        std::unique_ptr<MemSegment> buildingSeg = factory.CreateMemSegment(segmentMeta);
        ASSERT_NE(nullptr, buildingSeg);
        MultiShardMemSegment* multiShardBuildingSeg = dynamic_cast<MultiShardMemSegment*>(buildingSeg.get());
        ASSERT_NE(nullptr, multiShardBuildingSeg);
        ASSERT_EQ(2U, multiShardBuildingSeg->GetShardCount());
    }
    {
        std::string jsonStr = R"({
            "online_index_config": {
                "build_config": {
                    "sharding_column_num" : 3,
                    "level_num" : 3
                }
            }
        })";
        auto tabletOptions = KKVTableTestHelper::CreateTableOptions(jsonStr);
        KKVTabletFactory factory;
        factory.Init(tabletOptions, nullptr);

        SegmentMeta segmentMeta;
        segmentMeta.schema = _schema;
        segmentMeta.segmentInfo = std::make_shared<framework::SegmentInfo>();
        segmentMeta.segmentInfo->SetShardCount(framework::SegmentInfo::INVALID_SHARDING_COUNT);
        std::unique_ptr<MemSegment> buildingSeg = factory.CreateMemSegment(segmentMeta);
        ASSERT_EQ(nullptr, buildingSeg);
    }
}

}} // namespace indexlibv2::table
