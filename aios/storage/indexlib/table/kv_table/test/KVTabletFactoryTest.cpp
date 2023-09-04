#include "indexlib/table/kv_table/KVTabletFactory.h"

#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/DiskSegment.h"
#include "indexlib/framework/MemSegment.h"
#include "indexlib/framework/MetricsManager.h"
#include "indexlib/table/common/LSMTabletLoader.h"
#include "indexlib/table/kv_table/KVTabletReader.h"
#include "indexlib/table/kv_table/KVTabletSessionReader.h"
#include "indexlib/table/kv_table/KVTabletWriter.h"
#include "indexlib/table/kv_table/test/KVTabletSchemaMaker.h"
#include "indexlib/table/plain/MultiShardDiskSegment.h"
#include "indexlib/table/plain/MultiShardMemSegment.h"
#include "unittest/unittest.h"

using namespace indexlibv2::framework;
using namespace indexlibv2::plain;

namespace indexlibv2 { namespace table {

class KVTabletFactoryTest : public TESTBASE
{
public:
    void setUp() override
    {
        std::string field = "string1:string;string2:string";
        _schema = table::KVTabletSchemaMaker::Make(field, "string1", "string2");
        _options = std::make_shared<config::TabletOptions>();
    }

private:
    std::shared_ptr<config::ITabletSchema> _schema;
    std::shared_ptr<config::TabletOptions> _options;
};

TEST_F(KVTabletFactoryTest, TestCreateTabletWriter)
{
    KVTabletFactory factory;
    factory.Init(_options, nullptr);
    std::unique_ptr<TabletWriter> tabletWriter = factory.CreateTabletWriter(_schema);
    KVTabletWriter* kvTabletWriter = dynamic_cast<KVTabletWriter*>(tabletWriter.get());
    ASSERT_TRUE(kvTabletWriter);
}

TEST_F(KVTabletFactoryTest, TestCreateTabletReader)
{
    KVTabletFactory factory;
    factory.Init(_options, nullptr);
    std::unique_ptr<TabletReader> tabletReader = factory.CreateTabletReader(_schema);
    KVTabletReader* kvTabletReader = dynamic_cast<KVTabletReader*>(tabletReader.get());
    ASSERT_TRUE(kvTabletReader);
}

TEST_F(KVTabletFactoryTest, TestCreateTabletLoader)
{
    KVTabletFactory factory;
    factory.Init(_options, nullptr);
    std::unique_ptr<ITabletLoader> tabletLoader = factory.CreateTabletLoader(std::string("test"));
    LSMTabletLoader* lsmTabletLoader = dynamic_cast<LSMTabletLoader*>(tabletLoader.get());
    ASSERT_TRUE(lsmTabletLoader);
}

TEST_F(KVTabletFactoryTest, TestCreateDiskSegment)
{
    KVTabletFactory factory;
    factory.Init(_options, nullptr);
    BuildResource buildResource;
    std::shared_ptr<framework::MetricsManager> metricsManager =
        std::make_shared<framework::MetricsManager>("", nullptr);
    buildResource.metricsManager = metricsManager.get();

    std::unique_ptr<DiskSegment> builtSeg = factory.CreateDiskSegment(SegmentMeta(), buildResource);
    MultiShardDiskSegment* multiShardBuiltSeg = dynamic_cast<MultiShardDiskSegment*>(builtSeg.get());
    ASSERT_TRUE(multiShardBuiltSeg);
}

TEST_F(KVTabletFactoryTest, TestCreateMemSegment)
{
    {
        auto options = std::make_shared<config::TabletOptions>();
        KVTabletFactory factory;
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
        auto options = std::make_shared<config::TabletOptions>();
        FromJsonString(*options, jsonStr);
        KVTabletFactory factory;
        factory.Init(options, nullptr);
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
        auto options = std::make_shared<config::TabletOptions>();
        FromJsonString(*options, jsonStr);
        KVTabletFactory factory;
        factory.Init(options, nullptr);
        SegmentMeta segmentMeta;
        segmentMeta.schema = _schema;
        segmentMeta.segmentInfo = std::make_shared<framework::SegmentInfo>();
        segmentMeta.segmentInfo->SetShardCount(framework::SegmentInfo::INVALID_SHARDING_COUNT);
        std::unique_ptr<MemSegment> buildingSeg = factory.CreateMemSegment(segmentMeta);
        ASSERT_EQ(nullptr, buildingSeg);
    }
}

}} // namespace indexlibv2::table
