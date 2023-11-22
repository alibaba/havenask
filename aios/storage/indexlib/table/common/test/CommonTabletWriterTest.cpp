#include "indexlib/table/common/CommonTabletWriter.h"

#include "indexlib/base/MemoryQuotaController.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/framework/MetricsManager.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/mock/MockDocumentBatch.h"
#include "indexlib/framework/mock/MockMemSegment.h"
#include "unittest/unittest.h"

using namespace indexlibv2::framework;
using namespace indexlibv2::config;

namespace indexlibv2::table {

class CommonTabletWriterTest : public TESTBASE
{
public:
    CommonTabletWriterTest() {}
    ~CommonTabletWriterTest() {}

public:
    void setUp() override {}
    void tearDown() override {}

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.table, CommonTabletWriterTest);

class MockCommonTabletWriter : public CommonTabletWriter
{
public:
    MockCommonTabletWriter(const std::shared_ptr<config::TabletSchema>& schema, const config::TabletOptions* options)
        : CommonTabletWriter(schema, options)
    {
    }
    ~MockCommonTabletWriter() = default;

    int64_t getStartBuildTime() const { return _startBuildTime; }

public:
    MOCK_METHOD(int64_t, EstimateMaxMemoryUseOfCurrentSegment, (), (const, override));
};

TEST_F(CommonTabletWriterTest, TestNeedDump)
{
    auto schema = std::make_shared<TabletSchema>();
    auto options = std::make_shared<TabletOptions>();
    auto writer = std::make_shared<MockCommonTabletWriter>(schema, options.get());

    SegmentMeta segmentMeta(0 | Segment::PUBLIC_SEGMENT_ID_MASK);
    auto memSegment = std::make_shared<MockMemSegment>(segmentMeta, Segment::SegmentStatus::ST_BUILDING);
    auto tabletData = std::make_shared<TabletData>("simple");
    tabletData->TEST_PushSegment(memSegment);

    BuildResource buildResource;
    int64_t freeQuota = 1000;
    buildResource.buildingMemLimit = 5000;
    auto metricsManager = std::make_shared<MetricsManager>("", nullptr);
    buildResource.memController = std::make_shared<MemoryQuotaController>(freeQuota);
    buildResource.metricsManager = metricsManager.get();
    OpenOptions openOptions;
    ASSERT_FALSE(writer->Open(nullptr, buildResource, openOptions).IsOK());
    ASSERT_TRUE(writer->Open(tabletData, buildResource, openOptions).IsOK());

    buildResource.memController->Allocate(freeQuota);
    auto batch = std::make_shared<MockDocumentBatch>();
    sleep(2);
    ASSERT_TRUE(writer->Build(batch).IsNoMem());
    ASSERT_GT(autil::TimeUtility::currentTimeInSeconds(), writer->getStartBuildTime());
    buildResource.memController->Free(freeQuota);

    EXPECT_CALL(*memSegment, IsDirty()).WillRepeatedly(Return(true));
    EXPECT_CALL(*writer, EstimateMaxMemoryUseOfCurrentSegment()).WillRepeatedly(Return(1900));
    EXPECT_CALL(*memSegment, EvaluateCurrentMemUsed()).WillRepeatedly(Return(1000));
    ASSERT_TRUE(writer->Build(batch).IsNeedDump());

    writer->_buildResource.buildingMemLimit = 6000;
    ASSERT_TRUE(writer->Build(batch).IsNoMem());
}

TEST_F(CommonTabletWriterTest, TestCheckMemStatus)
{
    auto schema = std::make_shared<TabletSchema>();
    auto options = std::make_shared<TabletOptions>();
    auto writer = std::make_shared<MockCommonTabletWriter>(schema, options.get());

    SegmentMeta segmentMeta(0 | Segment::PUBLIC_SEGMENT_ID_MASK);
    auto memSegment = std::make_shared<MockMemSegment>(segmentMeta, Segment::SegmentStatus::ST_BUILDING);
    auto tabletData = std::make_shared<TabletData>("simple");
    tabletData->TEST_PushSegment(memSegment);

    BuildResource buildResource;
    int64_t freeQuota = 1000;
    buildResource.buildingMemLimit = 1899;
    auto metricsManager = std::make_shared<MetricsManager>("", nullptr);
    buildResource.memController = std::make_shared<MemoryQuotaController>(freeQuota);
    buildResource.metricsManager = metricsManager.get();
    OpenOptions openOptions;
    ASSERT_FALSE(writer->Open(nullptr, buildResource, openOptions).IsOK());
    ASSERT_TRUE(writer->Open(tabletData, buildResource, openOptions).IsOK());

    buildResource.memController->Allocate(freeQuota);
    auto batch = std::make_shared<MockDocumentBatch>();
    ASSERT_TRUE(writer->Build(batch).IsNoMem());
    buildResource.memController->Free(freeQuota);

    EXPECT_CALL(*memSegment, IsDirty()).WillRepeatedly(Return(true));
    EXPECT_CALL(*writer, EstimateMaxMemoryUseOfCurrentSegment()).WillRepeatedly(Return(1899));
    EXPECT_CALL(*memSegment, EvaluateCurrentMemUsed()).WillRepeatedly(Return(1000));
    ASSERT_TRUE(writer->Build(batch).IsNeedDump());

    ASSERT_TRUE(writer->CheckMemStatus().IsNeedDump());
    writer->_buildResource.buildingMemLimit = 1900;
    ASSERT_TRUE(writer->CheckMemStatus().IsOK());
}

} // namespace indexlibv2::table
