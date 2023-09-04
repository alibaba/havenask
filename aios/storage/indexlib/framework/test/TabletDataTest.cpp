#include "indexlib/framework/TabletData.h"

#include "indexlib/framework/DiskSegment.h"
#include "indexlib/framework/MemSegment.h"
#include "indexlib/framework/ResourceMap.h"
#include "unittest/unittest.h"

namespace indexlibv2::framework {

class TabletDataTest : public TESTBASE
{
};
namespace {

class TestSegment : public DiskSegment
{
public:
    TestSegment(segmentid_t segId) : DiskSegment(SegmentMeta(segId)) {}
    size_t EvaluateCurrentMemUsed() override
    {
        // TODO(xiuhcen) impl
        return 0;
    }
    size_t EstimateMemUsed(const std::shared_ptr<config::ITabletSchema>& schema) override { return 0; }

private:
    Status Open(const std::shared_ptr<MemoryQuotaController>& memoryQuotaController,
                framework::DiskSegment::OpenMode mode) override
    {
        return Status::OK();
    }
    Status Reopen(const std::vector<std::shared_ptr<config::ITabletSchema>>& schema) override { return Status::OK(); }
};

class TestMemSegment : public MemSegment
{
public:
    TestMemSegment(segmentid_t segId) : MemSegment(SegmentMeta(segId)) {}
    Status Open(const BuildResource& resource, indexlib::framework::SegmentMetrics* lastSegmentMetrics) override
    {
        return Status::OK();
    }
    Status Build(document::IDocumentBatch* batch) override { return Status::OK(); }
    std::shared_ptr<indexlib::util::BuildResourceMetrics> GetBuildResourceMetrics() const override { return nullptr; }
    bool NeedDump() const override { return false; }
    std::pair<Status, std::vector<std::shared_ptr<SegmentDumpItem>>> CreateSegmentDumpItems() override
    {
        return {Status::OK(), {}};
    }
    void EndDump() override {}
    size_t EvaluateCurrentMemUsed() override
    {
        // TODO(xiuchen) impl
        return 0;
    }
    void Seal() override {}
    bool IsDirty() const override { return false; }

private:
    std::string _branchName;
};

} // namespace

TEST_F(TabletDataTest, testSlice)
{
    Version onDiskVersion;
    Version buildVersion;

    std::vector<std::shared_ptr<Segment>> builtSegments;
    std::vector<std::shared_ptr<Segment>> dumpingSegments;
    for (auto segId : {1, 2, 3}) {
        auto seg = std::make_shared<TestSegment>(segId);
        builtSegments.push_back(seg);
    }
    for (auto segId : {4, 5}) {
        auto seg = std::make_shared<TestMemSegment>(segId);
        seg->SetSegmentStatus(framework::Segment::SegmentStatus::ST_DUMPING);
        dumpingSegments.push_back(seg);
    }
    std::vector<std::shared_ptr<Segment>> allSegments;
    allSegments.insert(allSegments.end(), builtSegments.begin(), builtSegments.end());
    allSegments.insert(allSegments.end(), dumpingSegments.begin(), dumpingSegments.end());
    allSegments.push_back(std::make_shared<TestMemSegment>(6));
    TabletData tabletData("test");
    EXPECT_TRUE(tabletData.Init(std::move(onDiskVersion), allSegments, std::make_shared<ResourceMap>()).IsOK());

    auto builtSlice = tabletData.CreateSlice(Segment::SegmentStatus::ST_BUILT);
    ASSERT_EQ(builtSegments.size(), builtSlice.end() - builtSlice.begin());
    auto expectIter = builtSegments.begin();
    for (auto seg : builtSlice) {
        EXPECT_EQ((*expectIter)->GetSegmentId(), seg->GetSegmentId());
        ++expectIter;
    }

    auto dumpingSlice = tabletData.CreateSlice(Segment::SegmentStatus::ST_DUMPING);
    ASSERT_EQ(dumpingSegments.size(), dumpingSlice.rend() - dumpingSlice.rbegin());
    auto reverseExpectIter = dumpingSegments.rbegin();
    for (auto iter = dumpingSlice.rbegin(); iter != dumpingSlice.rend(); ++iter) {
        auto seg = *iter;
        EXPECT_EQ((*reverseExpectIter)->GetSegmentId(), seg->GetSegmentId());
        ++reverseExpectIter;
    }

    auto buildingSlice = tabletData.CreateSlice(Segment::SegmentStatus::ST_BUILDING);
    ASSERT_EQ(1u, buildingSlice.end() - buildingSlice.begin());
    auto iter = buildingSlice.begin();
    EXPECT_EQ(6, (*iter)->GetSegmentId());

    auto all = tabletData.CreateSlice(Segment::SegmentStatus::ST_UNSPECIFY);
    EXPECT_EQ(6u, all.end() - all.begin());
    std::vector<segmentid_t> expectedSegIds = {1, 2, 3, 4, 5, 6};
    size_t idx = 0;
    for (auto seg : all) {
        EXPECT_EQ(expectedSegIds[idx++], seg->GetSegmentId());
    }
}

} // namespace indexlibv2::framework
