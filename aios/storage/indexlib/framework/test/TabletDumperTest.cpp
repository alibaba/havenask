#include "indexlib/framework/TabletDumper.h"

#include "future_lite/executors/SimpleExecutor.h"
#include "indexlib/framework/Version.h"
#include "indexlib/framework/mock/MockMemSegment.h"
#include "indexlib/framework/mock/MockSegmentDumpItem.h"
#include "indexlib/framework/mock/MockSegmentDumper.h"
#include "unittest/unittest.h"

namespace indexlibv2::framework {

class TabletDumperTest : public TESTBASE
{
public:
    TabletDumperTest() = default;
    ~TabletDumperTest() = default;

    void setUp() override { _executor.reset(new future_lite::executors::SimpleExecutor(5)); }
    void tearDown() override {}

private:
    std::unique_ptr<future_lite::Executor> _executor = nullptr;
};

namespace {

auto createSegmentDumpItem(bool success)
{
    auto dumpItem = std::make_shared<MockSegmentDumpItem>();
    if (success) {
        EXPECT_CALL(*dumpItem, Dump()).WillRepeatedly(Return(Status::OK()));
    } else {
        EXPECT_CALL(*dumpItem, Dump()).WillRepeatedly(Return(Status::Corruption()));
    }
    return dumpItem;
}

auto createSegmentDumper(segmentid_t segmentId, const std::vector<std::shared_ptr<SegmentDumpItem>>& segmentDumpItems)
{
    SegmentMeta segmentMeta(segmentId);
    int64_t dumpExpandMemSize = 2048;
    auto memSegment = std::make_shared<MockMemSegment>(segmentMeta, Segment::SegmentStatus::ST_BUILDING);
    EXPECT_CALL(*memSegment, EvaluateCurrentMemUsed()).WillRepeatedly(Return(1024));
    EXPECT_CALL(*memSegment, CreateSegmentDumpItems())
        .WillRepeatedly(Return(std::make_pair(Status::OK(), segmentDumpItems)));
    EXPECT_CALL(*memSegment, EndDump()).WillRepeatedly(Return());
    auto segmentDumper = std::make_unique<MockSegmentDumper>(memSegment, dumpExpandMemSize, nullptr);
    EXPECT_CALL(*segmentDumper, StoreSegmentInfo()).WillRepeatedly(Return(Status::OK()));
    assert(memSegment->GetSegmentStatus() == Segment::SegmentStatus::ST_DUMPING);
    return segmentDumper;
}
} // namespace

TEST_F(TabletDumperTest, testSimple)
{
    int64_t dumperInterval = 1;
    auto tabletCommitter = std::make_shared<TabletCommitter>(std::string("test_dumper"));
    auto tabletDumper =
        std::make_shared<TabletDumper>(std::string("test_dumper"), _executor.get(), tabletCommitter.get());
    tabletDumper->Init(dumperInterval);
    auto segmentDumper = createSegmentDumper(512034, {});
    tabletDumper->PushSegmentDumper(std::move(segmentDumper));
    ASSERT_TRUE(tabletDumper->NeedDump());
    ASSERT_TRUE(tabletDumper->GetDumpQueueSize() == 1);
    ASSERT_TRUE(tabletDumper->GetTotalDumpingSegmentsMemsize() == 1024);
    ASSERT_TRUE(tabletDumper->GetMaxDumpingSegmentExpandMemsize() == 2048);
}

TEST_F(TabletDumperTest, testDump)
{
    {
        // test dump success
        int64_t dumperInterval = 1;
        auto dumpItem = createSegmentDumpItem(true);
        auto segmentDumper = createSegmentDumper(510234, {dumpItem});
        auto tabletCommitter = std::make_shared<TabletCommitter>(std::string("test_dumper"));
        auto tabletDumper =
            std::make_shared<TabletDumper>(std::string("test_dumper"), _executor.get(), tabletCommitter.get());
        tabletDumper->Init(dumperInterval);
        tabletDumper->PushSegmentDumper(std::move(segmentDumper));
        ASSERT_TRUE(tabletDumper->NeedDump());
        Status status = tabletDumper->Dump();
        ASSERT_TRUE(status.IsOK());
        ASSERT_TRUE(tabletCommitter->NeedCommit());
    }
    {
        // test executor NULL
        int64_t dumperInterval = 1;
        auto dumpItem = createSegmentDumpItem(true);
        auto segmentDumper = createSegmentDumper(510234, {dumpItem});
        auto tabletCommitter = std::make_shared<TabletCommitter>(std::string("test_dumper"));
        auto tabletDumper = std::make_shared<TabletDumper>(std::string("test_dumper"), nullptr, tabletCommitter.get());
        tabletDumper->Init(dumperInterval);
        tabletDumper->PushSegmentDumper(std::move(segmentDumper));
        ASSERT_TRUE(tabletDumper->NeedDump());
        Status status = tabletDumper->Dump();
        ASSERT_TRUE(status.IsIOError());
        ASSERT_FALSE(tabletCommitter->NeedCommit());
    }
    {
        // test dumper failed
        int64_t dumperInterval = 1;
        auto dumpItem = createSegmentDumpItem(false);
        auto segmentDumper = createSegmentDumper(510234, {dumpItem});
        auto tabletCommitter = std::make_shared<TabletCommitter>(std::string("test_dumper"));
        auto tabletDumper =
            std::make_shared<TabletDumper>(std::string("test_dumper"), _executor.get(), tabletCommitter.get());
        tabletDumper->Init(dumperInterval);
        tabletDumper->PushSegmentDumper(std::move(segmentDumper));
        ASSERT_TRUE(tabletDumper->NeedDump());
        Status status = tabletDumper->Dump();
        ASSERT_TRUE(status.IsCorruption());
        ASSERT_TRUE(tabletCommitter->NeedCommit());
    }
}

TEST_F(TabletDumperTest, testDumpWithTrim)
{
    int64_t dumperInterval = 1;
    auto tabletCommitter = std::make_shared<TabletCommitter>(std::string("test_dumper"));
    auto tabletDumper =
        std::make_shared<TabletDumper>(std::string("test_dumper"), _executor.get(), tabletCommitter.get());
    tabletDumper->Init(dumperInterval);

    static std::atomic<bool> isQuit = false;
    static constexpr size_t totalDumpCnt = 1000;
    static constexpr size_t sleepInterval = 1000; // 100 us
    std::thread pushThread([tabletDumper]() {
        size_t dumpCnt = 0;
        while (!isQuit) {
            segmentid_t segId = 510234;
            auto dumpItem = createSegmentDumpItem(true);
            auto segmentDumper = createSegmentDumper(segId++, {dumpItem});
            tabletDumper->PushSegmentDumper(std::move(segmentDumper));

            if (++dumpCnt == totalDumpCnt) {
                isQuit = true;
            }
        }
        usleep(5 * sleepInterval);
    });
    auto handle = pushThread.native_handle();
    pthread_setname_np(handle, "pushThread");

    std::thread dumpThread([tabletDumper]() {
        while (!isQuit) {
            Status status = tabletDumper->Dump();
            ASSERT_TRUE(status.IsOK());
            usleep(5 * sleepInterval);
        }
    });
    handle = dumpThread.native_handle();
    pthread_setname_np(handle, "dumpThread");
    std::thread trimThread([tabletDumper]() {
        TabletData tabletData("ut");
        while (!isQuit) {
            tabletDumper->TrimDumpingQueue(tabletData);
            usleep(sleepInterval);
        }
    });
    handle = trimThread.native_handle();
    pthread_setname_np(handle, "trimThread");
    if (pushThread.joinable()) {
        pushThread.join();
    }
    if (trimThread.joinable()) {
        trimThread.join();
    }
    if (dumpThread.joinable()) {
        dumpThread.join();
    }
    ASSERT_TRUE(true);
}
TEST_F(TabletDumperTest, testEstimate)
{
    SegmentMeta segmentMeta(0);
    auto memSegment = std::make_shared<MockMemSegment>(segmentMeta, Segment::SegmentStatus::ST_BUILDING);
    EXPECT_CALL(*memSegment, EvaluateCurrentMemUsed()).WillRepeatedly(Return(1024));
    auto createSegmentDumper = [memSegment](size_t expandMemsize) {
        return std::make_unique<SegmentDumper>("", memSegment, expandMemsize, /*empty report*/ nullptr);
    };
    auto tabletCommitter = std::make_shared<TabletCommitter>(std::string("test_dumper"));
    auto tabletDumper =
        std::make_shared<TabletDumper>(std::string("test_dumper"), _executor.get(), tabletCommitter.get());
    tabletDumper->PushSegmentDumper(createSegmentDumper(100));
    tabletDumper->PushSegmentDumper(createSegmentDumper(200));
    tabletDumper->PushSegmentDumper(createSegmentDumper(50));
    ASSERT_EQ(200, tabletDumper->GetMaxDumpingSegmentExpandMemsize());
    ASSERT_EQ(1024 * 3, tabletDumper->GetTotalDumpingSegmentsMemsize());
}
} // namespace indexlibv2::framework
