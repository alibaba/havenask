#include "indexlib/framework/VersionMerger.h"

#include <string>

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/framework/ResourceMap.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/VersionCommitter.h"
#include "indexlib/framework/VersionLoader.h"
#include "indexlib/framework/index_task/IndexTaskContext.h"
#include "indexlib/framework/index_task/IndexTaskPlan.h"
#include "unittest/unittest.h"

namespace indexlibv2::framework {

class VersionMergerTest : public TESTBASE
{
public:
    VersionMergerTest() {}
    ~VersionMergerTest() {}

    void setUp() override;
    void tearDown() override {}

private:
    std::string _indexRoot;
    indexlib::file_system::DirectoryPtr _rootDir;
};

void VersionMergerTest::setUp()
{
    _indexRoot = GET_TEMP_DATA_PATH();
    auto fs = indexlib::file_system::FileSystemCreator::Create("test", _indexRoot).GetOrThrow();
    _rootDir = indexlib::file_system::Directory::Get(fs);
}

namespace {
class FakeSegment : public Segment
{
public:
    FakeSegment(segmentid_t segmentId) : Segment(Segment::SegmentStatus::ST_BUILT)
    {
        _segmentMeta.segmentId = segmentId;
    }
    std::pair<Status, size_t> EstimateMemUsed(const std::shared_ptr<config::ITabletSchema>& schema) override
    {
        return {Status::OK(), 0};
    }
    size_t EvaluateCurrentMemUsed() override { return 0; }
};

Status MakeVersion(const std::shared_ptr<indexlib::file_system::Directory> dir, versionid_t versionId)
{
    framework::Version version;
    version.SetVersionId(versionId);
    return VersionCommitter::Commit(version, dir, {});
}

class FakeMergeController : public ITabletMergeController
{
public:
    FakeMergeController(const indexlib::file_system::DirectoryPtr rootDir)
        : _rootDir(rootDir)
        , _lastMergedVersionId(INVALID_VERSIONID)
        , _baseVersionId(INVALID_VERSIONID)
    {
    }

public:
    future_lite::coro::Lazy<Status> Recover() override { co_return Status::OK(); }
    future_lite::coro::Lazy<std::pair<Status, versionid_t>> GetLastMergeTaskResult() override
    {
        co_return std::make_pair(Status::OK(), INVALID_VERSIONID);
    }
    std::optional<ITabletMergeController::TaskStat> GetRunningTaskStat() const override
    {
        if (_taskStatus) {
            ITabletMergeController::TaskStat stat;
            stat.baseVersionId = _taskStatus->baseVersion.GetVersionId();
            return stat;
        }
        return std::nullopt;
    }
    std::unique_ptr<IndexTaskContext> CreateTaskContext(versionid_t baseVersionId, const std::string& taskType,
                                                        const std::string& taskName, const std::string& taskTraceId,
                                                        const std::map<std::string, std::string>& params) override
    {
        return std::make_unique<IndexTaskContext>();
    }
    future_lite::coro::Lazy<Status> SubmitMergeTask(std::unique_ptr<IndexTaskPlan> plan,
                                                    IndexTaskContext* context) override
    {
        auto targetVersionId = Version::MERGED_VERSION_ID_MASK | (++_lastMergedVersionId);
        [[maybe_unused]] auto status = MakeVersion(_rootDir, targetVersionId);
        assert(status.IsOK());
        MergeTaskStatus taskStatus;
        taskStatus.code = MergeTaskStatus::DONE;
        taskStatus.targetVersion = Version(_lastMergedVersionId);
        taskStatus.baseVersion = Version(_baseVersionId);
        _taskStatus = std::make_unique<MergeTaskStatus>(taskStatus);
        co_return Status::OK();
    }
    future_lite::coro::Lazy<std::pair<Status, MergeTaskStatus>> WaitMergeResult() override
    {
        auto taskStatus = *_taskStatus;
        co_return std::make_pair(Status::OK(), taskStatus);
    };
    void Stop() override { return; };
    Status CleanTask(bool) override
    {
        _taskStatus.reset();
        return Status::OK();
    }
    void SetBaseVersionId(versionid_t versionId) { _baseVersionId = versionId; }
    future_lite::coro::Lazy<Status> CancelCurrentTask() override
    {
        _taskStatus.reset();
        co_return Status::OK();
    }

private:
    indexlib::file_system::DirectoryPtr _rootDir;
    std::unique_ptr<MergeTaskStatus> _taskStatus;
    versionid_t _lastMergedVersionId;
    versionid_t _baseVersionId;
};

class FakePlanCreator : public IIndexTaskPlanCreator
{
public:
    FakePlanCreator() = default;

    std::pair<Status, std::unique_ptr<IndexTaskPlan>> CreateTaskPlan(const IndexTaskContext* context) override
    {
        return {Status::OK(), std::make_unique<IndexTaskPlan>("", "")};
    }
};

} // namespace

TEST_F(VersionMergerTest, testSimple)
{
    auto controller = std::make_shared<FakeMergeController>(_rootDir);
    auto planCreator = std::make_unique<FakePlanCreator>();
    VersionMerger merger(/*tabletName*/ "", _indexRoot, controller, std::move(planCreator), nullptr);

    Version baseVersion;
    baseVersion.SetVersionId(0 | Version::PUBLIC_VERSION_ID_MASK);
    for (segmentid_t i = 0; i < 2; ++i) {
        auto segId = i | Segment::PUBLIC_SEGMENT_ID_MASK;
        baseVersion.AddSegment(segId);
    }

    ASSERT_TRUE(VersionCommitter::Commit(baseVersion, _rootDir, {}).IsOK());
    controller->SetBaseVersionId(0 | Version::PUBLIC_VERSION_ID_MASK);

    merger.UpdateVersion(baseVersion);

    future_lite::coro::syncAwait(merger.Run());
    auto info = merger.GetMergedVersionInfo();
    ASSERT_TRUE(info);
    ASSERT_EQ(baseVersion.GetVersionId(), info->baseVersion.GetVersionId());
    ASSERT_EQ(Version::MERGED_VERSION_ID_MASK | 0, info->targetVersion.GetVersionId());
}

TEST_F(VersionMergerTest, testRecover)
{
    auto createVersion = [&](versionid_t parentVersion, versionid_t newVersion, const std::string& newFence) {
        Version version(newVersion);
        Version oldVersion;
        if (parentVersion != INVALID_VERSIONID) {
            auto [st, oldVersionPtr] = VersionLoader::GetVersion(_rootDir->GetIDirectory(), parentVersion);
            ASSERT_TRUE(st.IsOK());
            oldVersion = *oldVersionPtr;
        }
        auto versionLine = oldVersion.GetVersionLine();
        VersionCoord versionCoord(newVersion, newFence);
        versionLine.AddCurrentVersion(versionCoord);
        version.SetFenceName(newFence);
        version.SetVersionLine(versionLine);
        _rootDir->Store(version.GetVersionFileName(), version.ToString());
    };

    /*
                  0
                  |
                  1---
                  |  |
                  2  3


     */
    createVersion(INVALID_VERSIONID, 0, "fence0");
    createVersion(0, 1, "fence0");
    createVersion(1, 2, "fence0");
    createVersion(1, 3, "fence1");
    auto checkRecover = [&](versionid_t currentVersionId, versionid_t runningVersion,
                            versionid_t expectedRunningVersion) {
        auto controller = std::make_shared<FakeMergeController>(_rootDir);
        auto planCreator = std::make_unique<FakePlanCreator>();
        VersionMerger merger(/*tabletName*/ "", _indexRoot, controller, std::move(planCreator), nullptr, true);
        auto [st, currentVersion] = VersionLoader::GetVersion(_rootDir->GetIDirectory(), currentVersionId);
        ASSERT_TRUE(st.IsOK());
        merger.UpdateVersion(*currentVersion);
        if (runningVersion != INVALID_VERSIONID) {
            MergeTaskStatus taskStatus;
            taskStatus.baseVersion = Version(runningVersion);
            controller->_taskStatus = std::make_unique<MergeTaskStatus>(taskStatus);
            taskStatus.code = MergeTaskStatus::DONE;
            auto recoverStat = future_lite::coro::syncAwait(merger.EnsureRecovered());
            ASSERT_TRUE(recoverStat.IsOK());
        }
        auto taskStat = merger.GetRunningTaskStat();
        if (expectedRunningVersion == INVALID_VERSIONID) {
            ASSERT_EQ(std::nullopt, taskStat) << " current " << currentVersionId << " running " << runningVersion
                                              << " expected nullptr, but has" << std::endl;
        } else {
            ASSERT_EQ(expectedRunningVersion, taskStat->baseVersionId)
                << " current " << currentVersionId << " running " << runningVersion << " expected "
                << expectedRunningVersion << " actually " << taskStat->baseVersionId << std::endl;
        }
    };
    checkRecover(1, 0, 0);
    checkRecover(1, 1, 1);
    checkRecover(1, 2, INVALID_VERSIONID);

    checkRecover(3, 0, 0);
    checkRecover(3, 2, INVALID_VERSIONID);
}

} // namespace indexlibv2::framework
