#include "indexlib/framework/TabletMetrics.h"

#include "future_lite/executors/SimpleExecutor.h"
#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/OnlineConfig.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/OpenOptions.h"
#include "indexlib/framework/ReadResource.h"
#include "indexlib/framework/ResourceMap.h"
#include "indexlib/framework/Tablet.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/TabletDataInfo.h"
#include "indexlib/framework/TabletInfos.h"
#include "indexlib/framework/TaskType.h"
#include "indexlib/framework/Version.h"
#include "indexlib/framework/mock/MockDiskSegment.h"
#include "indexlib/framework/mock/MockDocumentBatch.h"
#include "indexlib/framework/mock/MockMemSegment.h"
#include "indexlib/framework/mock/MockTabletFactory.h"
#include "indexlib/framework/mock/MockTabletLoader.h"
#include "indexlib/framework/mock/MockTabletOptions.h"
#include "indexlib/framework/mock/MockTabletReader.h"
#include "indexlib/framework/mock/MockTabletSchema.h"
#include "indexlib/framework/mock/MockTabletWriter.h"
#include "indexlib/framework/test/TabletTestAgent.h"
#include "unittest/unittest.h"

using testing::AtLeast;
using testing::ByMove;
using testing::ReturnRef;

namespace indexlibv2::framework {
MockTabletFactory* mockFactory = nullptr;
std::string TabletMetricsTestEmptyName = "";
class FakeMockTablet : public Tablet
{
public:
    FakeMockTablet(const framework::TabletResource& res) : Tablet(res) {}
    std::unique_ptr<framework::ITabletFactory>
    CreateTabletFactory(const std::string& tableType,
                        const std::shared_ptr<config::TabletOptions>& options) const override
    {
        return std::unique_ptr<MockTabletFactory>(mockFactory);
    }
};

class TabletMetricsTest : public TESTBASE
{
public:
    TabletMetricsTest() = default;
    ~TabletMetricsTest() = default;

public:
    void setUp() override
    {
        _executor.reset(new future_lite::executors::SimpleExecutor(2));
        _taskScheduler.reset(new future_lite::TaskScheduler(_executor.get()));
    }
    void tearDown() override {}

private:
    void CreateDefaultTablet()
    {
        kmonitor::MetricsTags metricsTags;
        indexlibv2::framework::TabletResource resource;
        resource.metricsReporter = std::make_shared<kmonitor::MetricsReporter>("", metricsTags, "");
        resource.dumpExecutor = _executor.get();
        resource.taskScheduler = _taskScheduler.get();
        resource.memoryQuotaController = std::make_shared<MemoryQuotaController>("ut", _memoryQuota);
        _tablet = std::make_unique<FakeMockTablet>(resource);
    }

private:
    std::unique_ptr<future_lite::Executor> _executor = nullptr;
    std::unique_ptr<future_lite::TaskScheduler> _taskScheduler = nullptr;
    std::unique_ptr<FakeMockTablet> _tablet = nullptr;
    int64_t _memoryQuota = 8 * 1024 * 1024; // same with double MemoryQuotaController::BLOCK_SIZE
};

void DisableCleanResourceTask(framework::Tablet* tablet)
{
    auto tabletTasker = framework::TabletTestAgent(tablet).TEST_GetTaskScheduler();
    std::string taskName = framework::TaskTypeToString(framework::TaskType::TT_CLEAN_RESOURCE);
    bool ret = tabletTasker->DeleteTask(taskName);
    ASSERT_TRUE(ret);
    ASSERT_TRUE(!tabletTasker->HasTask(taskName));
}

TEST_F(TabletMetricsTest, EmptyTest)
{
    CreateDefaultTablet();

    auto schema = std::make_shared<MockTabletSchema>();
    EXPECT_CALL(*schema, GetSchemaNameImpl()).Times(3).WillRepeatedly(ReturnRef(TabletMetricsTestEmptyName));
    EXPECT_CALL(*schema, SerializeImpl(_)).WillOnce(Return(true));
    auto options = std::make_unique<MockTabletOptions>();
    options->SetIsLeader(true);
    options->SetFlushLocal(false);
    options->SetFlushRemote(true);
    options->TEST_GetOnlineConfig().TEST_SetMaxRealtimeMemoryUse(INT64_MAX);
    options->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetEnablePackageFile(false);
    options->TEST_GetOnlineConfig().TEST_SetMaxRealtimeDumpIntervalSecond(-1);

    std::string localRoot = GET_TEMP_DATA_PATH();
    framework::IndexRoot indexRoot(localRoot, localRoot);
    framework::Version version;

    auto tabletLoader = std::make_unique<MockTabletLoader>();
    EXPECT_CALL(*tabletLoader, DoPreLoad(_, _, _)).WillOnce(Return(Status::OK()));
    auto tabletData = std::make_unique<TabletData>("demo");
    tabletData->_resourceMap = std::make_shared<ResourceMap>();
    EXPECT_CALL(*tabletLoader, FinalLoad(_))
        .WillOnce(Return(ByMove(std::pair<Status, std::unique_ptr<TabletData>>(Status::OK(), tabletData.release()))));
    auto tabletReader = std::make_unique<MockTabletReader>();
    EXPECT_CALL(*tabletReader, DoOpen(_, _)).WillOnce(Return(Status::OK()));

    mockFactory = new MockTabletFactory();
    EXPECT_CALL(*mockFactory, CreateTabletReader(_)).Times(1).WillOnce(Return(ByMove(std::move(tabletReader))));
    EXPECT_CALL(*mockFactory, CreateTabletLoader(_)).Times(1).WillOnce(Return(ByMove(std::move(tabletLoader))));
    ASSERT_TRUE(_tablet->Open(indexRoot, schema, std::move(options), version.GetVersionId()).IsOK());
    TabletMetrics* tabletMetrics = framework::TabletTestAgent(_tablet.get()).TEST_GetTabletMetrics();
    ASSERT_TRUE(tabletMetrics);
    DisableCleanResourceTask(_tablet.get());

    // int
    ASSERT_EQ(tabletMetrics->GetmemoryStatusValue(), 0);
    ASSERT_EQ(tabletMetrics->GettabletPhaseValue(), 0);
    ASSERT_EQ(tabletMetrics->GetdumpingSegmentCountValue(), 0);
    ASSERT_EQ(tabletMetrics->GetpartitionIndexSizeValue(), 0);
    ASSERT_EQ(tabletMetrics->GetpartitionReaderVersionCountValue(), 1);
    ASSERT_EQ(tabletMetrics->GetlatestReaderVersionIdValue(), INVALID_VERSIONID);
    ASSERT_EQ(tabletMetrics->GetoldestReaderVersionIdValue(), INVALID_VERSIONID);
    ASSERT_EQ(tabletMetrics->GetpartitionMemoryUseValue(), 0);
    ASSERT_EQ(tabletMetrics->GetincIndexMemoryUseValue(), 0);
    ASSERT_EQ(tabletMetrics->GetrtIndexMemoryUseValue(), 0);
    ASSERT_EQ(tabletMetrics->GetbuiltRtIndexMemoryUseValue(), 0);
    ASSERT_EQ(tabletMetrics->GetbuildingSegmentMemoryUseValue(), 0);
    ASSERT_EQ(tabletMetrics->GetpartitionMemoryQuotaUseValue(), 0);
    ASSERT_EQ(tabletMetrics->GettotalMemoryQuotaLimitValue(), _memoryQuota);
    ASSERT_EQ(tabletMetrics->GetfreeMemoryQuotaValue(), _memoryQuota);
    ASSERT_EQ(tabletMetrics->GetincVersionFreshnessValue(), 0);
    ASSERT_EQ(tabletMetrics->GetpartitionDocCountValue(), 0);
    ASSERT_EQ(tabletMetrics->GetsegmentCountValue(), 0);
    ASSERT_EQ(tabletMetrics->GetincSegmentCountValue(), 0);
    // double
    ASSERT_EQ(tabletMetrics->GetfreeMemoryQuotaRatioValue(), 1.0);
    ASSERT_EQ(tabletMetrics->GetpartitionMemoryQuotaUseRatioValue(), 0.0);
    ASSERT_EQ(tabletMetrics->GetrtIndexMemoryUseRatioValue(), 0.0);
}

TEST_F(TabletMetricsTest, 1BuildingSegment)
{
    CreateDefaultTablet();

    auto schema = std::make_shared<MockTabletSchema>();
    EXPECT_CALL(*schema, GetSchemaNameImpl()).Times(3).WillRepeatedly(ReturnRef(TabletMetricsTestEmptyName));
    EXPECT_CALL(*schema, SerializeImpl(_)).WillOnce(Return(true));
    auto options = std::make_unique<MockTabletOptions>();
    options->SetIsLeader(true);
    options->SetFlushLocal(false);
    options->SetFlushRemote(true);
    options->TEST_GetOnlineConfig().TEST_SetMaxRealtimeMemoryUse(1 * 1024 * 1024);
    options->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetBuildingMemoryLimit(1 * 1024 * 1024);
    options->TEST_GetOnlineConfig().TEST_SetPrintMetricsInterval(2);
    options->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetEnablePackageFile(false);
    options->TEST_GetOnlineConfig().TEST_SetMaxRealtimeDumpIntervalSecond(-1);

    std::string localRoot = GET_TEMP_DATA_PATH();
    framework::IndexRoot indexRoot(localRoot, localRoot);
    framework::Version version;

    auto tabletLoader = std::make_unique<MockTabletLoader>();
    EXPECT_CALL(*tabletLoader, DoPreLoad(_, _, _)).WillOnce(Return(Status::OK()));
    auto finalTabletData = std::make_unique<TabletData>("demo");
    finalTabletData->_resourceMap = std::make_shared<ResourceMap>();
    EXPECT_CALL(*tabletLoader, FinalLoad(_))
        .WillOnce(
            Return(ByMove(std::pair<Status, std::unique_ptr<TabletData>>(Status::OK(), finalTabletData.release()))));
    auto tabletReader = std::make_unique<MockTabletReader>();
    EXPECT_CALL(*tabletReader, DoOpen(_, _)).WillOnce(Return(Status::OK()));

    mockFactory = new MockTabletFactory();
    EXPECT_CALL(*mockFactory, CreateTabletReader(_)).Times(1).WillOnce(Return(ByMove(std::move(tabletReader))));
    EXPECT_CALL(*mockFactory, CreateTabletLoader(_)).Times(1).WillOnce(Return(ByMove(std::move(tabletLoader))));
    ASSERT_TRUE(_tablet->Open(indexRoot, schema, std::move(options), version.GetVersionId()).IsOK());
    auto tabletMetrics = framework::TabletTestAgent(_tablet.get()).TEST_GetTabletMetrics();
    ASSERT_TRUE(tabletMetrics);
    DisableCleanResourceTask(_tablet.get());

    auto tabletData = framework::TabletTestAgent(_tablet.get()).TEST_GetTabletData();
    ASSERT_TRUE(tabletData);

    segmentid_t segId = Segment::PUBLIC_SEGMENT_ID_MASK;
    SegmentMeta segMeta;
    segMeta.segmentId = segId;
    segMeta.segmentInfo->docCount = 2;
    tabletData->_tabletDataInfo->SetDocCount(2);
    auto buildingSegment = std::make_shared<MockMemSegment>(segMeta, Segment::SegmentStatus::ST_BUILDING);
    tabletData->TEST_PushSegment(buildingSegment);
    framework::TabletTestAgent(_tablet.get()).TEST_ReportMetrics();

    // int
    ASSERT_EQ(tabletMetrics->GetmemoryStatusValue(), 0); // tabletWriter == nullptr, so return 0
    ASSERT_EQ(tabletMetrics->GettabletPhaseValue(), 0);
    ASSERT_EQ(tabletMetrics->GetdumpingSegmentCountValue(), 0);
    ASSERT_EQ(tabletMetrics->GetpartitionIndexSizeValue(), 0);
    ASSERT_EQ(tabletMetrics->GetpartitionReaderVersionCountValue(), 1);
    ASSERT_EQ(tabletMetrics->GetlatestReaderVersionIdValue(), INVALID_VERSIONID);
    ASSERT_EQ(tabletMetrics->GetoldestReaderVersionIdValue(), INVALID_VERSIONID);
    ASSERT_EQ(tabletMetrics->GetpartitionMemoryUseValue(), 0); //
    ASSERT_EQ(tabletMetrics->GetincIndexMemoryUseValue(), 0);
    ASSERT_EQ(tabletMetrics->GetrtIndexMemoryUseValue(), 0);
    ASSERT_EQ(tabletMetrics->GetbuiltRtIndexMemoryUseValue(), 0);
    // tabletWriter is nullptr, so return 0
    ASSERT_EQ(tabletMetrics->GetbuildingSegmentMemoryUseValue(), 0); // building
    ASSERT_EQ(tabletMetrics->GetpartitionMemoryQuotaUseValue(), 0);
    ASSERT_EQ(tabletMetrics->GettotalMemoryQuotaLimitValue(), _memoryQuota);
    ASSERT_EQ(tabletMetrics->GetfreeMemoryQuotaValue(), _memoryQuota);
    ASSERT_EQ(tabletMetrics->GetincVersionFreshnessValue(), 0);
    ASSERT_EQ(tabletMetrics->GetpartitionDocCountValue(), 2); // 2 docs
    ASSERT_EQ(tabletMetrics->GetsegmentCountValue(), 1);
    ASSERT_EQ(tabletMetrics->GetincSegmentCountValue(), 0);
    // double
    ASSERT_EQ(tabletMetrics->GetfreeMemoryQuotaRatioValue(), 1.0);
    ASSERT_EQ(tabletMetrics->GetpartitionMemoryQuotaUseRatioValue(), 0.0);
    ASSERT_EQ(tabletMetrics->GetrtIndexMemoryUseRatioValue(), 0.0);
}

TEST_F(TabletMetricsTest, 1BuildingSegment2Dumping)
{
    CreateDefaultTablet();
    auto memoryQuotaController = framework::TabletTestAgent(_tablet.get()).TEST_GetTabletMemoryQuotaController();
    ASSERT_TRUE(memoryQuotaController->Reserve(4 * 1024 * 1024).IsOK());

    auto schema = std::make_shared<MockTabletSchema>();
    EXPECT_CALL(*schema, GetSchemaNameImpl()).WillRepeatedly(ReturnRef(TabletMetricsTestEmptyName));
    EXPECT_CALL(*schema, SerializeImpl(_)).WillOnce(Return(true));
    auto options = std::make_unique<MockTabletOptions>();
    options->SetIsLeader(true);
    options->SetFlushLocal(false);
    options->SetFlushRemote(true);
    options->TEST_GetOnlineConfig().TEST_SetMaxRealtimeMemoryUse(3 * 1024 * 1024);
    options->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetBuildingMemoryLimit(3 * 1024 * 1024);
    options->TEST_GetOnlineConfig().TEST_SetPrintMetricsInterval(0);
    options->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetEnablePackageFile(false);
    options->TEST_GetOnlineConfig().TEST_SetMaxRealtimeDumpIntervalSecond(-1);

    std::string localRoot = GET_TEMP_DATA_PATH();
    framework::IndexRoot indexRoot(localRoot, localRoot);
    framework::Version version;
    size_t buildingSegmentMemsize = 1024 * 1024;

    auto tabletLoader = std::make_unique<MockTabletLoader>();
    EXPECT_CALL(*tabletLoader, DoPreLoad(_, _, _)).WillOnce(Return(Status::OK()));
    EXPECT_CALL(*tabletLoader, EstimateMemUsed(_, _)).WillOnce(Return(std::make_pair(Status::OK(), 0)));
    auto finalTabletData = std::make_unique<TabletData>("demo");
    finalTabletData->_resourceMap = std::make_shared<ResourceMap>();
    EXPECT_CALL(*tabletLoader, FinalLoad(_))
        .WillOnce(
            Return(ByMove(std::pair<Status, std::unique_ptr<TabletData>>(Status::OK(), finalTabletData.release()))));
    auto tabletWriter = std::make_unique<MockTabletWriter>();
    EXPECT_CALL(*tabletWriter, Open(_, _, _)).WillOnce(Return(Status::OK()));
    EXPECT_CALL(*tabletWriter, Build(_)).Times(2).WillOnce(Return(Status::OK())).WillOnce(Return(Status::NeedDump()));
    EXPECT_CALL(*tabletWriter, GetTotalMemSize()).WillRepeatedly(Return(buildingSegmentMemsize));
    auto tabletWriter1 = std::make_unique<MockTabletWriter>();
    EXPECT_CALL(*tabletWriter1, Open(_, _, _)).WillOnce(Return(Status::OK()));
    EXPECT_CALL(*tabletWriter1, GetTotalMemSize()).WillRepeatedly(Return(buildingSegmentMemsize));
    EXPECT_CALL(*tabletWriter1, Build(_)).Times(2).WillOnce(Return(Status::OK())).WillOnce(Return(Status::NeedDump()));
    auto tabletWriter2 = std::make_unique<MockTabletWriter>();
    EXPECT_CALL(*tabletWriter2, Open(_, _, _)).WillOnce(Return(Status::OK()));
    EXPECT_CALL(*tabletWriter2, GetTotalMemSize()).WillRepeatedly(Return(buildingSegmentMemsize));
    EXPECT_CALL(*tabletWriter2, Build(_)).WillOnce(Return(Status::OK()));
    auto tabletReader = std::make_unique<MockTabletReader>();
    EXPECT_CALL(*tabletReader, DoOpen(_, _)).WillOnce(Return(Status::OK()));
    auto tabletReader1 = std::make_unique<MockTabletReader>();
    EXPECT_CALL(*tabletReader1, DoOpen(_, _)).WillOnce(Return(Status::OK()));
    auto tabletReader2 = std::make_unique<MockTabletReader>();
    EXPECT_CALL(*tabletReader2, DoOpen(_, _)).WillOnce(Return(Status::OK()));
    auto tabletReader3 = std::make_unique<MockTabletReader>();
    EXPECT_CALL(*tabletReader3, DoOpen(_, _)).WillOnce(Return(Status::OK()));

    segmentid_t segId = Segment::PUBLIC_SEGMENT_ID_MASK;
    SegmentMeta segMeta;
    Locator invalidLocator;
    segMeta.segmentId = segId;
    segMeta.segmentInfo->docCount = 2;
    segMeta.segmentInfo->SetLocator(invalidLocator);
    auto buildingSegment = std::make_unique<MockMemSegment>(segMeta, Segment::SegmentStatus::ST_BUILDING);
    auto buildingSegmentPtr = buildingSegment.get();
    ++segMeta.segmentId;
    auto buildingSegment1 = std::make_unique<MockMemSegment>(segMeta, Segment::SegmentStatus::ST_BUILDING);
    auto buildingSegmentPtr1 = buildingSegment1.get();
    ++segMeta.segmentId;
    auto buildingSegment2 = std::make_unique<MockMemSegment>(segMeta, Segment::SegmentStatus::ST_BUILDING);
    auto buildingSegmentPtr2 = buildingSegment2.get();
    ++segMeta.segmentId;
    auto dumpingSegment = std::make_shared<MockMemSegment>(segMeta, Segment::SegmentStatus::ST_DUMPING);
    Status st = Status::OK();
    EXPECT_CALL(*buildingSegment, EvaluateCurrentMemUsed()).WillRepeatedly(Return(buildingSegmentMemsize));
    EXPECT_CALL(*buildingSegment, Open(_, _)).WillRepeatedly(Return(st));
    EXPECT_CALL(*buildingSegment1, EvaluateCurrentMemUsed()).WillRepeatedly(Return(buildingSegmentMemsize));
    EXPECT_CALL(*buildingSegment1, Open(_, _)).WillRepeatedly(Return(st));
    EXPECT_CALL(*buildingSegment2, EvaluateCurrentMemUsed()).WillRepeatedly(Return(buildingSegmentMemsize));
    EXPECT_CALL(*buildingSegment2, Open(_, _)).WillRepeatedly(Return(st));
    EXPECT_CALL(*dumpingSegment, EvaluateCurrentMemUsed()).WillRepeatedly(Return(buildingSegmentMemsize));
    EXPECT_CALL(*tabletWriter, CreateSegmentDumper())
        .WillOnce(
            DoAll([buildingSegmentPtr]() { buildingSegmentPtr->SetSegmentStatus(Segment::SegmentStatus::ST_DUMPING); },
                  Return(ByMove(std::make_unique<SegmentDumper>("", dumpingSegment, 0, nullptr)))));
    EXPECT_CALL(*tabletWriter, ReportMetrics()).WillRepeatedly(Return());
    EXPECT_CALL(*tabletWriter, GetBuildingSegmentDumpExpandSize()).WillRepeatedly(Return(0));
    EXPECT_CALL(*tabletWriter1, CreateSegmentDumper())
        .WillOnce(DoAll(
            [buildingSegmentPtr1]() { buildingSegmentPtr1->SetSegmentStatus(Segment::SegmentStatus::ST_DUMPING); },
            Return(ByMove(std::make_unique<SegmentDumper>("", dumpingSegment, 0, nullptr)))));
    EXPECT_CALL(*tabletWriter1, ReportMetrics()).WillRepeatedly(Return());
    EXPECT_CALL(*tabletWriter1, GetBuildingSegmentDumpExpandSize()).WillRepeatedly(Return(0));
    EXPECT_CALL(*tabletWriter2, ReportMetrics()).WillRepeatedly(Return());

    mockFactory = new MockTabletFactory();
    std::unique_ptr<IIndexTaskPlanCreator> emptyCreator;
    EXPECT_CALL(*mockFactory, CreateIndexTaskPlanCreator()).WillOnce(Return(ByMove(std::move(emptyCreator))));
    EXPECT_CALL(*mockFactory, CreateTabletReader(_))
        .Times(4)
        .WillOnce(Return(ByMove(std::move(tabletReader))))
        .WillOnce(Return(ByMove(std::move(tabletReader1))))
        .WillOnce(Return(ByMove(std::move(tabletReader2))))
        .WillOnce(Return(ByMove(std::move(tabletReader3))));
    EXPECT_CALL(*mockFactory, CreateTabletWriter(_))
        .Times(3)
        .WillOnce(Return(ByMove(std::move(tabletWriter))))
        .WillOnce(Return(ByMove(std::move(tabletWriter1))))
        .WillOnce(Return(ByMove(std::move(tabletWriter2))));
    EXPECT_CALL(*mockFactory, CreateTabletLoader(_)).Times(1).WillOnce(Return(ByMove(std::move(tabletLoader))));
    EXPECT_CALL(*mockFactory, CreateMemSegment(_))
        .Times(3)
        .WillOnce(Return(ByMove(std::move(buildingSegment))))
        .WillOnce(Return(ByMove(std::move(buildingSegment1))))
        .WillOnce(Return(ByMove(std::move(buildingSegment2))));
    // buildingSegment + buildingSegment1 + buildingSegment2
    memoryQuotaController->Allocate(3 * buildingSegmentMemsize);
    ASSERT_TRUE(_tablet->Open(indexRoot, schema, std::move(options), version.GetVersionId()).IsOK());
    auto tabletMetrics = framework::TabletTestAgent(_tablet.get()).TEST_GetTabletMetrics();
    ASSERT_TRUE(tabletMetrics);

    // DisableCleanResourceTask(_tablet.get());
    framework::TabletTestAgent(_tablet.get()).TEST_GetTaskScheduler()->CleanTasks();

    Locator locator1(0, 1);
    Locator locator2(0, 2);
    Locator locator3(0, 3);
    Locator locator4(0, 1);
    auto docBatch1 = std::make_shared<MockDocumentBatch>();
    auto docBatch2 = std::make_shared<MockDocumentBatch>();
    auto docBatch3 = std::make_shared<MockDocumentBatch>();
    auto docBatch4 = std::make_shared<MockDocumentBatch>();
    EXPECT_CALL(*docBatch1, GetLastLocator()).WillRepeatedly(ReturnRef(locator1));
    EXPECT_CALL(*docBatch2, GetLastLocator()).WillRepeatedly(ReturnRef(locator2));
    EXPECT_CALL(*docBatch3, GetLastLocator()).WillRepeatedly(ReturnRef(locator3));
    EXPECT_CALL(*docBatch4, GetLastLocator()).WillRepeatedly(ReturnRef(locator4));
    buildingSegmentPtr->_segmentMeta.segmentInfo->SetLocator(locator1);
    ASSERT_TRUE(_tablet->Build(docBatch1).IsOK());
    buildingSegmentPtr1->_segmentMeta.segmentInfo->SetLocator(locator2);
    ASSERT_TRUE(_tablet->Build(docBatch2).IsOK());
    // check locator rollback
    auto getFault = [tabletMetrics]() {
        std::map<std::string, std::string> infoMap;
        tabletMetrics->FillMetricsInfo(infoMap);
        TabletFaultManager _manager;
        FromJsonString(_manager, infoMap["tabletFaults"]);
        return _manager;
    };
    ASSERT_EQ(0, getFault().GetFaultCount());
    ASSERT_TRUE(_tablet->Build(docBatch4).IsOK());
    ASSERT_EQ(1, getFault().GetFaultCount());

    buildingSegmentPtr2->_segmentMeta.segmentInfo->SetLocator(locator3);
    ASSERT_TRUE(_tablet->Build(docBatch3).IsOK());

    auto tabletInfos = _tablet->GetTabletInfos();
    ASSERT_TRUE(tabletInfos);
    // int
    ASSERT_EQ(tabletMetrics->GetmemoryStatusValue(), 1); // buildQuota = 3M, which equal to 1building + 2dumping
    ASSERT_EQ(tabletInfos->GetMemoryStatus(), MemoryStatus::REACH_MAX_RT_INDEX_SIZE);
    ASSERT_EQ(tabletMetrics->GettabletPhaseValue(), 0);
    ASSERT_EQ(tabletMetrics->GetdumpingSegmentCountValue(), 2); // 1building + 2dumping
    ASSERT_EQ(tabletMetrics->GetpartitionIndexSizeValue(), 0);  // 0 inc + 0 rt
    ASSERT_EQ(tabletMetrics->GetpartitionReaderVersionCountValue(), 4);
    ASSERT_EQ(tabletMetrics->GetlatestReaderVersionIdValue(), INVALID_VERSIONID);
    ASSERT_EQ(tabletMetrics->GetoldestReaderVersionIdValue(), INVALID_VERSIONID);
    ASSERT_EQ(tabletMetrics->GetbuildingSegmentMemoryUseValue(), buildingSegmentMemsize);    // building
    ASSERT_EQ(tabletMetrics->GetdumpingSegmentMemoryUseValue(), 2 * buildingSegmentMemsize); // dumping
    ASSERT_EQ(tabletMetrics->GetbuiltRtIndexMemoryUseValue(), 0);
    ASSERT_EQ(tabletMetrics->GetpartitionMemoryUseValue(), buildingSegmentMemsize * 3);
    ASSERT_EQ(tabletMetrics->GetincIndexMemoryUseValue(), 0);
    ASSERT_EQ(tabletMetrics->GetrtIndexMemoryUseValue(), 3 * buildingSegmentMemsize);
    ASSERT_EQ(tabletMetrics->GetpartitionMemoryQuotaUseValue(), 3 * buildingSegmentMemsize);
    ASSERT_EQ(tabletMetrics->GettotalMemoryQuotaLimitValue(), _memoryQuota);
    ASSERT_EQ(tabletMetrics->GetfreeMemoryQuotaValue(), 4 * 1024 * 1024); // left 4MB. already allocate 4M, total 8MB
    ASSERT_EQ(tabletMetrics->GetincVersionFreshnessValue(), 0);
    ASSERT_EQ(tabletMetrics->GetpartitionDocCountValue(), 6);
    ASSERT_EQ(tabletMetrics->GetsegmentCountValue(), 3);
    ASSERT_EQ(tabletMetrics->GetincSegmentCountValue(), 0);
    // double
    ASSERT_EQ(tabletMetrics->GetfreeMemoryQuotaRatioValue(), 0.5);
    ASSERT_EQ(tabletMetrics->GetpartitionMemoryQuotaUseRatioValue(), 3.0 / 8);
    ASSERT_EQ(tabletMetrics->GetrtIndexMemoryUseRatioValue(), 1);
}

TEST_F(TabletMetricsTest, testAddTabletFault)
{
    CreateDefaultTablet();

    auto schema = std::make_shared<MockTabletSchema>();
    EXPECT_CALL(*schema, GetSchemaNameImpl()).Times(3).WillRepeatedly(ReturnRef(TabletMetricsTestEmptyName));
    EXPECT_CALL(*schema, SerializeImpl(_)).WillOnce(Return(true));
    auto options = std::make_unique<MockTabletOptions>();
    options->SetIsLeader(true);
    options->SetFlushLocal(false);
    options->SetFlushRemote(true);
    options->TEST_GetOnlineConfig().TEST_SetMaxRealtimeMemoryUse(INT64_MAX);
    options->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetEnablePackageFile(false);
    options->TEST_GetOnlineConfig().TEST_SetMaxRealtimeDumpIntervalSecond(-1);

    std::string localRoot = GET_TEMP_DATA_PATH();
    framework::IndexRoot indexRoot(localRoot, localRoot);
    framework::Version version;

    auto tabletLoader = std::make_unique<MockTabletLoader>();
    EXPECT_CALL(*tabletLoader, DoPreLoad(_, _, _)).WillOnce(Return(Status::OK()));
    auto tabletData = std::make_unique<TabletData>("demo");
    tabletData->_resourceMap = std::make_shared<ResourceMap>();
    EXPECT_CALL(*tabletLoader, EstimateMemUsed(_, _)).WillOnce(Return(std::make_pair(Status::OK(), 0)));
    EXPECT_CALL(*tabletLoader, FinalLoad(_))
        .WillOnce(Return(ByMove(std::pair<Status, std::unique_ptr<TabletData>>(Status::OK(), tabletData.release()))));
    auto tabletReader = std::make_unique<MockTabletReader>();
    EXPECT_CALL(*tabletReader, DoOpen(_, _)).WillOnce(Return(Status::OK()));

    mockFactory = new MockTabletFactory();
    EXPECT_CALL(*mockFactory, CreateTabletReader(_)).Times(1).WillOnce(Return(ByMove(std::move(tabletReader))));
    EXPECT_CALL(*mockFactory, CreateTabletLoader(_)).Times(1).WillOnce(Return(ByMove(std::move(tabletLoader))));
    std::unique_ptr<IIndexTaskPlanCreator> emptyCreator;
    EXPECT_CALL(*mockFactory, CreateIndexTaskPlanCreator()).Times(1).WillOnce(Return(ByMove(std::move(emptyCreator))));
    ASSERT_TRUE(_tablet->Open(indexRoot, schema, std::move(options), version.GetVersionId()).IsOK());
    TabletMetrics* tabletMetrics = framework::TabletTestAgent(_tablet.get()).TEST_GetTabletMetrics();
    ASSERT_TRUE(tabletMetrics != nullptr);
    DisableCleanResourceTask(_tablet.get());
    auto getFault = [tabletMetrics]() {
        std::map<std::string, std::string> infoMap;
        tabletMetrics->FillMetricsInfo(infoMap);
        TabletFaultManager _manager;
        FromJsonString(_manager, infoMap["tabletFaults"]);
        return _manager;
    };
    tabletMetrics->AddTabletFault("locator rollback");
    ASSERT_EQ(1, getFault().GetFaultCount());
    tabletMetrics->AddTabletFault("locator rollback");
    ASSERT_EQ(1, getFault().GetFaultCount());
    tabletMetrics->AddTabletFault("unexpceted build");
    ASSERT_EQ(2, getFault().GetFaultCount());
}

TEST_F(TabletMetricsTest, 1BuildingSegment2Dumping2Inc)
{
    // CreateDefaultTablet();

    // auto tabletMetrics = framework::TabletTestAgent(_tablet.get()).TEST_GetTabletMetrics();
    // ASSERT_TRUE(tabletMetrics != nullptr);
    // auto schema = std::make_shared<MockTabletSchema>();
    // EXPECT_CALL(*schema, GetSchemaNameImpl()).WillOnce(ReturnRef(TabletMetricsTestEmptyName));
    // EXPECT_CALL(*schema, SerializeImpl()).WillOnce(Return(""));
    // auto options = std::make_unique<MockTabletOptions>();
    // EXPECT_CALL(*options, FlushLocal()).WillRepeatedly(Return(false));
    // EXPECT_CALL(*options, FlushRemote()).WillRepeatedly(Return(true));
    // EXPECT_CALL(*options, GetBuildMemoryQuota()).WillRepeatedly(Return(4 * 1024 * 1024));
    // EXPECT_CALL(*options, GetPrintMetricsInterval()).WillRepeatedly(Return(0));
    // EXPECT_CALL(*options, IsLeader()).Times(AtLeast(1)).WillRepeatedly(Return(true));
    // EXPECT_CALL(*options, GetBuildingMemoryLimit()).WillRepeatedly(Return(INT64_MAX));
    // EXPECT_CALL(*options, IsEnablePackageFile()).WillRepeatedly(Return(false));
    // EXPECT_CALL(*options, GetMaxRealtimeDumpIntervalSecond())
    //     .WillRepeatedly(Return(config::TabletOptions::DEFAULT_MAX_REALTIME_DUMP_INTERVAL_SECOND));
    // auto loadConfigList = indexlib::file_system::LoadConfigList();
    // EXPECT_CALL(*options, GetLoadConfigList()).WillRepeatedly(ReturnRef(loadConfigList));

    // std::string localRoot = GET_TEMP_DATA_PATH();
    // framework::IndexRoot indexRoot(localRoot, localRoot);
    // framework::Version version;
    // size_t buildingSegmentMemsize = 1024 * 1024;
    // size_t incSegmentMemsize = 0.5 * 1024 * 1024;

    // auto tabletLoader = std::make_unique<MockTabletLoader>();
    // auto tabletLoader1 = std::make_unique<MockTabletLoader>();
    // EXPECT_CALL(*tabletLoader, DoPreLoad(_, _, _)).WillOnce(Return(Status::OK()));
    // auto finalTabletData = std::make_unique<TabletData>("demo");
    // EXPECT_CALL(*tabletLoader, FinalLoad())
    //     .WillOnce(
    //         Return(ByMove(std::pair<Status, std::unique_ptr<TabletData>>(Status::OK(), finalTabletData.release()))));
    // EXPECT_CALL(*tabletLoader1, DoPreLoad(_, _, _)).WillOnce(Return(Status::OK()));
    // auto finalTabletData1 = std::make_unique<TabletData>("demo1");
    // auto tabletWriter = std::make_unique<MockTabletWriter>();
    // EXPECT_CALL(*tabletWriter, Open(_, _)).WillOnce(Return(Status::OK()));
    // EXPECT_CALL(*tabletWriter,
    // Build(_)).Times(2).WillOnce(Return(Status::OK())).WillOnce(Return(Status::NeedDump()));
    // EXPECT_CALL(*tabletWriter, GetTotalMemSize()).WillRepeatedly(Return(buildingSegmentMemsize));
    // auto tabletWriter1 = std::make_unique<MockTabletWriter>();
    // EXPECT_CALL(*tabletWriter1, Open(_, _)).WillOnce(Return(Status::OK()));
    // EXPECT_CALL(*tabletWriter1, GetTotalMemSize()).WillRepeatedly(Return(buildingSegmentMemsize));
    // EXPECT_CALL(*tabletWriter1,
    // Build(_)).Times(2).WillOnce(Return(Status::OK())).WillOnce(Return(Status::NeedDump())); auto tabletWriter2 =
    // std::make_unique<MockTabletWriter>(); EXPECT_CALL(*tabletWriter2, Open(_, _)).WillOnce(Return(Status::OK()));
    // EXPECT_CALL(*tabletWriter2, GetTotalMemSize()).WillRepeatedly(Return(buildingSegmentMemsize));
    // EXPECT_CALL(*tabletWriter2, Build(_)).WillOnce(Return(Status::OK()));
    // auto tabletReader = std::make_unique<MockTabletReader>();
    // EXPECT_CALL(*tabletReader, Open(_, _)).WillOnce(Return(Status::OK()));
    // auto tabletReader1 = std::make_unique<MockTabletReader>();
    // EXPECT_CALL(*tabletReader1, Open(_, _)).WillOnce(Return(Status::OK()));
    // auto tabletReader2 = std::make_unique<MockTabletReader>();
    // EXPECT_CALL(*tabletReader2, Open(_, _)).WillOnce(Return(Status::OK()));
    // auto tabletReader3 = std::make_unique<MockTabletReader>();
    // EXPECT_CALL(*tabletReader3, Open(_, _)).WillOnce(Return(Status::OK()));
    // auto tabletReader4 = std::make_unique<MockTabletReader>();
    // EXPECT_CALL(*tabletReader4, Open(_, _)).WillOnce(Return(Status::OK()));

    // segmentid_t segId = Segment::PUBLIC_SEGMENT_ID_MASK;
    // SegmentMeta segMeta;
    // segMeta.segmentId = segId;
    // segMeta.segmentInfo->docCount = 2;
    // auto buildingSegment = std::make_unique<MockMemSegment>(segMeta, Segment::SegmentStatus::ST_BUILDING);
    // ++segMeta.segmentId;
    // auto buildingSegment1 = std::make_unique<MockMemSegment>(segMeta, Segment::SegmentStatus::ST_DUMPING);
    // ++segMeta.segmentId;
    // auto buildingSegment2 = std::make_unique<MockMemSegment>(segMeta, Segment::SegmentStatus::ST_DUMPING);
    // ++segMeta.segmentId;
    // auto dumpingSegment = std::make_shared<MockMemSegment>(segMeta, Segment::SegmentStatus::ST_DUMPING);
    // SegmentMeta segMeta1;
    // segMeta1.segmentId = Segment::MERGED_SEGMENT_ID_MASK;
    // auto incBuiltSegment = std::make_shared<MockDiskSegment>(segMeta1);
    // auto tmpIncBuiltSegment = std::make_unique<MockDiskSegment>(segMeta1);
    // ++segMeta1.segmentId;
    // segMeta1.segmentInfo->docCount = 20;
    // auto incBuiltSegment1 = std::make_shared<MockDiskSegment>(segMeta1);
    // auto tmpIncBuiltSegment1 = std::make_unique<MockDiskSegment>(segMeta1);
    // Status st = Status::OK();
    // EXPECT_CALL(*buildingSegment, EvaluateCurrentMemUsed()).WillRepeatedly(Return(buildingSegmentMemsize));
    // EXPECT_CALL(*buildingSegment, Open(_, _)).WillRepeatedly(Return(st));
    // EXPECT_CALL(*buildingSegment1, EvaluateCurrentMemUsed()).WillRepeatedly(Return(buildingSegmentMemsize));
    // EXPECT_CALL(*buildingSegment1, Open(_, _)).WillRepeatedly(Return(st));
    // EXPECT_CALL(*buildingSegment2, EvaluateCurrentMemUsed()).WillRepeatedly(Return(buildingSegmentMemsize));
    // EXPECT_CALL(*buildingSegment2, Open(_, _)).WillRepeatedly(Return(st));
    // EXPECT_CALL(*dumpingSegment, EvaluateCurrentMemUsed()).WillRepeatedly(Return(buildingSegmentMemsize));
    // EXPECT_CALL(*incBuiltSegment, EvaluateCurrentMemUsed()).WillRepeatedly(Return(incSegmentMemsize));
    // EXPECT_CALL(*tmpIncBuiltSegment, Open(_)).WillRepeatedly(Return(st));
    // EXPECT_CALL(*incBuiltSegment1, EvaluateCurrentMemUsed()).WillRepeatedly(Return(incSegmentMemsize));
    // EXPECT_CALL(*tmpIncBuiltSegment1, Open(_)).WillRepeatedly(Return(st));

    // EXPECT_CALL(*tabletWriter, CreateSegmentDumper())
    //     .WillOnce(Return(ByMove(std::make_unique<SegmentDumper>(dumpingSegment, 1024 * 1024, nullptr))));
    // EXPECT_CALL(*tabletWriter, ReportMetrics()).WillRepeatedly(Return());
    // EXPECT_CALL(*tabletWriter, GetBuildingSegmentDumpExpandSize()).WillRepeatedly(Return(0));
    // EXPECT_CALL(*tabletWriter1, CreateSegmentDumper())
    //     .WillOnce(Return(ByMove(std::make_unique<SegmentDumper>(dumpingSegment, 1024 * 1024, nullptr))));
    // EXPECT_CALL(*tabletWriter1, ReportMetrics()).WillRepeatedly(Return());
    // EXPECT_CALL(*tabletWriter1, GetBuildingSegmentDumpExpandSize()).WillRepeatedly(Return(0));
    // EXPECT_CALL(*tabletWriter2, ReportMetrics()).WillRepeatedly(Return());
    // EXPECT_CALL(*tabletWriter2, GetBuildingSegmentDumpExpandSize()).WillRepeatedly(Return(1024 * 1024));
    // mockFactory = new MockTabletFactory();
    // EXPECT_CALL(*mockFactory, CreateTabletReader())
    //     .Times(5)
    //     .WillOnce(Return(ByMove(std::move(tabletReader))))
    //     .WillOnce(Return(ByMove(std::move(tabletReader1))))
    //     .WillOnce(Return(ByMove(std::move(tabletReader2))))
    //     .WillOnce(Return(ByMove(std::move(tabletReader3))))
    //     .WillOnce(Return(ByMove(std::move(tabletReader4))));
    // EXPECT_CALL(*mockFactory, CreateTabletWriter())
    //     .Times(3)
    //     .WillOnce(Return(ByMove(std::move(tabletWriter))))
    //     .WillOnce(Return(ByMove(std::move(tabletWriter1))))
    //     .WillOnce(Return(ByMove(std::move(tabletWriter2))));
    // EXPECT_CALL(*mockFactory, CreateTabletLoader(_)).Times(1).WillOnce(Return(ByMove(std::move(tabletLoader))));
    // EXPECT_CALL(*mockFactory, CreateMemSegment(_))
    //     .Times(3)
    //     .WillOnce(Return(ByMove(std::move(buildingSegment))))
    //     .WillOnce(Return(ByMove(std::move(buildingSegment1))))
    //     .WillOnce(Return(ByMove(std::move(buildingSegment2))));
    // EXPECT_CALL(*mockFactory, CreateDiskSegment(_))
    //     .Times(2)
    //     .WillOnce(Return(ByMove(std::move(tmpIncBuiltSegment))))
    //     .WillOnce(Return(ByMove(std::move(tmpIncBuiltSegment1))));
    // // buildingSegment + buildingSegment1 + buildingSegment2
    // auto memoryQuotaController = framework::TabletTestAgent(_tablet.get()).TESTo_GetTabletMemoryQuotaController();
    // memoryQuotaController->Allocate(3 * buildingSegmentMemsize);
    // memoryQuotaController->Allocate(2 * incSegmentMemsize);
    // ASSERT_TRUE(_tablet->Open(indexRoot, schema, std::move(options), version).IsOK());
    // DisableCleanResourceTask(_tablet.get());
    // Locator locator;
    // MockDocumentBatch docBatch;
    // EXPECT_CALL(docBatch, GetLastLocator()).WillRepeatedly(ReturnRef(locator));
    // ASSERT_TRUE(_tablet->Build(&docBatch).IsOK());
    // ASSERT_TRUE(_tablet->Build(&docBatch).IsOK());
    // ASSERT_TRUE(_tablet->Build(&docBatch).IsOK());

    // // using fsWrapper = indexlib::file_system::FslibWrapper;
    // using fsEC = indexlib::file_system::ErrorCode;
    // auto rootPath = GET_TEMP_DATA_PATH();
    // auto rootDir = indexlib::file_system::Directory::Get(rootPath);
    // ASSERT_TRUE(rootDir != nullptr);
    // auto segDir0 = rootDir->MakeDirectory(version.GetSegmentDirName(0));
    // ASSERT_TRUE(segDir0 != nullptr);
    // segDir0->Store("segment_info", /* empty content */"");
    // auto segDir1 = rootDir->MakeDirectory(version.GetSegmentDirName(1));
    // ASSERT_TRUE(segDir1 != nullptr);
    // segDir1->Store("segment_info", /* empty content */"");

    // // auto ec = fsWrapper::MkDir(rootPath + "/" + version.GetSegmentDirName(0));
    // // ASSERT_TRUE(ec == fsEC::FSEC_OK || ec == fsEC::FSEC_EXIST);
    // // // ec = fsWrapper::AtomicStore(rootPath + version.GetSegmentDirName(0) + "/segment_info",
    // // ec = fsWrapper::AtomicStore(rootPath + "segment_0/segment_info",
    // //                             segMeta1.segmentInfo->ToString());
    // // ASSERT_TRUE(ec == fsEC::FSEC_OK || ec == fsEC::FSEC_EXIST);
    // // ec = fsWrapper::MkDir(rootPath + version.GetSegmentDirName(1));
    // // ASSERT_TRUE(ec == fsEC::FSEC_OK || ec == fsEC::FSEC_EXIST);
    // // // ec = fsWrapper::AtomicStore(rootPath + version.GetSegmentDirName(1) + "/segment_info",
    // // ec = fsWrapper::AtomicStore(rootPath + "segment_1/segment_info",
    // //                             segMeta1.segmentInfo->ToString());
    // // ASSERT_TRUE(ec == fsEC::FSEC_OK || ec == fsEC::FSEC_EXIST);
    // int64_t latestVersionTs = 10;
    // version.AddSegment(0);
    // version.AddSegment(1);
    // version.SetTimestamp(latestVersionTs);
    // version.IncVersionId();
    // rootDir->Store("version.0", /* empty content */ "");
    // // ec = fsWrapper::AtomicStore(rootPath + "/version.0", version.ToString());
    // // ASSERT_TRUE(ec == fsEC::FSEC_OK || ec == fsEC::FSEC_EXIST);
    // rootDir->Sync(/*waitFinish*/ true);
    // // auto ec = rootDir->GetFileSystem()->TEST_Commit(0, "version.0", version.ToString());
    // auto fs = rootDir->GetFileSystem();
    // auto ec = fs->TEST_Commit(0, "version.0", version.ToString());
    // ASSERT_TRUE(ec == fsEC::FSEC_OK || ec == fsEC::FSEC_EXIST);
    // std::vector<std::shared_ptr<Segment>> segments {incBuiltSegment, incBuiltSegment1};
    // ASSERT_TRUE(finalTabletData1->Init(version, segments).IsOK());
    // EXPECT_CALL(*tabletLoader1, FinalLoad())
    //     .WillOnce(
    //         Return(ByMove(std::pair<Status, std::unique_ptr<TabletData>>(Status::OK(),
    //         finalTabletData1.release()))));
    // EXPECT_CALL(*mockFactory, CreateTabletLoader(_)).Times(1).WillOnce(Return(ByMove(std::move(tabletLoader1))));
    // ReopenOptions reopenOptions;
    // ASSERT_TRUE(_tablet->Reopen(reopenOptions, version).IsOK());

    // TabletInfos* tabletInfos = _tablet->GetTabletInfos();
    // ASSERT_NE(tabletInfos, nullptr);
    // // int
    // // buildQuota is 4M, which greater than 1building + 2dumping(3M)
    // ASSERT_EQ(tabletMetrics->GetmemoryStatusValue(), 2);
    // ASSERT_EQ(tabletInfos->GetMemoryStatus(), MemoryStatus::REACH_TOTAL_MEM_LIMIT);
    // ASSERT_EQ(tabletMetrics->GettabletPhaseValue(), 0);
    // // building and dumping segment have been dropped already(drop rt after reopen)
    // ASSERT_EQ(tabletMetrics->GetdumpingSegmentCountValue(), 0);
    // ASSERT_EQ(tabletMetrics->GetpartitionIndexSizeValue(), 2 * 0.5 * buildingSegmentMemsize); // 2 inc + 0 rt
    // ASSERT_EQ(tabletMetrics->GetpartitionReaderVersionCountValue(), 5);
    // ASSERT_EQ(tabletMetrics->GetlatestReaderVersionIdValue(), 0);
    // ASSERT_EQ(tabletMetrics->GetoldestReaderVersionIdValue(), INVALID_VERSIONID);
    // ASSERT_EQ(tabletMetrics->GetbuildingSegmentMemoryUseValue(), 0); // building has been dropped already
    // ASSERT_EQ(tabletMetrics->GetdumpingSegmentMemoryUseValue(), 0);  // dumping has been dropped already
    // ASSERT_EQ(tabletMetrics->GetbuiltRtIndexMemoryUseValue(), 0);
    // ASSERT_EQ(tabletMetrics->GetpartitionMemoryUseValue(), 2 * 0.5 * buildingSegmentMemsize);
    // ASSERT_EQ(tabletMetrics->GetincIndexMemoryUseValue(), 2 * 0.5 * buildingSegmentMemsize);
    // ASSERT_EQ(tabletMetrics->GetrtIndexMemoryUseValue(), 0);
    // // 4MB used by 1building + 2dumping + 2inc. another 4MB used by lfs when load incBuiltSegment(segment info)
    // ASSERT_EQ(tabletMetrics->GetpartitionMemoryQuotaUseValue(), 8 * buildingSegmentMemsize);
    // ASSERT_EQ(tabletMetrics->GettotalMemoryQuotaLimitValue(), _memoryQuota);
    // ASSERT_EQ(tabletMetrics->GetfreeMemoryQuotaValue(), 0);
    // ASSERT_GE(tabletMetrics->GetincVersionFreshnessValue(), latestVersionTs);
    // // 0 docs, for segmentMeta has changed to construct default
    // ASSERT_EQ(tabletMetrics->GetpartitionDocCountValue(), 40);
    // ASSERT_EQ(tabletMetrics->GetsegmentCountValue(), 2);
    // ASSERT_EQ(tabletMetrics->GetincSegmentCountValue(), 2);
    // // double
    // ASSERT_EQ(tabletMetrics->GetfreeMemoryQuotaRatioValue(), 0);
    // ASSERT_EQ(tabletMetrics->GetpartitionMemoryQuotaUseRatioValue(), 1);
    // ASSERT_EQ(tabletMetrics->GetrtIndexMemoryUseRatioValue(), 0);
}

} // namespace indexlibv2::framework
