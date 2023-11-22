#include "indexlib/framework/Tablet.h"

#include <limits>

#include "autil/LambdaWorkItem.h"
#include "autil/ThreadPool.h"
#include "future_lite/coro/Lazy.h"
#include "future_lite/executors/SimpleExecutor.h"
#include "indexlib/base/MemoryQuotaController.h"
#include "indexlib/base/MemoryQuotaSynchronizer.h"
#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/OnlineConfig.h"
#include "indexlib/config/SortDescription.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/LogicalFileSystem.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/load_config/CacheLoadStrategy.h"
#include "indexlib/framework/BuildResource.h"
#include "indexlib/framework/DefaultMemoryControlStrategy.h"
#include "indexlib/framework/DiskSegment.h"
#include "indexlib/framework/LSMMemTableControlStrategy.h"
#include "indexlib/framework/ResourceMap.h"
#include "indexlib/framework/TabletCenter.h"
#include "indexlib/framework/TabletLoader.h"
#include "indexlib/framework/Version.h"
#include "indexlib/framework/VersionLoader.h"
#include "indexlib/framework/cleaner/OnDiskIndexCleaner.h"
#include "indexlib/framework/mock/MockBuildOptionConfig.h"
#include "indexlib/framework/mock/MockDiskSegment.h"
#include "indexlib/framework/mock/MockDocumentBatch.h"
#include "indexlib/framework/mock/MockMemSegment.h"
#include "indexlib/framework/mock/MockSegmentDumpItem.h"
#include "indexlib/framework/mock/MockTablet.h"
#include "indexlib/framework/mock/MockTabletFactory.h"
#include "indexlib/framework/mock/MockTabletLoader.h"
#include "indexlib/framework/mock/MockTabletOptions.h"
#include "indexlib/framework/mock/MockTabletReader.h"
#include "indexlib/framework/mock/MockTabletSchema.h"
#include "indexlib/framework/mock/MockTabletWriter.h"
#include "indexlib/framework/test/FakeTablet.h"
#include "indexlib/framework/test/TabletTestAgent.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "unittest/unittest.h"

using testing::AtLeast;
using testing::ByMove;
using testing::ReturnRef;

namespace indexlibv2::framework {

class TabletTest : public TESTBASE
{
public:
    TabletTest() = default;
    ~TabletTest() = default;

    void setUp() override
    {
        _executor.reset(new future_lite::executors::SimpleExecutor(5));
        _taskScheduler.reset(new future_lite::TaskScheduler(_executor.get()));

        _resource.dumpExecutor = _executor.get();
        _resource.taskScheduler = _taskScheduler.get();
        _resource.metricsReporter = std::make_shared<kmonitor::MetricsReporter>("", kmonitor::MetricsTags(), "");
        _resource.tabletId = indexlib::framework::TabletId("TestTablet");

        _options = std::make_unique<MockTabletOptions>();
        _options->SetIsLeader(false);
        _options->SetFlushLocal(true);
        _options->SetFlushRemote(false);
        _options->TEST_GetOnlineConfig().TEST_SetMaxRealtimeMemoryUse(INT64_MAX);
        _options->TEST_GetOnlineConfig().TEST_SetPrintMetricsInterval(INT64_MAX);
        _options->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetBuildingMemoryLimit(INT64_MAX);
        _options->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetEnablePackageFile(false);
        _options->TEST_GetOnlineConfig().TEST_SetMaxRealtimeDumpIntervalSecond(-1);

        std::string localRoot = GET_TEMP_DATA_PATH();
        _indexRoot = framework::IndexRoot(localRoot, localRoot);
    }
    void tearDown() override {}
    void SetProgress(Locator& locator, std::vector<base::Progress> progress) { locator.SetMultiProgress({progress}); }
    std::vector<base::Progress> GetProgress(std::vector<std::tuple<uint32_t, uint32_t, int64_t>> params)
    {
        std::vector<base::Progress> progress;
        for (auto [from, to, ts] : params) {
            base::Progress::Offset offset = {ts, 0};
            progress.emplace_back(from, to, offset);
        }
        return progress;
    }
    void SetProgress(Locator& locator, std::vector<std::tuple<uint32_t, uint32_t, int64_t>> params)
    {
        locator.SetMultiProgress({GetProgress(params)});
    }

private:
    void InnerTestFilterDoc(const Locator& latestLocator, const Locator& docLocator, bool allowRollback,
                            bool expectedBuild);

private:
    std::unique_ptr<future_lite::Executor> _executor;
    std::unique_ptr<future_lite::TaskScheduler> _taskScheduler;

    indexlibv2::framework::TabletResource _resource;
    std::unique_ptr<MockTabletOptions> _options;
    framework::IndexRoot _indexRoot;
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.framework, TabletTest);

TEST_F(TabletTest, testBlockCacheSupport)
{
    indexlib::file_system::LoadConfigList loadConfigList = indexlib::file_system::LoadConfigList();
    indexlib::file_system::LoadConfig loadConfig = indexlib::file_system::LoadConfig();
    std::shared_ptr<indexlib::file_system::LoadStrategy> loadStategy =
        std::make_shared<indexlib::file_system::CacheLoadStrategy>();
    loadConfig.SetLoadStrategyPtr(loadStategy);
    loadConfigList.PushBack(loadConfig);
    _options->TEST_GetOnlineConfig().MutableLoadConfigList() = loadConfigList;

    auto tablet = std::make_unique<FakeTablet>(_resource);
    tablet->_tabletOptions = std::move(_options);
    auto tabletInfos = tablet->GetTabletInfos();
    ASSERT_TRUE(tabletInfos);
    ASSERT_EQ(tabletInfos->GetTabletName(), _resource.tabletId.GenerateTabletName());
    auto status = tablet->InitIndexDirectory(_indexRoot);
    ASSERT_TRUE(status.IsOK());
    auto fileSystem = framework::TabletTestAgent(tablet.get()).TEST_GetFileSystem();
    assert(fileSystem);
    auto fsOptions = fileSystem->GetFileSystemOptions();
    ASSERT_TRUE(fsOptions.loadConfigList.Size() == loadConfigList.Size());
}

TEST_F(TabletTest, testBuildAndCommit)
{
    std::string emptyName;
    auto schema = std::make_shared<MockTabletSchema>();
    EXPECT_CALL(*schema, GetSchemaNameImpl()).Times(3).WillRepeatedly(ReturnRef(emptyName));
    EXPECT_CALL(*schema, SerializeImpl(_)).WillOnce(Return(true));

    framework::Version version;
    version.SetSchemaId(0);
    version.SetReadSchemaId(0);

    auto tablet = std::make_unique<FakeTablet>(_resource);
    tablet->SetSchema(schema);
    tablet->_tabletOptions = std::move(_options);
    auto tabletInfos = tablet->GetTabletInfos();
    ASSERT_TRUE(tabletInfos);
    ASSERT_EQ(tabletInfos->GetTabletName(), _resource.tabletId.GenerateTabletName());
    auto status = tablet->InitIndexDirectory(_indexRoot);
    ASSERT_TRUE(status.IsOK());
    auto fs = framework::TabletTestAgent(tablet.get()).TEST_GetFileSystem();

    // TabletLoader
    auto tabletLoader = std::make_unique<MockTabletLoader>();
    EXPECT_CALL(*tabletLoader, DoPreLoad(_, _, _)).WillOnce(Return(Status::OK()));
    auto tabletData = std::make_unique<TabletData>("demo");
    auto resourceMap = std::make_shared<ResourceMap>();
    ASSERT_TRUE(tabletData->Init(version, {}, resourceMap).IsOK());
    EXPECT_CALL(*tabletLoader, FinalLoad(_))
        .WillOnce(Return(ByMove(std::pair<Status, std::unique_ptr<TabletData>>(Status::OK(), tabletData.release()))));
    EXPECT_CALL(*tabletLoader, EstimateMemUsed(_, _)).WillOnce(Return(std::make_pair(Status::OK(), 0)));

    // MemSegment
    std::vector<std::shared_ptr<SegmentDumpItem>> segmentDumpItems;
    auto dumpItem = std::make_shared<MockSegmentDumpItem>();
    EXPECT_CALL(*dumpItem, Dump()).WillOnce(Return(Status::OK()));
    segmentDumpItems.push_back(dumpItem);
    auto memSegment = std::make_unique<MockMemSegment>(536870912, fs, Segment::SegmentStatus::ST_BUILDING);
    EXPECT_CALL(*memSegment, Open(_, _)).WillOnce(Return(Status::OK()));
    auto memSegment2 = std::make_unique<MockMemSegment>(536870913, fs, Segment::SegmentStatus::ST_BUILDING);
    EXPECT_CALL(*memSegment2, Open(_, _)).WillOnce(Return(Status::OK()));
    auto memSegmentPtr = memSegment.get();
    // TabletWriter
    auto tabletWriter = std::make_unique<MockTabletWriter>();
    EXPECT_CALL(*tabletWriter, Open(_, _, _)).WillOnce(Return(Status::OK()));
    EXPECT_CALL(*tabletWriter, Build(_)).Times(2).WillOnce(Return(Status::OK())).WillOnce(Return(Status::NeedDump()));
    auto segment = std::make_shared<MockMemSegment>(536870912, fs, Segment::SegmentStatus::ST_DUMPING);
    EXPECT_CALL(*segment, CreateSegmentDumpItems()).WillOnce(Return(make_pair(Status::OK(), segmentDumpItems)));
    EXPECT_CALL(*segment, EndDump()).WillOnce(Return());
    EXPECT_CALL(*tabletWriter, CreateSegmentDumper())
        .WillOnce(DoAll([memSegmentPtr]() { memSegmentPtr->SetSegmentStatus(Segment::SegmentStatus::ST_DUMPING); },
                        Return(ByMove(std::make_unique<SegmentDumper>("", segment, 0, nullptr)))));
    EXPECT_CALL(*tabletWriter, GetTotalMemSize()).Times(AtLeast(1)).WillRepeatedly(Return(0));
    EXPECT_CALL(*tabletWriter, ReportMetrics()).Times(AtLeast(1)).WillRepeatedly(Return());
    EXPECT_CALL(*tabletWriter, GetBuildingSegmentDumpExpandSize()).Times(AtLeast(1)).WillRepeatedly(Return(0));

    auto tabletWriter2 = std::make_unique<MockTabletWriter>();
    EXPECT_CALL(*tabletWriter2, Open(_, _, _)).WillOnce(Return(Status::OK()));
    EXPECT_CALL(*tabletWriter2, Build(_)).WillOnce(Return(Status::OK()));
    EXPECT_CALL(*tabletWriter2, GetTotalMemSize()).Times(AtLeast(1)).WillRepeatedly(Return(0));
    EXPECT_CALL(*tabletWriter2, ReportMetrics()).Times(AtLeast(1)).WillRepeatedly(Return());
    auto segment2 = std::make_shared<MockMemSegment>(536870913, fs, Segment::SegmentStatus::ST_DUMPING);

    // TabletReader
    auto tabletReader = std::make_unique<MockTabletReader>();
    EXPECT_CALL(*tabletReader, DoOpen(_, _)).WillOnce(Return(Status::OK()));
    auto tabletReader2 = std::make_unique<MockTabletReader>();
    EXPECT_CALL(*tabletReader2, DoOpen(_, _)).WillOnce(Return(Status::OK()));
    auto tabletReader3 = std::make_unique<MockTabletReader>();
    EXPECT_CALL(*tabletReader3, DoOpen(_, _)).WillOnce(Return(Status::OK()));

    // TabletFactory
    factory = new MockTabletFactory();
    EXPECT_CALL(*factory, CreateTabletReader(_))
        .Times(3)
        .WillOnce(Return(ByMove(std::move(tabletReader))))
        .WillOnce(Return(ByMove(std::move(tabletReader2))))
        .WillOnce(Return(ByMove(std::move(tabletReader3))));
    EXPECT_CALL(*factory, CreateTabletLoader(_)).Times(1).WillOnce(Return(ByMove(std::move(tabletLoader))));
    EXPECT_CALL(*factory, CreateMemSegment(_))
        .Times(2)
        .WillOnce(Return(ByMove(std::move(memSegment))))
        .WillOnce(Return(ByMove(std::move(memSegment2))));
    EXPECT_CALL(*factory, CreateTabletWriter(_))
        .Times(2)
        .WillOnce(Return(ByMove(std::move(tabletWriter))))
        .WillOnce(Return(ByMove(std::move(tabletWriter2))));

    Status st = tablet->Open(_indexRoot, schema, std::move(tablet->_tabletOptions), version.GetVersionId());
    ASSERT_TRUE(st.IsOK());
    framework::TabletTestAgent(tablet.get()).TEST_GetTaskScheduler()->CleanTasks();
    ASSERT_EQ(tabletInfos->GetLoadedPublishVersion().GetVersionId(), version.GetVersionId());
    ASSERT_EQ(tabletInfos->GetIndexRoot(), _indexRoot);

    // Not need dump.
    Locator locator;
    auto docBatch = std::make_shared<MockDocumentBatch>();
    status = tablet->Build(docBatch);
    ASSERT_TRUE(status.IsOK());
    ASSERT_FALSE(tablet->NeedCommit());

    // Need dump.
    auto docBatch1 = std::make_shared<MockDocumentBatch>();
    status = tablet->Build(docBatch1);
    ASSERT_TRUE(status.IsOK()) << status.ToString();

    ASSERT_TRUE(framework::TabletTestAgent(tablet.get()).TEST_TriggerDump().IsOK());
    ASSERT_TRUE(tablet->NeedCommit());
    auto ret = tablet->Commit(CommitOptions().SetNeedPublish(true));
    ASSERT_TRUE(ret.first.IsOK());
    framework::Version newVersion = framework::TabletTestAgent(tablet.get()).TEST_GetVersion(ret.second);
    ASSERT_TRUE(newVersion.IsValid());

    ASSERT_EQ(newVersion.GetTimestamp(), -1);
    ASSERT_EQ(newVersion.GetLocator().DebugString(), "0:-1:[]");
    ASSERT_EQ(newVersion.GetLastSegmentId(), 536870912);
    ASSERT_EQ(newVersion.GetVersionId(), Version::PRIVATE_VERSION_ID_MASK);
}

TEST_F(TabletTest, testSealSegment)
{
    std::string emptyName;
    auto schema = std::make_shared<MockTabletSchema>();
    EXPECT_CALL(*schema, GetSchemaNameImpl()).Times(3).WillRepeatedly(ReturnRef(emptyName));
    EXPECT_CALL(*schema, SerializeImpl(_)).WillOnce(Return(true));

    framework::Version version;
    version.SetSchemaId(0);
    version.SetReadSchemaId(0);

    auto tablet = std::make_unique<FakeTablet>(_resource);
    tablet->SetSchema(schema);
    auto tabletInfos = tablet->GetTabletInfos();
    ASSERT_TRUE(tabletInfos);
    ASSERT_EQ(tabletInfos->GetTabletName(), _resource.tabletId.GenerateTabletName());
    tablet->_tabletOptions = std::move(_options);
    auto status = tablet->InitIndexDirectory(_indexRoot);
    ASSERT_TRUE(status.IsOK());
    auto fs = framework::TabletTestAgent(tablet.get()).TEST_GetFileSystem();

    // TabletLoader
    auto tabletLoader = std::make_unique<MockTabletLoader>();
    EXPECT_CALL(*tabletLoader, DoPreLoad(_, _, _)).WillOnce(Return(Status::OK()));
    auto tabletData = std::make_unique<TabletData>("demo");
    auto resourceMap = std::make_shared<ResourceMap>();
    ASSERT_TRUE(tabletData->Init(version, {}, resourceMap).IsOK());
    EXPECT_CALL(*tabletLoader, FinalLoad(_))
        .WillOnce(Return(ByMove(std::pair<Status, std::unique_ptr<TabletData>>(Status::OK(), tabletData.release()))));
    EXPECT_CALL(*tabletLoader, EstimateMemUsed(_, _)).WillOnce(Return(std::make_pair(Status::OK(), 0)));

    // MemSegment
    std::vector<std::shared_ptr<SegmentDumpItem>> segmentDumpItems;
    auto dumpItem = std::make_shared<MockSegmentDumpItem>();
    EXPECT_CALL(*dumpItem, Dump()).WillOnce(Return(Status::OK()));
    segmentDumpItems.push_back(dumpItem);
    auto memSegment = std::make_unique<MockMemSegment>(536870912, fs, Segment::SegmentStatus::ST_BUILDING);
    EXPECT_CALL(*memSegment, Open(_, _)).WillOnce(Return(Status::OK()));

    // TabletWriter
    auto tabletWriter = std::make_unique<MockTabletWriter>();
    EXPECT_CALL(*tabletWriter, Open(_, _, _)).WillOnce(Return(Status::OK()));
    EXPECT_CALL(*tabletWriter, Build(_)).Times(1).WillOnce(Return(Status::OK()));
    auto segment = std::make_shared<MockMemSegment>(536870912, fs, Segment::SegmentStatus::ST_BUILDING);
    EXPECT_CALL(*segment, CreateSegmentDumpItems()).WillOnce(Return(make_pair(Status::OK(), segmentDumpItems)));
    EXPECT_CALL(*segment, EvaluateCurrentMemUsed()).WillRepeatedly(Return(0));
    EXPECT_CALL(*segment, EndDump()).WillOnce(Return());
    EXPECT_CALL(*tabletWriter, CreateSegmentDumper())
        .WillOnce(Return(ByMove(std::make_unique<SegmentDumper>("", segment, 0, nullptr))));
    EXPECT_CALL(*tabletWriter, GetTotalMemSize()).WillRepeatedly(Return(0));
    EXPECT_CALL(*tabletWriter, ReportMetrics()).WillRepeatedly(Return());

    // TabletReader
    auto tabletReader = std::make_unique<MockTabletReader>();
    EXPECT_CALL(*tabletReader, DoOpen(_, _)).WillOnce(Return(Status::OK()));
    auto tabletReader2 = std::make_unique<MockTabletReader>();
    EXPECT_CALL(*tabletReader2, DoOpen(_, _)).WillOnce(Return(Status::OK()));

    // TabletFactory
    factory = new MockTabletFactory();
    EXPECT_CALL(*factory, CreateTabletReader(_))
        .Times(2)
        .WillOnce(Return(ByMove(std::move(tabletReader))))
        .WillOnce(Return(ByMove(std::move(tabletReader2))));
    EXPECT_CALL(*factory, CreateTabletLoader(_)).Times(1).WillOnce(Return(ByMove(std::move(tabletLoader))));
    EXPECT_CALL(*factory, CreateMemSegment(_)).Times(1).WillOnce(Return(ByMove(std::move(memSegment))));
    EXPECT_CALL(*factory, CreateTabletWriter(_)).Times(1).WillOnce(Return(ByMove(std::move(tabletWriter))));

    Status st = tablet->Open(_indexRoot, schema, std::move(tablet->_tabletOptions), version.GetVersionId());
    ASSERT_TRUE(st.IsOK());
    ASSERT_EQ(tabletInfos->GetLoadedPublishVersion().GetVersionId(), version.GetVersionId());
    ASSERT_EQ(tabletInfos->GetIndexRoot(), _indexRoot);

    // Seal on empty data
    ASSERT_TRUE(tablet->Flush().IsOK());
    ASSERT_FALSE(tablet->NeedCommit());

    // Not need dump.
    Locator locator;
    auto docBatch = std::make_shared<MockDocumentBatch>();
    status = tablet->Build(docBatch);
    ASSERT_TRUE(status.IsOK());
    ASSERT_FALSE(tablet->NeedCommit());

    // Seal segment
    ASSERT_TRUE(tablet->Flush().IsOK());
    ASSERT_TRUE(framework::TabletTestAgent(tablet.get()).TEST_TriggerDump().IsOK());
    ASSERT_TRUE(tablet->NeedCommit());

    ASSERT_TRUE(tablet->Flush().IsOK()); // no effect
    auto ret = tablet->Commit(CommitOptions().SetNeedPublish(true));
    ASSERT_TRUE(ret.first.IsOK());
    auto newVersion = framework::TabletTestAgent(tablet.get()).TEST_GetVersion(ret.second);
    ASSERT_TRUE(newVersion.IsValid());

    ASSERT_EQ(newVersion.GetTimestamp(), -1);
    ASSERT_EQ(newVersion.GetLocator().DebugString(), "0:-1:[]");
    ASSERT_EQ(newVersion.GetLastSegmentId(), 536870912);
    ASSERT_EQ(newVersion.GetVersionId(), Version::PRIVATE_VERSION_ID_MASK);

    ASSERT_TRUE(tablet->Flush().IsOK()); // seal on empty building data
}

TEST_F(TabletTest, testMemoryController4TabletLoader4NoMem)
{
    const size_t tabletTotalQuota = 8 * 1024 * 1024;
    auto executor = std::make_unique<future_lite::executors::SimpleExecutor>(2);
    _resource.dumpExecutor = executor.get();
    _resource.memoryQuotaController = std::make_shared<MemoryQuotaController>("ut", tabletTotalQuota);

    std::string emptyName;
    auto schema = std::make_shared<MockTabletSchema>();
    EXPECT_CALL(*schema, GetSchemaNameImpl()).Times(3).WillRepeatedly(ReturnRef(emptyName));
    EXPECT_CALL(*schema, SerializeImpl(_)).WillOnce(Return(true));

    framework::Version version;

    auto tablet = std::make_unique<FakeTablet>(_resource);
    tablet->SetSchema(schema);
    auto tabletInfos = tablet->GetTabletInfos();
    ASSERT_TRUE(tabletInfos);
    ASSERT_EQ(tabletInfos->GetTabletName(), _resource.tabletId.GenerateTabletName());
    tablet->_tabletOptions = std::move(_options);
    auto status = tablet->InitIndexDirectory(_indexRoot);
    ASSERT_TRUE(status.IsOK());

    auto tabletData = std::make_unique<TabletData>("demo");
    // TabletLoader
    auto tabletLoader = std::make_unique<MockTabletLoader>();
    EXPECT_CALL(*tabletLoader, DoPreLoad(_, _, _)).WillRepeatedly(Return(Status::OK()));
    EXPECT_CALL(*tabletLoader, EstimateMemUsed(_, _))
        .WillRepeatedly(Return(std::make_pair(Status::OK(), 10 * 1024 * 1024)));
    // TabletFactory
    factory = new MockTabletFactory();
    EXPECT_CALL(*factory, CreateTabletLoader(_)).Times(1).WillOnce(Return(ByMove(std::move(tabletLoader))));

    status = tablet->Open(_indexRoot, schema, std::move(tablet->_tabletOptions), version.GetVersionId());
    ASSERT_TRUE(status.IsNoMem());
}

TEST_F(TabletTest, testMemoryController4TabletLoader4Success)
{
    auto executor = std::make_unique<future_lite::executors::SimpleExecutor>(2);
    const size_t tabletTotalQuota = 8 * 1024 * 1024;
    _resource.dumpExecutor = executor.get();
    _resource.memoryQuotaController = std::make_shared<MemoryQuotaController>("ut", tabletTotalQuota);
    std::string emptyName;
    auto schema = std::make_shared<MockTabletSchema>();
    EXPECT_CALL(*schema, GetSchemaNameImpl()).Times(3).WillRepeatedly(ReturnRef(emptyName));
    EXPECT_CALL(*schema, SerializeImpl(_)).WillOnce(Return(true));

    auto tablet = std::make_unique<FakeTablet>(_resource);
    tablet->SetSchema(schema);
    auto tabletInfos = tablet->GetTabletInfos();
    ASSERT_TRUE(tabletInfos);
    ASSERT_EQ(tabletInfos->GetTabletName(), _resource.tabletId.GenerateTabletName());
    tablet->_tabletOptions = std::move(_options);
    auto status = tablet->InitIndexDirectory(_indexRoot);
    ASSERT_TRUE(status.IsOK());

    auto tabletData = std::make_unique<TabletData>("demo");
    tabletData->_resourceMap = std::make_shared<ResourceMap>();
    // TabletLoader
    auto tabletLoader = std::make_unique<MockTabletLoader>();
    EXPECT_CALL(*tabletLoader, DoPreLoad(_, _, _)).WillRepeatedly(Return(Status::OK()));
    EXPECT_CALL(*tabletLoader, EstimateMemUsed(_, _))
        .WillRepeatedly(Return(std::make_pair(Status::OK(), tabletTotalQuota)));
    EXPECT_CALL(*tabletLoader, FinalLoad(_))
        .WillOnce(Return(ByMove(std::pair<Status, std::unique_ptr<TabletData>>(Status::OK(), tabletData.release()))));

    // TabletFactory
    factory = new MockTabletFactory();
    EXPECT_CALL(*factory, CreateTabletLoader(_)).Times(1).WillOnce(Return(ByMove(std::move(tabletLoader))));

    auto tabletReader = std::make_unique<MockTabletReader>();
    // auto tabletWriter = std::make_unique<MockTabletWriter>();
    // EXPECT_CALL(*tabletWriter, Open(_, _,_)).WillOnce(Return(Status::OK()));
    EXPECT_CALL(*factory, CreateTabletReader(_)).Times(1).WillOnce(Return(ByMove(std::move(tabletReader))));
    // EXPECT_CALL(*factory, CreateTabletWriter()).Times(1).WillOnce(Return(ByMove(std::move(tabletWriter))));

    framework::Version version;
    status = tablet->Open(_indexRoot, schema, std::move(tablet->_tabletOptions), version.GetVersionId());
    ASSERT_TRUE(status.IsOK());
}

TEST_F(TabletTest, testCheckMemoryStatus4MaxRt4SingleTablet)
{
    _resource.memoryQuotaController = std::make_shared<MemoryQuotaController>("ut", 8 * 1024 * 1024);
    _resource.buildMemoryQuotaController = std::make_shared<MemoryQuotaController>("ut", 8 * 1024 * 1024);

    std::string emptyName;
    auto schema = std::make_shared<MockTabletSchema>();
    EXPECT_CALL(*schema, GetSchemaNameImpl()).Times(3).WillRepeatedly(ReturnRef(emptyName));
    Version emptyVersion;

    auto tablet = std::make_unique<FakeTablet>(_resource);
    tablet->SetSchema(schema);

    // just construt a available tablet, not complete
    // not greater than the quota of buildMemoryQuotaController, make sure run the logic of single-tablet
    _options->SetIsLeader(true);
    _options->TEST_GetOnlineConfig().TEST_SetMaxRealtimeMemoryUse(8 * 1024 * 1024);

    factory = new MockTabletFactory();
    ASSERT_TRUE(tablet->Open(_indexRoot, schema, std::move(_options), emptyVersion.GetVersionId()).IsInvalidArgs());
    ASSERT_TRUE(tablet->InitIndexDirectory(_indexRoot).IsOK());
    factory = new MockTabletFactory();
    tablet->_tabletFactory = tablet->CreateTabletFactory(schema->GetTableType(), tablet->_tabletOptions);
    ASSERT_TRUE(tablet->PrepareResource().IsOK());
    auto tabletMetrics = framework::TabletTestAgent(tablet.get()).TEST_GetTabletMetrics();
    ASSERT_EQ(MemoryStatus::OK, tablet->CheckMemoryStatus());

    auto tabletData = std::make_shared<TabletData>("");
    auto tabletReaderContainer = std::make_shared<TabletReaderContainer>("ut");

    std::vector<std::shared_ptr<Segment>> segments;
    const size_t dumpingSegmentCnt = 8;
    const int64_t buildingSegmentMemsize = 1024 * 1024;
    segmentid_t segmentId = Segment::PUBLIC_SEGMENT_ID_MASK;
    for (size_t i = 0; i < dumpingSegmentCnt; ++i) {
        SegmentMeta segMeta;
        segMeta.segmentId = segmentId++;
        auto dumpingSegment = std::make_shared<MockMemSegment>(segMeta, Segment::SegmentStatus::ST_DUMPING);
        EXPECT_CALL(*dumpingSegment, EvaluateCurrentMemUsed()).WillRepeatedly(Return(buildingSegmentMemsize));

        segments.emplace_back(dumpingSegment);
        auto tabletData = std::make_shared<TabletData>(/*empty name*/ "");
        ASSERT_TRUE(tabletData->Init(emptyVersion, segments, std::make_shared<ResourceMap>()).IsOK());
        tabletReaderContainer->AddTabletReader(tabletData, /*empty reader*/ nullptr,
                                               /*version deploy description*/ nullptr);
        tabletMetrics->UpdateMetrics(tabletReaderContainer, tabletData, /*null writer*/ nullptr, /*maxRtMemsize*/ 0,
                                     /*fileSystem*/ nullptr);
        if (i < dumpingSegmentCnt - 1) {
            ASSERT_EQ(MemoryStatus::OK, tablet->CheckMemoryStatus());
        } else {
            EXPECT_EQ(MemoryStatus::REACH_MAX_RT_INDEX_SIZE, tablet->CheckMemoryStatus());
        }
    }

    for (size_t i = 0; i < dumpingSegmentCnt; ++i) {
        tabletReaderContainer->EvictOldestTabletReader();
        tabletMetrics->UpdateMetrics(tabletReaderContainer, tabletData, /*null writer*/ nullptr, /*maxRtMemsize*/ 0,
                                     /*fileSystem*/ nullptr);
        if (i < dumpingSegmentCnt - 1) {
            ASSERT_EQ(MemoryStatus::REACH_MAX_RT_INDEX_SIZE, tablet->CheckMemoryStatus());
        } else {
            ASSERT_EQ(MemoryStatus::OK, tablet->CheckMemoryStatus());
        }
    }
    tabletReaderContainer->EvictOldestTabletReader();
    tabletMetrics->UpdateMetrics(tabletReaderContainer, tabletData, /*null writer*/ nullptr, /*maxRtMemsize*/ 0,
                                 /*fileSystem*/ nullptr);
    EXPECT_EQ(MemoryStatus::OK, tablet->CheckMemoryStatus());

    ASSERT_TRUE(tabletData->Init(emptyVersion, segments, std::make_shared<ResourceMap>()).IsOK());
    tabletReaderContainer->AddTabletReader(tabletData, /*empty reader*/ nullptr,
                                           /*version deploy description*/ nullptr);
    tabletMetrics->UpdateMetrics(tabletReaderContainer, tabletData, /*null writer*/ nullptr, /*maxRtMemsize*/ 0,
                                 /*fileSystem*/ nullptr);
    EXPECT_EQ(MemoryStatus::REACH_MAX_RT_INDEX_SIZE, tablet->CheckMemoryStatus());
}

TEST_F(TabletTest, testTabletCenter)
{
    TabletCenter::GetInstance()->Activate();
    auto createFakeTablet = [&](const indexlib::framework::TabletId& tabletId) {
        auto executor = std::make_unique<future_lite::executors::SimpleExecutor>(2);
        _resource.tabletId = tabletId;
        const size_t tabletTotalQuota = 8 * 1024 * 1024;
        _resource.memoryQuotaController = std::make_shared<MemoryQuotaController>("ut", tabletTotalQuota);
        _resource.dumpExecutor = executor.get();
        auto tablet = std::make_shared<FakeTablet>(_resource);
        static std::string emptyName;
        auto schema = std::make_shared<MockTabletSchema>();
        EXPECT_CALL(*schema, GetSchemaNameImpl()).WillRepeatedly(ReturnRef(emptyName));
        EXPECT_CALL(*schema, SerializeImpl(_)).WillRepeatedly(Return(true));

        tablet->SetSchema(schema);
        if (TabletCenter::GetInstance()->RegisterTablet(tablet) != nullptr) {
            tablet->SetRunAfterCloseFunction(
                [ptr = tablet.get()]() { TabletCenter::GetInstance()->UnregisterTablet(ptr); });
        }

        return tablet;
    };

    const size_t TEST_NUM = 3;
    std::vector<std::shared_ptr<ITablet>> tablets;
    for (size_t i = 0; i < TEST_NUM; i++) {
        indexlib::framework::TabletId tid("TestTabletCenter");
        tid.SetRange(i, i);
        tablets.push_back(createFakeTablet(tid));
    }

    std::vector<TabletCenter::TabletResource> result;
    TabletCenter::GetInstance()->GetAllTablets(result);
    ASSERT_EQ(TEST_NUM, result.size());

    for (size_t i = 0; i < TEST_NUM; i++) {
        indexlib::framework::TabletId tid("TestTabletCenter");
        tid.SetRange(i, i);
        std::string tabletName = tid.GenerateTabletName();
        TabletCenter::GetInstance()->GetTabletByTabletName(tabletName, result);
        ASSERT_EQ(1, result.size());
        ASSERT_EQ(tabletName, result[0].tabletPtr->GetTabletInfos()->GetTabletName());
        tablets[i]->Close();

        sleep(2);
        TabletCenter::GetInstance()->GetTabletByTabletName(tabletName, result);
        ASSERT_TRUE(result.empty());
    }
    TabletCenter::GetInstance()->GetAllTablets(result);
    ASSERT_TRUE(result.empty());

    // clear orphan tablets
    tablets.clear();
    for (size_t i = 0; i < TEST_NUM; i++) {
        indexlib::framework::TabletId tid("TestTabletCenter");
        tid.SetRange(i, i);
        tablets.push_back(createFakeTablet(tid));
    }

    TabletCenter::GetInstance()->GetAllTablets(result);
    ASSERT_EQ(TEST_NUM, result.size());
    result.clear();
    tablets.clear();

    sleep(2);
    TabletCenter::GetInstance()->GetAllTablets(result);
    ASSERT_TRUE(result.empty());
    TabletCenter::GetInstance()->Deactivate();
}

TEST_F(TabletTest, testCheckMemoryStatus4MaxRt4MultiTablet)
{
    auto executor = std::make_unique<future_lite::executors::SimpleExecutor>(2);
    _resource.dumpExecutor = executor.get();
    _resource.memoryQuotaController = std::make_shared<MemoryQuotaController>("ut", 8 * 1024 * 1024);
    _resource.buildMemoryQuotaController = std::make_shared<MemoryQuotaController>("ut", 8 * 1024 * 1024);

    std::string emptyName;
    auto schema = std::make_shared<MockTabletSchema>();
    EXPECT_CALL(*schema, GetSchemaNameImpl()).Times(3).WillRepeatedly(ReturnRef(emptyName));
    Version emptyVersion;

    auto tablet = std::make_unique<FakeTablet>(_resource);
    tablet->SetSchema(schema);
    // just construt a available tablet, not complete
    // greater than the quota of buildMemoryQuotaController, make sure run the logic of multi-tablet
    _options->SetIsLeader(true);
    _options->TEST_GetOnlineConfig().TEST_SetMaxRealtimeMemoryUse(16 * 1024 * 1024);

    factory = new MockTabletFactory();
    ASSERT_TRUE(tablet->Open(_indexRoot, schema, std::move(_options), emptyVersion.GetVersionId()).IsInvalidArgs());
    ASSERT_TRUE(tablet->InitIndexDirectory(_indexRoot).IsOK());
    factory = new MockTabletFactory();
    tablet->_tabletFactory = tablet->CreateTabletFactory(schema->GetTableType(), tablet->_tabletOptions);
    ASSERT_TRUE(tablet->PrepareResource().IsOK());
    auto tabletMetrics = framework::TabletTestAgent(tablet.get()).TEST_GetTabletMetrics();
    ASSERT_EQ(MemoryStatus::OK, tablet->CheckMemoryStatus());

    auto tabletData = std::make_shared<TabletData>("");
    auto tabletReaderContainer = std::make_shared<TabletReaderContainer>("ut");

    std::vector<std::shared_ptr<Segment>> segments;
    const size_t dumpingSegmentCnt = 8;
    const int64_t buildingSegmentMemsize = 1024 * 1024;
    segmentid_t segmentId = Segment::PUBLIC_SEGMENT_ID_MASK;
    for (size_t i = 0; i < dumpingSegmentCnt; ++i) {
        SegmentMeta segMeta;
        segMeta.segmentId = segmentId++;
        auto dumpingSegment = std::make_shared<MockMemSegment>(segMeta, Segment::SegmentStatus::ST_DUMPING);
        EXPECT_CALL(*dumpingSegment, EvaluateCurrentMemUsed()).WillRepeatedly(Return(buildingSegmentMemsize));

        segments.emplace_back(dumpingSegment);
        auto tabletData = std::make_shared<TabletData>(/*empty name*/ "");
        ASSERT_TRUE(tabletData->Init(emptyVersion, segments, std::make_shared<ResourceMap>()).IsOK());
        tabletReaderContainer->AddTabletReader(tabletData, /*empty reader*/ nullptr,
                                               /*version deploy description*/ nullptr);
        tabletMetrics->UpdateMetrics(tabletReaderContainer, tabletData, /*null writer*/ nullptr, /*maxRtMemsize*/ 0,
                                     /*fileSystem*/ nullptr);
        framework::TabletTestAgent(tablet.get())
            .TEST_GetBuildMemoryQuotaSynchronizer()
            ->SyncMemoryQuota(tabletMetrics->GetRtIndexMemSize());
        if (i < dumpingSegmentCnt / 2) {
            ASSERT_EQ(MemoryStatus::OK, tablet->CheckMemoryStatus());
        } else {
            EXPECT_EQ(MemoryStatus::REACH_MAX_RT_INDEX_SIZE, tablet->CheckMemoryStatus());
        }
    }

    for (size_t i = 0; i < dumpingSegmentCnt - 1; ++i) {
        tabletReaderContainer->EvictOldestTabletReader();
        tabletMetrics->UpdateMetrics(tabletReaderContainer, tabletData, /*null writer*/ nullptr, /*maxRtMemsize*/ 0,
                                     /*fileSystem*/ nullptr);
        framework::TabletTestAgent(tablet.get())
            .TEST_GetBuildMemoryQuotaSynchronizer()
            ->SyncMemoryQuota(tabletMetrics->GetRtIndexMemSize());
        ASSERT_EQ(MemoryStatus::REACH_MAX_RT_INDEX_SIZE, tablet->CheckMemoryStatus());
    }
    tabletReaderContainer->EvictOldestTabletReader();
    tabletMetrics->UpdateMetrics(tabletReaderContainer, tabletData, /*null writer*/ nullptr, /*maxRtMemsize*/ 0,
                                 /*fileSystem*/ nullptr);
    framework::TabletTestAgent(tablet.get())
        .TEST_GetBuildMemoryQuotaSynchronizer()
        ->SyncMemoryQuota(tabletMetrics->GetRtIndexMemSize());
    EXPECT_EQ(MemoryStatus::OK, tablet->CheckMemoryStatus());

    ASSERT_TRUE(tabletData->Init(emptyVersion, segments, std::make_shared<ResourceMap>()).IsOK());
    tabletReaderContainer->AddTabletReader(tabletData, /*empty reader*/ nullptr,
                                           /*version deploy description*/ nullptr);
    tabletMetrics->UpdateMetrics(tabletReaderContainer, tabletData, /*null writer*/ nullptr, /*maxRtMemsize*/ 0,
                                 /*fileSystem*/ nullptr);
    framework::TabletTestAgent(tablet.get())
        .TEST_GetBuildMemoryQuotaSynchronizer()
        ->SyncMemoryQuota(tabletMetrics->GetRtIndexMemSize());
    EXPECT_EQ(MemoryStatus::REACH_MAX_RT_INDEX_SIZE, tablet->CheckMemoryStatus());
}

TEST_F(TabletTest, testCheckMemoryStatus4TotalMem)
{
    _resource.memoryQuotaController = std::make_shared<MemoryQuotaController>("ut", 18 * 1024 * 1024);
    _resource.buildMemoryQuotaController = std::make_shared<MemoryQuotaController>("ut", 10 * 1024 * 1024);

    std::string emptyName;
    auto schema = std::make_shared<MockTabletSchema>();
    EXPECT_CALL(*schema, GetSchemaNameImpl()).Times(3).WillRepeatedly(ReturnRef(emptyName));
    Version emptyVersion;
    auto tablet = std::make_unique<FakeTablet>(_resource);
    tablet->SetSchema(schema);
    auto tabletDumper = framework::TabletTestAgent(tablet.get()).TEST_GetTabletDumper();
    // just construt a available tablet, not complete
    _options->SetIsLeader(true);
    _options->TEST_GetOnlineConfig().TEST_SetMaxRealtimeMemoryUse(100 * 1024 * 1024);
    factory = new MockTabletFactory();

    ASSERT_TRUE(tablet->Open(_indexRoot, schema, std::move(_options), emptyVersion.GetVersionId()).IsInvalidArgs());
    ASSERT_TRUE(tablet->InitIndexDirectory(_indexRoot).IsOK());
    factory = new MockTabletFactory();
    tablet->_tabletFactory = tablet->CreateTabletFactory(schema->GetTableType(), tablet->_tabletOptions);
    ASSERT_TRUE(tablet->PrepareResource().IsOK());

    auto tabletMetrics = framework::TabletTestAgent(tablet.get()).TEST_GetTabletMetrics();
    ASSERT_EQ(MemoryStatus::OK, tablet->CheckMemoryStatus());

    auto tabletData = std::make_shared<TabletData>("");
    auto tabletReaderContainer = std::make_shared<TabletReaderContainer>("ut");
    tabletMetrics->UpdateMetrics(tabletReaderContainer, tabletData, /*null writer*/ nullptr, /*maxRtMemsize*/ 0,
                                 /*fileSystem*/ nullptr);

    const size_t segmentDumperCnt = 8;
    const int64_t dumpExpandMemsize = 4 * 1024 * 1024;
    const int64_t buildingSegmentMemsize = 1024 * 1024;
    SegmentMeta segMeta;
    segMeta.segmentId = Segment::PUBLIC_SEGMENT_ID_MASK;
    segMeta.segmentInfo->docCount = 2;
    auto dumpingSegment = std::make_shared<MockMemSegment>(segMeta, Segment::SegmentStatus::ST_DUMPING);
    EXPECT_CALL(*dumpingSegment, EvaluateCurrentMemUsed()).WillRepeatedly(Return(buildingSegmentMemsize));
    size_t peakExpandIdx = 5;
    for (size_t i = 0; i < segmentDumperCnt; ++i) {
        int64_t expandMemsize = dumpExpandMemsize;
        if (i == peakExpandIdx) {
            expandMemsize = 5 * dumpExpandMemsize;
        }
        auto segmentDumper =
            std::make_unique<SegmentDumper>("", dumpingSegment, expandMemsize, /*empty metric provider*/ nullptr);
        tabletDumper->PushSegmentDumper(std::move(segmentDumper));
    }
    ASSERT_EQ(MemoryStatus::REACH_TOTAL_MEM_LIMIT, tablet->CheckMemoryStatus());
    for (size_t i = 0; i < segmentDumperCnt; ++i) {
        tabletDumper->PopOne();
        if (i >= peakExpandIdx) {
            EXPECT_EQ(MemoryStatus::OK, tablet->CheckMemoryStatus());
        } else {
            EXPECT_EQ(MemoryStatus::REACH_TOTAL_MEM_LIMIT, tablet->CheckMemoryStatus());
        }
    }
}

TEST_F(TabletTest, testCheckMem)
{
    FakeTablet tablet(_resource);
    framework::TabletTestAgent(&tablet).TEST_GetTabletInfos()->SetMemoryStatus(
        indexlibv2::framework::MemoryStatus::REACH_MAX_RT_INDEX_SIZE);
    tablet._tabletOptions = std::move(_options);
    auto docBatch = std::make_shared<MockDocumentBatch>();
    EXPECT_TRUE(tablet.Build(docBatch).IsNoMem());
}

TEST_F(TabletTest, testCleanIndex)
{
    std::shared_ptr<Tablet> tablet(new Tablet(_resource));
    std::string localRoot = GET_TEMP_DATA_PATH() + "/local/";
    std::string remoteRoot = GET_TEMP_DATA_PATH() + "/remote/";
    std::shared_ptr<TabletReaderContainer> readerContainer(new TabletReaderContainer("tableName"));
    tablet->_tabletInfos.reset(new TabletInfos);
    tablet->_tabletInfos->SetIndexRoot({localRoot, ""});
    auto tabletOptions = std::make_shared<config::TabletOptions>();
    tabletOptions->SetIsOnline(true);
    tabletOptions->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetKeepVersionCount(1);
    tablet->_tabletOptions = tabletOptions;
    tablet->_tabletReaderContainer = readerContainer;
    Version version0(0), version1(1), version2(2);
    std::string versionStr = version0.ToString();
    ASSERT_TRUE(indexlib::file_system::FslibWrapper::AtomicStore(localRoot + "/version.0", versionStr.data(),
                                                                 versionStr.length())
                    .OK());
    versionStr = version1.ToString();
    ASSERT_TRUE(indexlib::file_system::FslibWrapper::AtomicStore(localRoot + "/version.1", versionStr.data(),
                                                                 versionStr.length())
                    .OK());
    versionStr = version2.ToString();
    ASSERT_TRUE(indexlib::file_system::FslibWrapper::AtomicStore(remoteRoot + "/version.2", versionStr.data(),
                                                                 versionStr.length())
                    .OK());
    ASSERT_TRUE(tablet->CleanIndexFiles({1, 2}).IsOK());
    auto existResult = indexlib::file_system::FslibWrapper::IsExist(localRoot + "/version.0");
    ASSERT_TRUE(existResult.OK());
    ASSERT_TRUE(existResult.Value());
    ASSERT_TRUE(indexlib::file_system::FslibWrapper::AtomicStore(localRoot + "/version.2", versionStr.data(),
                                                                 versionStr.length())
                    .OK());
    ASSERT_TRUE(tablet->CleanIndexFiles({1, 2}).IsOK());
    existResult = indexlib::file_system::FslibWrapper::IsExist(localRoot + "/version.0");
    ASSERT_TRUE(existResult.OK());
    ASSERT_FALSE(existResult.Value());
}

TEST_F(TabletTest, testNeedDumpButCreateSegmentDumperFailed)
{
    std::string emptyName;
    auto schema = std::make_shared<MockTabletSchema>();
    EXPECT_CALL(*schema, GetSchemaNameImpl()).Times(3).WillRepeatedly(ReturnRef(emptyName));
    EXPECT_CALL(*schema, SerializeImpl(_)).WillOnce(Return(true));

    framework::Version version;
    version.SetSchemaId(0);
    version.SetReadSchemaId(0);

    auto tablet = std::make_unique<FakeTablet>(_resource);
    tablet->SetSchema(schema);
    auto tabletInfos = tablet->GetTabletInfos();
    ASSERT_TRUE(tabletInfos);
    ASSERT_EQ(tabletInfos->GetTabletName(), _resource.tabletId.GenerateTabletName());
    tablet->_tabletOptions = std::move(_options);
    auto status = tablet->InitIndexDirectory(_indexRoot);
    ASSERT_TRUE(status.IsOK());
    auto fs = framework::TabletTestAgent(tablet.get()).TEST_GetFileSystem();

    // TabletLoader
    auto tabletLoader = std::make_unique<MockTabletLoader>();
    EXPECT_CALL(*tabletLoader, DoPreLoad(_, _, _)).WillOnce(Return(Status::OK()));
    auto tabletData = std::make_unique<TabletData>("demo");
    auto resourceMap = std::make_shared<ResourceMap>();
    ASSERT_TRUE(tabletData->Init(version, {}, resourceMap).IsOK());
    EXPECT_CALL(*tabletLoader, FinalLoad(_))
        .WillOnce(Return(ByMove(std::pair<Status, std::unique_ptr<TabletData>>(Status::OK(), tabletData.release()))));
    EXPECT_CALL(*tabletLoader, EstimateMemUsed(_, _)).WillOnce(Return(std::make_pair(Status::OK(), 0)));

    // MemSegment
    auto memSegment = std::make_unique<MockMemSegment>(536870912, fs, Segment::SegmentStatus::ST_BUILDING);
    EXPECT_CALL(*memSegment, Open(_, _)).WillOnce(Return(Status::OK()));

    // TabletWriter
    auto tabletWriter = std::make_unique<MockTabletWriter>();
    EXPECT_CALL(*tabletWriter, Open(_, _, _)).WillOnce(Return(Status::OK()));
    EXPECT_CALL(*tabletWriter, Build(_)).Times(1).WillOnce(Return(Status::NeedDump()));
    EXPECT_CALL(*tabletWriter, CreateSegmentDumper()).WillOnce(Return(ByMove(std::unique_ptr<SegmentDumper>())));

    // TabletReader
    auto tabletReader = std::make_unique<MockTabletReader>();
    EXPECT_CALL(*tabletReader, DoOpen(_, _)).WillOnce(Return(Status::OK()));
    auto tabletReader2 = std::make_unique<MockTabletReader>();
    EXPECT_CALL(*tabletReader2, DoOpen(_, _)).WillOnce(Return(Status::OK()));

    // TabletFactory
    factory = new MockTabletFactory();
    EXPECT_CALL(*factory, CreateTabletReader(_))
        .Times(2)
        .WillOnce(Return(ByMove(std::move(tabletReader))))
        .WillOnce(Return(ByMove(std::move(tabletReader2))));
    EXPECT_CALL(*factory, CreateTabletLoader(_)).Times(1).WillOnce(Return(ByMove(std::move(tabletLoader))));
    EXPECT_CALL(*factory, CreateMemSegment(_)).Times(1).WillOnce(Return(ByMove(std::move(memSegment))));
    EXPECT_CALL(*factory, CreateTabletWriter(_)).Times(1).WillOnce(Return(ByMove(std::move(tabletWriter))));

    Status st = tablet->Open(_indexRoot, schema, std::move(tablet->_tabletOptions), version.GetVersionId());
    ASSERT_TRUE(st.IsOK());
    ASSERT_EQ(tabletInfos->GetLoadedPublishVersion().GetVersionId(), version.GetVersionId());
    ASSERT_EQ(tabletInfos->GetIndexRoot(), _indexRoot);

    Locator locator;
    auto docBatch = std::make_shared<MockDocumentBatch>();
    st = tablet->Build(docBatch);
    ASSERT_TRUE(st.IsCorruption());
}

TEST_F(TabletTest, TestUpdateControlFlow)
{
    std::string emptyName;
    auto schema = std::make_shared<MockTabletSchema>();
    EXPECT_CALL(*schema, GetSchemaNameImpl()).WillRepeatedly(ReturnRef(emptyName));
    EXPECT_CALL(*schema, SerializeImpl(_)).WillOnce(Return(true));

    framework::Version version;
    version.SetSchemaId(0);
    version.SetReadSchemaId(0);
    auto tablet = std::make_unique<FakeTablet>(_resource);
    tablet->SetSchema(schema);
    tablet->_tabletOptions = std::move(_options);
    auto status = tablet->InitIndexDirectory(_indexRoot);
    ASSERT_TRUE(status.IsOK());

    auto tabletLoader = std::make_unique<MockTabletLoader>();
    EXPECT_CALL(*tabletLoader, DoPreLoad(_, _, _)).WillOnce(Return(Status::OK()));
    auto tabletData = std::make_unique<TabletData>("demo");
    auto resourceMap = std::make_shared<ResourceMap>();
    ASSERT_TRUE(tabletData->Init(version, {}, resourceMap).IsOK());
    EXPECT_CALL(*tabletLoader, FinalLoad(_))
        .WillOnce(Return(ByMove(std::pair<Status, std::unique_ptr<TabletData>>(Status::OK(), tabletData.release()))));
    EXPECT_CALL(*tabletLoader, EstimateMemUsed(_, _)).WillOnce(Return(std::make_pair(Status::OK(), 0)));

    auto fs = framework::TabletTestAgent(tablet.get()).TEST_GetFileSystem();
    auto memSegment = std::make_unique<MockMemSegment>(536870912, fs, Segment::SegmentStatus::ST_BUILDING);
    EXPECT_CALL(*memSegment, Open(_, _)).WillOnce(Return(Status::OK()));

    auto tabletReader = std::make_unique<MockTabletReader>();
    EXPECT_CALL(*tabletReader, DoOpen(_, _)).WillOnce(Return(Status::OK()));
    auto tabletReader2 = std::make_unique<MockTabletReader>();
    EXPECT_CALL(*tabletReader2, DoOpen(_, _)).WillOnce(Return(Status::OK()));

    auto tabletWriter = std::make_unique<MockTabletWriter>();
    factory = new MockTabletFactory();
    EXPECT_CALL(*factory, CreateTabletReader(_))
        .Times(2)
        .WillOnce(Return(ByMove(std::move(tabletReader))))
        .WillOnce(Return(ByMove(std::move(tabletReader2))));
    EXPECT_CALL(*factory, CreateMemSegment(_)).Times(1).WillOnce(Return(ByMove(std::move(memSegment))));
    EXPECT_CALL(*factory, CreateTabletLoader(_)).Times(1).WillOnce(Return(ByMove(std::move(tabletLoader))));

    Status st = tablet->Open(_indexRoot, schema, std::move(tablet->_tabletOptions), version.GetVersionId());
    ASSERT_TRUE(st.IsOK());

    EXPECT_EQ(tablet->_inconsistentModeBuildThreadPool, nullptr);
    EXPECT_EQ(tablet->_consistentModeBuildThreadPool, nullptr);

    indexlibv2::framework::OpenOptions openOptions(indexlibv2::framework::OpenOptions::INCONSISTENT_BATCH,
                                                   /*consistentModeBuildThreadCount=*/4,
                                                   /*inconsistentModeBuildThreadCount=*/10);
    openOptions.SetUpdateControlFlowOnly(true);
    indexlibv2::framework::ReopenOptions reopenOptions(openOptions);
    EXPECT_TRUE(tablet->UpdateControlFlow(reopenOptions).IsOK());
    EXPECT_NE(tablet->_inconsistentModeBuildThreadPool, nullptr);
    EXPECT_NE(tablet->_consistentModeBuildThreadPool, nullptr);

    EXPECT_CALL(*tabletWriter, Open(_, _, _)).WillOnce(Return(Status::OK()));
    EXPECT_CALL(*tabletWriter, Build(_)).Times(1).WillOnce(Return(Status::OK()));
    EXPECT_CALL(*factory, CreateTabletWriter(_)).Times(1).WillOnce(Return(ByMove(std::move(tabletWriter))));

    auto docBatch = std::make_shared<MockDocumentBatch>();
    status = tablet->Build(docBatch);
    EXPECT_TRUE(status.IsOK());
}

TEST_F(TabletTest, TestUpdateControlFlowRaceCondition)
{
    std::string emptyName;
    auto schema = std::make_shared<MockTabletSchema>();
    EXPECT_CALL(*schema, GetSchemaNameImpl()).WillRepeatedly(ReturnRef(emptyName));
    EXPECT_CALL(*schema, SerializeImpl(_)).WillOnce(Return(true));

    framework::Version version;
    version.SetSchemaId(0);
    version.SetReadSchemaId(0);
    auto tablet = std::make_unique<FakeTablet>(_resource);
    tablet->SetSchema(schema);
    tablet->_tabletOptions = std::move(_options);
    auto status = tablet->InitIndexDirectory(_indexRoot);
    ASSERT_TRUE(status.IsOK());

    auto tabletLoader = std::make_unique<MockTabletLoader>();
    EXPECT_CALL(*tabletLoader, DoPreLoad(_, _, _)).WillOnce(Return(Status::OK()));
    auto tabletData = std::make_unique<TabletData>("demo");
    auto resourceMap = std::make_shared<ResourceMap>();
    ASSERT_TRUE(tabletData->Init(version, {}, resourceMap).IsOK());
    EXPECT_CALL(*tabletLoader, FinalLoad(_))
        .WillOnce(Return(ByMove(std::pair<Status, std::unique_ptr<TabletData>>(Status::OK(), tabletData.release()))));
    EXPECT_CALL(*tabletLoader, EstimateMemUsed(_, _)).WillOnce(Return(std::make_pair(Status::OK(), 0)));

    auto fs = framework::TabletTestAgent(tablet.get()).TEST_GetFileSystem();
    auto memSegment = std::make_unique<MockMemSegment>(536870912, fs, Segment::SegmentStatus::ST_BUILDING);
    EXPECT_CALL(*memSegment, Open(_, _)).WillOnce(Return(Status::OK()));

    auto tabletReader = std::make_unique<MockTabletReader>();
    EXPECT_CALL(*tabletReader, DoOpen(_, _)).WillOnce(Return(Status::OK()));
    auto tabletReader2 = std::make_unique<MockTabletReader>();
    EXPECT_CALL(*tabletReader2, DoOpen(_, _)).WillOnce(Return(Status::OK()));

    auto tabletWriter = std::make_unique<MockTabletWriter>();
    auto segment = std::make_shared<MockMemSegment>(536870912, fs, Segment::SegmentStatus::ST_BUILT);
    EXPECT_CALL(*tabletWriter, CreateSegmentDumper())
        .WillOnce(Return(ByMove(std::make_unique<SegmentDumper>("", segment, 0, nullptr))));
    factory = new MockTabletFactory();
    EXPECT_CALL(*factory, CreateTabletReader(_))
        .Times(2)
        .WillOnce(Return(ByMove(std::move(tabletReader))))
        .WillOnce(Return(ByMove(std::move(tabletReader2))));
    EXPECT_CALL(*factory, CreateMemSegment(_)).Times(1).WillOnce(Return(ByMove(std::move(memSegment))));
    EXPECT_CALL(*factory, CreateTabletLoader(_)).Times(1).WillOnce(Return(ByMove(std::move(tabletLoader))));

    Status st = tablet->Open(_indexRoot, schema, std::move(tablet->_tabletOptions), version.GetVersionId());
    ASSERT_TRUE(st.IsOK());

    ON_CALL(*tabletWriter, Open(_, _, _)).WillByDefault(Return(Status::OK()));
    EXPECT_CALL(*tabletWriter, Build(_)).Times(1).WillOnce(Return(Status::OK()));
    EXPECT_CALL(*factory, CreateTabletWriter(_)).Times(1).WillOnce(Return(ByMove(std::move(tabletWriter))));
    auto docBatch = std::make_shared<MockDocumentBatch>();
    status = tablet->Build(docBatch);
    EXPECT_TRUE(status.IsOK());

    indexlibv2::framework::OpenOptions openOptions(indexlibv2::framework::OpenOptions::INCONSISTENT_BATCH,
                                                   /*consistentModeBuildThreadCount=*/0,
                                                   /*inconsistentModeBuildThreadCount=*/0);
    openOptions.SetUpdateControlFlowOnly(true);
    indexlibv2::framework::ReopenOptions reopenOptions(openOptions);

    autil::ThreadPool threadPool(/*threadNum=*/2, /*queueSize=*/100, true);

    auto workItem = new autil::LambdaWorkItem(
        [&]() { EXPECT_TRUE(tablet->Reopen(reopenOptions, VersionCoord(CONTROL_FLOW_VERSIONID)).IsOK()); });
    ASSERT_EQ(autil::ThreadPool::ERROR_NONE, threadPool.pushWorkItem(workItem));
    workItem = new autil::LambdaWorkItem([&]() { EXPECT_TRUE(tablet->Flush().IsOK()); });
    ASSERT_EQ(autil::ThreadPool::ERROR_NONE, threadPool.pushWorkItem(workItem));

    threadPool.start("");
    threadPool.waitFinish();

    EXPECT_EQ(tablet->_inconsistentModeBuildThreadPool, nullptr);
    EXPECT_EQ(tablet->_consistentModeBuildThreadPool, nullptr);
}

void TabletTest::InnerTestFilterDoc(const Locator& latestLocator, const Locator& docLocator, bool allowRollback,
                                    bool expectedBuild)
{
    setUp();
    std::cout << "check begin latest locator: " << latestLocator.DebugString() << " doc locator "
              << docLocator.DebugString() << " but not build" << std::endl;
    _options->TEST_GetOnlineConfig().TEST_SetAllowLocatorRollback(allowRollback);
    std::string emptyName;
    auto schema = std::make_shared<MockTabletSchema>();
    ON_CALL(*schema, GetSchemaNameImpl()).WillByDefault(ReturnRef(emptyName));
    ON_CALL(*schema, SerializeImpl(_)).WillByDefault(Return(true));

    framework::Version version;
    version.SetSchemaId(0);
    version.SetReadSchemaId(0);

    auto tablet = std::make_unique<FakeTablet>(_resource);
    tablet->SetSchema(schema);
    tablet->_tabletOptions = std::move(_options);
    auto status = tablet->InitIndexDirectory(_indexRoot);
    ASSERT_TRUE(status.IsOK());
    auto fs = framework::TabletTestAgent(tablet.get()).TEST_GetFileSystem();

    // TabletLoader
    auto tabletLoader = std::make_unique<MockTabletLoader>();
    ON_CALL(*tabletLoader, DoPreLoad(_, _, _)).WillByDefault(Return(Status::OK()));
    auto tabletData = std::make_unique<TabletData>("demo");
    auto resourceMap = std::make_shared<ResourceMap>();
    ASSERT_TRUE(tabletData->Init(version, {}, resourceMap).IsOK());
    ON_CALL(*tabletLoader, FinalLoad(_))
        .WillByDefault(
            Return(ByMove(std::pair<Status, std::unique_ptr<TabletData>>(Status::OK(), tabletData.release()))));
    ON_CALL(*tabletLoader, EstimateMemUsed(_, _)).WillByDefault(Return(std::make_pair(Status::OK(), 0)));

    // MemSegment
    std::vector<std::shared_ptr<SegmentDumpItem>> segmentDumpItems;
    auto memSegment = std::make_unique<MockMemSegment>(536870912, fs, Segment::SegmentStatus::ST_BUILDING);
    ON_CALL(*memSegment, Open(_, _)).WillByDefault(Return(Status::OK()));

    // TabletWriter
    auto tabletWriter = std::make_unique<MockTabletWriter>();
    ON_CALL(*tabletWriter, Open(_, _, _)).WillByDefault(Return(Status::OK()));
    if (expectedBuild) {
        EXPECT_CALL(*tabletWriter, Build(_)).WillOnce(Return(Status::OK()));
    } else {
        EXPECT_CALL(*tabletWriter, Build(_)).Times(0);
    }

    // TabletReader
    auto tabletReader1 = std::make_unique<MockTabletReader>();
    auto tabletReader2 = std::make_unique<MockTabletReader>();
    ON_CALL(*tabletReader1, DoOpen(_, _)).WillByDefault(Return(Status::OK()));
    ON_CALL(*tabletReader2, DoOpen(_, _)).WillByDefault(Return(Status::OK()));

    // TabletFactory
    factory = new MockTabletFactory();
    //    ON_CALL(*factory, CreateTabletReader(_)).WillOnce(Return(ByMove(std::move(tabletReader1))));
    ON_CALL(*factory, CreateTabletReader(_)).WillByDefault(Invoke([] {
        auto tabletReader1 = std::make_unique<MockTabletReader>();
        ON_CALL(*tabletReader1, DoOpen(_, _)).WillByDefault(Return(Status::OK()));
        return tabletReader1;
    }));

    ON_CALL(*factory, CreateTabletLoader(_)).WillByDefault(Return(ByMove(std::move(tabletLoader))));
    ON_CALL(*factory, CreateMemSegment(_)).WillByDefault(Return(ByMove(std::move(memSegment))));
    ON_CALL(*factory, CreateTabletWriter(_)).WillByDefault(Return(ByMove(std::move(tabletWriter))));

    Status st = tablet->Open(_indexRoot, schema, std::move(tablet->_tabletOptions), version.GetVersionId());
    ASSERT_TRUE(st.IsOK());
    framework::TabletTestAgent(tablet.get()).TEST_GetTaskScheduler()->CleanTasks();
    auto tabletInfo = framework::TabletTestAgent(tablet.get()).TEST_GetTabletInfos();
    tabletInfo->SetBuildLocator(latestLocator);
    ASSERT_EQ(latestLocator, tabletInfo->GetLatestLocator());
    auto docBatch = std::make_shared<MockDocumentBatch>();
    ON_CALL(*docBatch, GetLastLocator()).WillByDefault(ReturnRef(docLocator));
    status = tablet->Build(docBatch);
    ASSERT_TRUE(status.IsOK());
}

TEST_F(TabletTest, testFilterDoc)
{
    // rollback
    InnerTestFilterDoc(/*LatestLocator = */ Locator(0, 100), /*DocLocator = */ Locator(0, 90), false, false);

    // rollback but allowed
    InnerTestFilterDoc(/*LatestLocator = */ Locator(0, 100), /*DocLocator = */ Locator(0, 90), true, true);

    // same
    InnerTestFilterDoc(/*LatestLocator = */ Locator(0, 100), /*DocLocator = */ Locator(0, 100), false, true);

    // same, but allow
    InnerTestFilterDoc(/*LatestLocator = */ Locator(0, 100), /*DocLocator = */ Locator(0, 100), true, true);

    // normal
    InnerTestFilterDoc(/*LatestLocator = */ Locator(0, 100), /*DocLocator = */ Locator(0, 101), false, true);

    // diffrent src
    InnerTestFilterDoc(/*LatestLocator = */ Locator(0, 100), /*DocLocator = */ Locator(1, 90), false, true);

    // legacy locator, regard as different src
    Locator legacyLocator(0, 100);
    legacyLocator.SetLegacyLocator();
    InnerTestFilterDoc(/*LatestLocator = */ legacyLocator, /*DocLocator = */ Locator(1, 90), false, true);

    Locator locator1;
    SetProgress(locator1, {{0, 99, 100}, {100, 200, 200}});

    Locator locator2;
    SetProgress(locator2, {{0, 99, 200}, {100, 200, 100}});

    Locator locator3;
    SetProgress(locator3, {{0, 99, 200}, {100, 200, 200}});

    InnerTestFilterDoc(/*LatestLocator = */ locator1, /*DocLocator = */ locator2, false, false);

    InnerTestFilterDoc(/*LatestLocator = */ locator2, /*DocLocator = */ locator3, false, true);
}

TEST_F(TabletTest, testImportExternalFiles)
{
    std::string emptyName;
    auto schema = std::make_shared<MockTabletSchema>();
    ON_CALL(*schema, GetSchemaNameImpl()).WillByDefault(ReturnRef(emptyName));
    ON_CALL(*schema, SerializeImpl(_)).WillByDefault(Return(true));

    framework::Version version;
    version.SetSchemaId(0);
    version.SetReadSchemaId(0);

    auto tablet = std::make_unique<FakeTablet>(_resource);
    tablet->SetSchema(schema);
    tablet->_tabletOptions = std::move(_options);
    auto status = tablet->InitIndexDirectory(_indexRoot);
    ASSERT_TRUE(status.IsOK());
    auto fs = framework::TabletTestAgent(tablet.get()).TEST_GetFileSystem();

    // TabletLoader
    auto tabletLoader = std::make_unique<MockTabletLoader>();
    ON_CALL(*tabletLoader, DoPreLoad(_, _, _)).WillByDefault(Return(Status::OK()));
    auto tabletData = std::make_unique<TabletData>("demo");
    auto resourceMap = std::make_shared<ResourceMap>();
    ASSERT_TRUE(tabletData->Init(version, {}, resourceMap).IsOK());
    ON_CALL(*tabletLoader, FinalLoad(_))
        .WillByDefault(
            Return(ByMove(std::pair<Status, std::unique_ptr<TabletData>>(Status::OK(), tabletData.release()))));
    ON_CALL(*tabletLoader, EstimateMemUsed(_, _)).WillByDefault(Return(std::make_pair(Status::OK(), 0)));

    // MemSegment
    std::vector<std::shared_ptr<SegmentDumpItem>> segmentDumpItems;
    auto memSegment = std::make_unique<MockMemSegment>(536870912, fs, Segment::SegmentStatus::ST_BUILDING);
    ON_CALL(*memSegment, Open(_, _)).WillByDefault(Return(Status::OK()));

    // TabletWriter
    auto tabletWriter = std::make_unique<MockTabletWriter>();
    ON_CALL(*tabletWriter, Open(_, _, _)).WillByDefault(Return(Status::OK()));

    EXPECT_CALL(*tabletWriter, Build(_)).Times(0);

    // TabletReader
    auto tabletReader1 = std::make_unique<MockTabletReader>();
    auto tabletReader2 = std::make_unique<MockTabletReader>();
    ON_CALL(*tabletReader1, DoOpen(_, _)).WillByDefault(Return(Status::OK()));
    ON_CALL(*tabletReader2, DoOpen(_, _)).WillByDefault(Return(Status::OK()));

    // TabletFactory
    factory = new MockTabletFactory();
    //    ON_CALL(*factory, CreateTabletReader(_)).WillOnce(Return(ByMove(std::move(tabletReader1))));
    ON_CALL(*factory, CreateTabletReader(_)).WillByDefault(Invoke([] {
        auto tabletReader1 = std::make_unique<MockTabletReader>();
        ON_CALL(*tabletReader1, DoOpen(_, _)).WillByDefault(Return(Status::OK()));
        return tabletReader1;
    }));

    ON_CALL(*factory, CreateTabletLoader(_)).WillByDefault(Return(ByMove(std::move(tabletLoader))));
    ON_CALL(*factory, CreateMemSegment(_)).WillByDefault(Return(ByMove(std::move(memSegment))));
    ON_CALL(*factory, CreateTabletWriter(_)).WillByDefault(Return(ByMove(std::move(tabletWriter))));
    auto options = std::move(tablet->_tabletOptions);
    options->SetIsLeader(true);
    Status st = tablet->Open(_indexRoot, schema, std::move(options), version.GetVersionId());
    ASSERT_TRUE(st.IsOK());
    {
        // success import
        std::string bulkloadId = "123";
        std::vector<std::string> externalFiles;
        std::string externalFile1 = indexlib::util::PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "1.sst");
        std::string externalFile2 = indexlib::util::PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "2.sst");
        externalFiles.emplace_back(externalFile1);
        externalFiles.emplace_back(externalFile2);
        auto importOptions = std::make_shared<ImportExternalFileOptions>();
        auto status = tablet->ImportExternalFiles(bulkloadId, externalFiles, importOptions, Action::ADD,
                                                  /*eventTimeInSecs=*/indexlib::INVALID_TIMESTAMP);
        ASSERT_TRUE(status.IsOK());
    }
    {
        // suspend
        std::string bulkloadId = "123";
        std::vector<std::string> externalFiles;
        auto importOptions = std::make_shared<ImportExternalFileOptions>();
        auto status = tablet->ImportExternalFiles(bulkloadId, externalFiles, /*importOptions=*/nullptr, Action::SUSPEND,
                                                  /*eventTimeInSecs=*/indexlib::INVALID_TIMESTAMP);
        ASSERT_TRUE(status.IsOK());
    }
    {
        // abort
        std::string bulkloadId = "123";
        std::vector<std::string> externalFiles;
        auto importOptions = std::make_shared<ImportExternalFileOptions>();
        auto status = tablet->ImportExternalFiles(bulkloadId, externalFiles, /*importOptions=*/nullptr, Action::ABORT,
                                                  /*eventTimeInSecs=*/indexlib::INVALID_TIMESTAMP);
        ASSERT_TRUE(status.IsOK());
    }
    {
        // invalid external files
        std::string bulkloadId = "1234";
        std::vector<std::string> externalFiles;
        auto importOptions = std::make_shared<ImportExternalFileOptions>();
        auto status = tablet->ImportExternalFiles(bulkloadId, externalFiles, importOptions, Action::ADD,
                                                  /*eventTimeInSecs=*/indexlib::INVALID_TIMESTAMP);
        ASSERT_FALSE(status.IsOK());
    }
    {
        // invalid import options
        std::string bulkloadId = "12345";
        std::vector<std::string> externalFiles;
        auto status = tablet->ImportExternalFiles(bulkloadId, externalFiles, /*importOptions=*/nullptr, Action::ADD,
                                                  /*eventTimeInSecs=*/indexlib::INVALID_TIMESTAMP);
        ASSERT_FALSE(status.IsOK());
    }
    {
        // invalid action
        std::string bulkloadId = "123";
        std::vector<std::string> externalFiles;
        std::string externalFile1 = indexlib::util::PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "1.sst");
        std::string externalFile2 = indexlib::util::PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "2.sst");
        externalFiles.emplace_back(externalFile1);
        externalFiles.emplace_back(externalFile2);
        auto importOptions = std::make_shared<ImportExternalFileOptions>();
        auto status = tablet->ImportExternalFiles(bulkloadId, externalFiles, importOptions, Action::UNKNOWN,
                                                  /*eventTimeInSecs=*/indexlib::INVALID_TIMESTAMP);
        ASSERT_FALSE(status.IsOK());
    }
    {
        // with env IGNORE_BULKLOAD_ID
        std::vector<std::string> externalFiles;
        std::string externalFile1 = indexlib::util::PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "1.sst");
        std::string externalFile2 = indexlib::util::PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "2.sst");
        externalFiles.emplace_back(externalFile1);
        externalFiles.emplace_back(externalFile2);
        auto importOptions = std::make_shared<ImportExternalFileOptions>();
        autil::EnvUtil::setEnv("IGNORE_BULKLOAD_ID", "1;2;3");
        auto status = tablet->ImportExternalFiles("1", externalFiles, importOptions, Action::ADD,
                                                  /*eventTimeInSecs=*/indexlib::INVALID_TIMESTAMP);
        ASSERT_FALSE(status.IsOK());
        status = tablet->ImportExternalFiles("2", externalFiles, importOptions, Action::ADD,
                                             /*eventTimeInSecs=*/indexlib::INVALID_TIMESTAMP);
        ASSERT_FALSE(status.IsOK());
        status = tablet->ImportExternalFiles("3", externalFiles, importOptions, Action::ADD,
                                             /*eventTimeInSecs=*/indexlib::INVALID_TIMESTAMP);
        ASSERT_FALSE(status.IsOK());
        autil::EnvUtil::unsetEnv("IGNORE_BULKLOAD_ID");
        status = tablet->ImportExternalFiles("1", externalFiles, importOptions, Action::ADD,
                                             /*eventTimeInSecs=*/indexlib::INVALID_TIMESTAMP);
        ASSERT_TRUE(status.IsOK());
        status = tablet->ImportExternalFiles("2", externalFiles, importOptions, Action::ADD,
                                             /*eventTimeInSecs=*/indexlib::INVALID_TIMESTAMP);
        ASSERT_TRUE(status.IsOK());
        status = tablet->ImportExternalFiles("3", externalFiles, importOptions, Action::ADD,
                                             /*eventTimeInSecs=*/indexlib::INVALID_TIMESTAMP);
        ASSERT_TRUE(status.IsOK());
    }
    {
        // invalid overwrite
        std::string bulkloadId = "123456";
        std::vector<std::string> externalFiles1;
        auto importOptions = std::make_shared<ImportExternalFileOptions>();
        auto status = tablet->ImportExternalFiles(bulkloadId, externalFiles1, importOptions, Action::ADD,
                                                  /*eventTimeInSecs=*/indexlib::INVALID_TIMESTAMP);
        ASSERT_FALSE(status.IsOK());
        std::vector<std::string> externalFiles2;
        std::string externalFile3 = indexlib::util::PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "1.sst");
        std::string externalFile4 = indexlib::util::PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "2.sst");
        externalFiles2.emplace_back(externalFile3);
        externalFiles2.emplace_back(externalFile4);
        status = tablet->ImportExternalFiles(bulkloadId, externalFiles2, /*importOptions=*/nullptr, Action::OVERWRITE,
                                             /*eventTimeInSecs=*/indexlib::INVALID_TIMESTAMP);
        ASSERT_FALSE(status.IsOK());
        status = tablet->ImportExternalFiles(bulkloadId, externalFiles2, importOptions, Action::OVERWRITE,
                                             /*eventTimeInSecs=*/indexlib::INVALID_TIMESTAMP);
        ASSERT_TRUE(status.IsOK());
    }
    {
        // import external files for follower
        std::string bulkloadId = "1234567";
        std::vector<std::string> externalFiles;
        std::string externalFile1 = indexlib::util::PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "1.sst");
        std::string externalFile2 = indexlib::util::PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "2.sst");
        externalFiles.emplace_back(externalFile1);
        externalFiles.emplace_back(externalFile2);
        auto importOptions = std::make_shared<ImportExternalFileOptions>();
        auto& options = tablet->_tabletOptions;
        options->SetIsLeader(false);
        auto status = tablet->ImportExternalFiles(bulkloadId, externalFiles, importOptions, Action::ADD,
                                                  /*eventTimeInSecs=*/indexlib::INVALID_TIMESTAMP);
        ASSERT_TRUE(tablet->NeedCommit());
        ASSERT_TRUE(status.IsOK());
    }
}

TEST_F(TabletTest, testCheckMemoryStatus)
{
    auto tabletOptions = std::make_shared<indexlibv2::config::TabletOptions>();
    tabletOptions->SetIsOnline(true);
    auto& onlineConfig = tabletOptions->TEST_GetOnlineConfig();
    onlineConfig.TEST_SetMaxRealtimeMemoryUse(1024 * 1024);
    auto memoryQuotaController = std::make_shared<MemoryQuotaController>("test", 1024 * 1024);
    auto memoryQuotaSynchronizer = std::make_shared<MemoryQuotaSynchronizer>(memoryQuotaController);
    auto tabletCommitter = std::make_unique<TabletCommitter>("test");
    auto tabletDumper = std::make_unique<TabletDumper>("test", nullptr, tabletCommitter.get());
    auto resourceMap = std::make_shared<ResourceMap>();
    SegmentMeta segMeta;
    segMeta.segmentId = Segment::RT_SEGMENT_ID_MASK;
    {
        // default memory strategy
        auto tablet = std::make_unique<Tablet>(_resource);
        tablet->_tabletMetrics =
            std::make_shared<TabletMetrics>(nullptr, memoryQuotaController.get(), "test", tabletDumper.get(), nullptr);
        auto readerContainer = std::make_shared<TabletReaderContainer>("testTableName");
        tablet->_tabletMetrics->_tabletMemoryCalculator.reset(new TabletMemoryCalculator(nullptr, readerContainer));
        tablet->_memControlStrategy.reset(new DefaultMemoryControlStrategy(tabletOptions, memoryQuotaSynchronizer));
        EXPECT_EQ(MemoryStatus::OK, tablet->CheckMemoryStatus());
    }
    {
        // default memory strategy
        auto tabletData = std::make_shared<TabletData>("demo");
        auto mockDiskSegment = std::make_unique<MockDiskSegment>(segMeta);
        ON_CALL(*mockDiskSegment, EvaluateCurrentMemUsed).WillByDefault(Return(1024 * 1024 + 1));
        auto segment = std::shared_ptr<Segment>(mockDiskSegment.release());
        ASSERT_TRUE(tabletData->Init(framework::Version(), {segment}, resourceMap).IsOK());
        auto readerContainer = std::make_shared<TabletReaderContainer>("testTableName");
        readerContainer->AddTabletReader(tabletData, nullptr, nullptr);
        auto tablet = std::make_unique<Tablet>(_resource);
        tablet->_tabletMetrics =
            std::make_shared<TabletMetrics>(nullptr, memoryQuotaController.get(), "test", tabletDumper.get(), nullptr);
        tablet->_tabletMetrics->_tabletMemoryCalculator.reset(new TabletMemoryCalculator(nullptr, readerContainer));
        tablet->_memControlStrategy.reset(new DefaultMemoryControlStrategy(tabletOptions, memoryQuotaSynchronizer));
        EXPECT_EQ(MemoryStatus::REACH_MAX_RT_INDEX_SIZE, tablet->CheckMemoryStatus());
    }
    {
        // exclude builtRt memory strategy
        auto tabletData = std::make_shared<TabletData>("demo");
        auto mockDiskSegment = std::make_unique<MockDiskSegment>(segMeta);
        ON_CALL(*mockDiskSegment, EvaluateCurrentMemUsed).WillByDefault(Return(1024 * 1024 + 1));
        auto segment = std::shared_ptr<Segment>(mockDiskSegment.release());
        ASSERT_TRUE(tabletData->Init(framework::Version(), {segment}, resourceMap).IsOK());
        auto readerContainer = std::make_shared<TabletReaderContainer>("testTableName");
        readerContainer->AddTabletReader(tabletData, nullptr, nullptr);
        auto tablet = std::make_unique<Tablet>(_resource);
        tablet->_tabletMetrics =
            std::make_shared<TabletMetrics>(nullptr, memoryQuotaController.get(), "test", tabletDumper.get(), nullptr);
        tablet->_tabletMetrics->_tabletMemoryCalculator.reset(new TabletMemoryCalculator(nullptr, readerContainer));
        tablet->_memControlStrategy.reset(new LSMMemTableControlStrategy(tabletOptions, memoryQuotaSynchronizer));
        EXPECT_EQ(MemoryStatus::OK, tablet->CheckMemoryStatus());
    }
}

} // namespace indexlibv2::framework
