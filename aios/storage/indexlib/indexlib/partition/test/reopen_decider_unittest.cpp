#include "indexlib/partition/test/reopen_decider_unittest.h"

#include "indexlib/config/impl/online_config_impl.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/schema_rewriter.h"
#include "indexlib/partition/building_partition_data.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/partition/online_partition_writer.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/test/mock_partition_resource_calculator.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/version_maker.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"

using namespace std;
using namespace autil;
using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::file_system;
using namespace indexlib::index_base;

using namespace indexlib::util;
namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, ReopenDeciderTest);

namespace {
class MockReopenDecider : public ReopenDecider
{
public:
    MockReopenDecider(const config::OnlineConfig& onlineConfig, bool forceReopen)
        : ReopenDecider(onlineConfig, forceReopen)
    {
        ON_CALL(*this, GetNewOnDiskVersion(_, _, _, _))
            .WillByDefault(Invoke(this, &MockReopenDecider::DoGetNewOnDiskVersion));
    }

    MOCK_METHOD(void, ValidateDeploySegments,
                (const PartitionDataPtr& partitionData, const index_base::Version& version), (const, override));
    MOCK_METHOD(bool, GetNewOnDiskVersion,
                (const PartitionDataPtr& partitionData, const std::string& indexSourceRoot, versionid_t reopenVersionId,
                 index_base::Version& version),
                (const, override));

    bool DoGetNewOnDiskVersion(const PartitionDataPtr& partitionData, const std::string& indexSourceRoot,
                               versionid_t reopenVersionId, index_base::Version& version)
    {
        return ReopenDecider::GetNewOnDiskVersion(partitionData, indexSourceRoot, reopenVersionId, version);
    }
};

class MockBuildingPartitionData : public BuildingPartitionData
{
public:
    MockBuildingPartitionData(const config::IndexPartitionOptions& options,
                              const config::IndexPartitionSchemaPtr& schema,
                              const util::PartitionMemoryQuotaControllerPtr& memController)
        : BuildingPartitionData(options, schema, memController, DumpSegmentContainerPtr(new DumpSegmentContainer))
    {
    }

    MOCK_METHOD(segmentid_t, GetLastValidRtSegmentInLinkDirectory, (), (const, override));
    MOCK_METHOD(index_base::Version, GetOnDiskVersion, (), (const, override));
    MOCK_METHOD(string, GetLastLocator, (), (const, override));
};

} // namespace

ReopenDeciderTest::ReopenDeciderTest() {}

ReopenDeciderTest::~ReopenDeciderTest() {}

void ReopenDeciderTest::CaseSetUp()
{
    string field = "string1:string;string2:string;price:uint32";
    string index = "index2:string:string2;"
                   "pk:primarykey64:string1";

    string attribute = "string1;price";
    mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
    mRtSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
    SchemaRewriter::RewriteForRealtimeTimestamp(mRtSchema);
    mRootDir = GET_TEMP_DATA_PATH();
    file_system::FileSystemOptions fsOptions;
    fsOptions.isOffline = true;
    fsOptions.outputStorage = FSST_MEM;
    RESET_FILE_SYSTEM("ut", false, fsOptions);
}

void ReopenDeciderTest::CaseTearDown() {}

void ReopenDeciderTest::TestNoNeedReopen()
{
    OnlineConfig onlineConfig;
    AttributeMetrics attrMetrics;
    InnerTestForNoNewIncVersion(onlineConfig, attrMetrics, true, ReopenDecider::NO_NEED_REOPEN);

    InnerTestForNoNewIncVersion(onlineConfig, attrMetrics, false, ReopenDecider::NO_NEED_REOPEN);
}

void ReopenDeciderTest::TestNeedReclaimReaderReopen()
{
    OnlineConfig onlineConfig;
    AttributeMetrics attrMetrics;
    attrMetrics.SetCurReaderReclaimableBytesValue(10 * 1024 * 1024);
    onlineConfig.maxCurReaderReclaimableMem = 10;

    // need reclaim reader
    InnerTestForNoNewIncVersion(onlineConfig, attrMetrics, true, ReopenDecider::RECLAIM_READER_REOPEN);
    InnerTestForNoNewIncVersion(onlineConfig, attrMetrics, false, ReopenDecider::RECLAIM_READER_REOPEN);
    // no need reclaim reader
    onlineConfig.maxCurReaderReclaimableMem = 11;
    InnerTestForNoNewIncVersion(onlineConfig, attrMetrics, true, ReopenDecider::NO_NEED_REOPEN);
    InnerTestForNoNewIncVersion(onlineConfig, attrMetrics, false, ReopenDecider::NO_NEED_REOPEN);
}

void ReopenDeciderTest::TestNeedSwitchFlushRtSegments()
{
    Version invalidVersion;
    IndexPartitionOptions options;
    MockBuildingPartitionData* mockPartData = new MockBuildingPartitionData(
        options, mSchema, util::MemoryQuotaControllerCreator::CreatePartitionMemoryController());

    EXPECT_CALL(*mockPartData, GetOnDiskVersion()).WillRepeatedly(Return(index_base::Version(INVALID_VERSIONID)));
    PartitionDataPtr partData(mockPartData);

    std::set<segmentid_t> dumpingSegmentIds;
    OnlinePartitionReaderPtr onlineReader(
        new OnlinePartitionReader(options, mSchema, util::SearchCachePartitionWrapperPtr()));
    onlineReader->TEST_SetPartitionVersion(
        std::make_unique<PartitionVersion>(invalidVersion, INVALID_SEGMENTID, 100, dumpingSegmentIds));

    AttributeMetrics attrMetrics;
    MockReopenDecider rd(options.GetOnlineConfig(), false);
    EXPECT_CALL(rd, GetNewOnDiskVersion(_, _, _, _)).WillRepeatedly(Return(false));

    {
        EXPECT_CALL(*mockPartData, GetLastValidRtSegmentInLinkDirectory()).WillOnce(Return(INVALID_SEGMENTID));
        rd.Init(partData, "", &attrMetrics, std::numeric_limits<int64_t>::max(), INVALID_VERSIONID, onlineReader);
        ASSERT_EQ(ReopenDecider::NO_NEED_REOPEN, rd.GetReopenType());
    }

    {
        EXPECT_CALL(*mockPartData, GetLastValidRtSegmentInLinkDirectory()).WillOnce(Return(100));
        rd.Init(partData, "", &attrMetrics, std::numeric_limits<int64_t>::max(), INVALID_VERSIONID, onlineReader);
        ASSERT_EQ(ReopenDecider::NO_NEED_REOPEN, rd.GetReopenType());
    }

    {
        EXPECT_CALL(*mockPartData, GetLastValidRtSegmentInLinkDirectory()).WillOnce(Return(101));
        rd.Init(partData, "", &attrMetrics, std::numeric_limits<int64_t>::max(), INVALID_VERSIONID, onlineReader);
        ASSERT_EQ(ReopenDecider::SWITCH_RT_SEGMENT_REOPEN, rd.GetReopenType());
    }
}

void ReopenDeciderTest::TestNoReopenWhenRecover()
{
    IndexPartitionOptions options;
    Version invalidVersion;
    OnlineConfig onlineConfig;
    onlineConfig.SetAllowReopenOnRecoverRt(false);
    MockReopenDecider rd(onlineConfig, false);

    EXPECT_CALL(rd, GetNewOnDiskVersion(_, _, _, _)).WillRepeatedly(Return(true));
    DumpSegmentContainerPtr dumpContainer(new DumpSegmentContainer);

    MockBuildingPartitionData* mockPartData = new MockBuildingPartitionData(
        options, mSchema, util::MemoryQuotaControllerCreator::CreatePartitionMemoryController());

    document::IndexLocator locator(0, 10, /*userData=*/"");
    EXPECT_CALL(*mockPartData, GetLastLocator()).WillRepeatedly(Return(locator.toString()));
    const file_system::DirectoryPtr dir;

    AttributeMetrics attrMetrics;
    PartitionDataPtr partData(mockPartData);
    rd.Init(partData, "", &attrMetrics, 100);
    ASSERT_EQ(ReopenDecider::UNABLE_REOPEN, rd.GetReopenType());
}

void ReopenDeciderTest::TestNormalReopen() { InnerTestForNewIncVersion(false, false, ReopenDecider::NORMAL_REOPEN); }

void ReopenDeciderTest::TestForceReopen() { InnerTestForNewIncVersion(true, false, ReopenDecider::FORCE_REOPEN); }

void ReopenDeciderTest::TestReopenWithNewSchemaVersion()
{
    InnerTestForNewIncVersion(false, true, ReopenDecider::INCONSISTENT_SCHEMA_REOPEN);
}

void ReopenDeciderTest::InnerTestForNoNewIncVersion(const OnlineConfig& onlineConfig,
                                                    const AttributeMetrics& attrMetrics, bool forceReopen,
                                                    ReopenDecider::ReopenType expectType)
{
    Version expectIncReopenVersion = index_base::Version(INVALID_VERSIONID);
    IndexPartitionOptions options;
    options.SetOnlineConfig(onlineConfig);
    DumpSegmentContainerPtr dumpContainer(new DumpSegmentContainer);
    auto memController = util::MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    util::CounterMapPtr counterMap;
    plugin::PluginManagerPtr pluginManager;
    util::MetricProviderPtr metricProvider;
    BuildingPartitionParam param(options, mSchema, memController, dumpContainer, counterMap, pluginManager,
                                 metricProvider, document::SrcSignature());
    PartitionDataPtr partitionData = PartitionDataCreator::CreateBuildingPartitionData(param, GET_FILE_SYSTEM());

    PartitionWriterPtr writer;
    ReopenDecider rd(onlineConfig, forceReopen);
    rd.Init(partitionData, GET_TEMP_DATA_PATH(), &attrMetrics, std::numeric_limits<int64_t>::max());

    ASSERT_EQ(expectType, rd.GetReopenType());
    ASSERT_EQ(expectIncReopenVersion, rd.GetReopenIncVersion());
}

void ReopenDeciderTest::InnerTestForNewIncVersion(bool forceReopen, bool newSchemaVersion,
                                                  ReopenDecider::ReopenType expectType, int64_t timestamp)
{
    tearDown();
    setUp();

    IndexPartitionOptions options;
    Version invalidVersion;
    OnlineConfig onlineConfig;
    MockReopenDecider rd(onlineConfig, forceReopen);

    EXPECT_CALL(rd, GetNewOnDiskVersion(_, _, _, _))
        .WillRepeatedly(Invoke(&rd, &MockReopenDecider::DoGetNewOnDiskVersion));

    // make version.0 (has segment_0)
    PartitionDataMaker::MakePartitionDataFiles(0, 0, GET_PARTITION_DIRECTORY(), "0,10,0");

    DumpSegmentContainerPtr dumpContainer(new DumpSegmentContainer);
    auto memController = util::MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    util::CounterMapPtr counterMap;
    plugin::PluginManagerPtr pluginManager;
    util::MetricProviderPtr metricProvider;
    BuildingPartitionParam param(options, mSchema, memController, dumpContainer, counterMap, pluginManager,
                                 metricProvider, document::SrcSignature());
    PartitionDataPtr partitionData = PartitionDataCreator::CreateBuildingPartitionData(param, GET_FILE_SYSTEM());
    // make version.1 (has segment_0, segment_1)
    Version expectIncVersion = VersionMaker::Make(1, "0,1", "", "", 1);
    if (newSchemaVersion) {
        expectIncVersion.SetSchemaVersionId(1);
    }
    expectIncVersion.Store(GET_PARTITION_DIRECTORY(), true);

    AttributeMetrics attrMetrics;
    rd.Init(partitionData, GET_TEMP_DATA_PATH(), &attrMetrics, timestamp);
    ASSERT_EQ(expectType, rd.GetReopenType());
    if (!newSchemaVersion) {
        ASSERT_EQ(expectIncVersion, rd.GetReopenIncVersion());
    }
}
}} // namespace indexlib::partition
