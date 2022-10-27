#include "indexlib/partition/test/reopen_decider_unittest.h"
#include "indexlib/partition/test/mock_partition_resource_calculator.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/online_partition_writer.h"
#include "indexlib/index_base/schema_rewriter.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/test/version_maker.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, ReopenDeciderTest);

namespace {
class MockReopenDecider : public ReopenDecider
{
public:
    MockReopenDecider(const config::OnlineConfig& onlineConfig,
                      bool forceReopen)
        : ReopenDecider(onlineConfig, forceReopen)
    {
        ON_CALL(*this, GetNewOnDiskVersion(_,_,_))
            .WillByDefault(Invoke(this, &MockReopenDecider::DoGetNewOnDiskVersion));
    }

    MOCK_CONST_METHOD2(ValidateDeploySegments, void(
                    const file_system::DirectoryPtr& rootDirectory,
                    const index_base::Version& version));
    
    MOCK_CONST_METHOD3(GetNewOnDiskVersion, bool(
                    const PartitionDataPtr& partitionData,
                    versionid_t reopenVersionId,
                    index_base::Version& version));

    bool DoGetNewOnDiskVersion(const PartitionDataPtr& partitionData,
                               versionid_t reopenVersionId,
                               index_base::Version& version)
    {
        return ReopenDecider::GetNewOnDiskVersion(partitionData, reopenVersionId, version);
    }

};

class MockBuildingPartitionData : public BuildingPartitionData
{
public:
    MockBuildingPartitionData(
            const config::IndexPartitionOptions& options,
            const config::IndexPartitionSchemaPtr& schema,
            const util::PartitionMemoryQuotaControllerPtr& memController)
        : BuildingPartitionData(options, schema, memController,
                                DumpSegmentContainerPtr(new DumpSegmentContainer))
    {}

    MOCK_CONST_METHOD0(GetLastValidRtSegmentInLinkDirectory, segmentid_t());
    MOCK_CONST_METHOD0(GetOnDiskVersion, index_base::Version());
};

}



ReopenDeciderTest::ReopenDeciderTest()
{
}

ReopenDeciderTest::~ReopenDeciderTest()
{
}

void ReopenDeciderTest::CaseSetUp()
{
    string field = "string1:string;string2:string;price:uint32";
    string index = "index2:string:string2;"
                   "pk:primarykey64:string1";

    string attribute = "string1;price";
    mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
    mRtSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
    SchemaRewriter::RewriteForRealtimeTimestamp(mRtSchema);
    mRootDir = GET_TEST_DATA_PATH();
}

void ReopenDeciderTest::CaseTearDown()
{
}

void ReopenDeciderTest::TestNoNeedReopen()
{
    OnlineConfig onlineConfig;
    AttributeMetrics attrMetrics;
    InnerTestForNoNewIncVersion(onlineConfig, attrMetrics, true, 
                                ReopenDecider::NO_NEED_REOPEN);

    InnerTestForNoNewIncVersion(onlineConfig, attrMetrics, false, 
                                ReopenDecider::NO_NEED_REOPEN);
}

void ReopenDeciderTest::TestNeedReclaimReaderReopen()
{
    OnlineConfig onlineConfig;
    AttributeMetrics attrMetrics;
    attrMetrics.SetCurReaderReclaimableBytesValue(10 * 1024 * 1024);
    onlineConfig.maxCurReaderReclaimableMem = 10;

    // need reclaim reader
    InnerTestForNoNewIncVersion(onlineConfig, attrMetrics, true, 
                                ReopenDecider::RECLAIM_READER_REOPEN);
    InnerTestForNoNewIncVersion(onlineConfig, attrMetrics, false, 
                                ReopenDecider::RECLAIM_READER_REOPEN);
    // no need reclaim reader
    onlineConfig.maxCurReaderReclaimableMem = 11;
    InnerTestForNoNewIncVersion(onlineConfig, attrMetrics, true, 
                                ReopenDecider::NO_NEED_REOPEN);
    InnerTestForNoNewIncVersion(onlineConfig, attrMetrics, false, 
                                ReopenDecider::NO_NEED_REOPEN);
}

void ReopenDeciderTest::TestNeedSwitchFlushRtSegments()
{
    Version invalidVersion;
    IndexPartitionOptions options;
    MockBuildingPartitionData* mockPartData = new MockBuildingPartitionData(
            options, mSchema, 
            util::MemoryQuotaControllerCreator::CreatePartitionMemoryController());

    EXPECT_CALL(*mockPartData, GetOnDiskVersion()).WillRepeatedly(
            Return(INVALID_VERSION));
    PartitionDataPtr partData(mockPartData);
    
    OnlinePartitionReaderPtr onlineReader(new OnlinePartitionReader(
                    options, mSchema, util::SearchCachePartitionWrapperPtr()));
    onlineReader->TEST_SetPartitionVersion(
            PartitionVersion(invalidVersion, INVALID_SEGMENTID, 100));

    AttributeMetrics attrMetrics;
    MockReopenDecider rd(options.GetOnlineConfig(), false);
    EXPECT_CALL(rd, GetNewOnDiskVersion(_, _, _)).WillRepeatedly(Return(false));

    {
        EXPECT_CALL(*mockPartData, GetLastValidRtSegmentInLinkDirectory())
            .WillOnce(Return(INVALID_SEGMENTID));
        rd.Init(partData, &attrMetrics, INVALID_VERSION, onlineReader);
        ASSERT_EQ(ReopenDecider::NO_NEED_REOPEN, rd.GetReopenType());
    }

    {
        EXPECT_CALL(*mockPartData, GetLastValidRtSegmentInLinkDirectory())
            .WillOnce(Return(100));
        rd.Init(partData, &attrMetrics, INVALID_VERSION, onlineReader);
        ASSERT_EQ(ReopenDecider::NO_NEED_REOPEN, rd.GetReopenType());
    }

    {
        EXPECT_CALL(*mockPartData, GetLastValidRtSegmentInLinkDirectory())
            .WillOnce(Return(101));
        rd.Init(partData, &attrMetrics, INVALID_VERSION, onlineReader);
        ASSERT_EQ(ReopenDecider::SWITCH_RT_SEGMENT_REOPEN, rd.GetReopenType());
    }
}

void ReopenDeciderTest::TestNormalReopen()
{
    InnerTestForNewIncVersion(false, false, ReopenDecider::NORMAL_REOPEN);
}

void ReopenDeciderTest::TestForceReopen()
{
    InnerTestForNewIncVersion(true, false, ReopenDecider::FORCE_REOPEN);
}

void ReopenDeciderTest::TestReopenWithNewSchemaVersion()
{
    InnerTestForNewIncVersion(false, true, ReopenDecider::INCONSISTENT_SCHEMA_REOPEN);
}

void ReopenDeciderTest::InnerTestForNoNewIncVersion(
        const OnlineConfig& onlineConfig,
        const AttributeMetrics& attrMetrics,
        bool forceReopen,
        ReopenDecider::ReopenType expectType)
{
    Version expectIncReopenVersion = INVALID_VERSION;
    IndexPartitionOptions options;
    options.SetOnlineConfig(onlineConfig);
    DumpSegmentContainerPtr dumpContainer(new DumpSegmentContainer);
    PartitionDataPtr partitionData = 
        PartitionDataCreator::CreateBuildingPartitionData(
                GET_FILE_SYSTEM(), mSchema, options,
                util::MemoryQuotaControllerCreator::CreatePartitionMemoryController(),
                dumpContainer);

    PartitionWriterPtr writer;
    ReopenDecider rd(onlineConfig, forceReopen);
    rd.Init(partitionData, &attrMetrics);

    ASSERT_EQ(expectType, rd.GetReopenType());
    ASSERT_EQ(expectIncReopenVersion, rd.GetReopenIncVersion());
}

void ReopenDeciderTest::InnerTestForNewIncVersion(
        bool forceReopen, bool newSchemaVersion,
        ReopenDecider::ReopenType expectType)
{
    TearDown();
    SetUp();

    IndexPartitionOptions options;
    Version invalidVersion;
    OnlineConfig onlineConfig;
    MockReopenDecider rd(onlineConfig, forceReopen);

    if (!newSchemaVersion)
    {
        EXPECT_CALL(rd, ValidateDeploySegments(_,_)).Times(1);
    }
    EXPECT_CALL(rd, GetNewOnDiskVersion(_,_,_))
        .WillRepeatedly(Invoke(&rd, &MockReopenDecider::DoGetNewOnDiskVersion));
    
    // make version.0 (has segment_0)
    PartitionDataMaker::MakePartitionDataFiles(
            0, 0, GET_PARTITION_DIRECTORY(), "0,10,0");

    DumpSegmentContainerPtr dumpContainer(new DumpSegmentContainer);
    PartitionDataPtr partitionData = 
        PartitionDataCreator::CreateBuildingPartitionData(
                GET_FILE_SYSTEM(), mSchema, options,
                util::MemoryQuotaControllerCreator::CreatePartitionMemoryController(),
                dumpContainer);
    // make version.1 (has segment_0, segment_1)
    Version expectIncVersion = VersionMaker::Make(1, "0,1", "", "", 1);
    if (newSchemaVersion)
    {
        expectIncVersion.SetSchemaVersionId(1);
    }
    expectIncVersion.Store(GET_PARTITION_DIRECTORY(), true);

    AttributeMetrics attrMetrics;
    rd.Init(partitionData, &attrMetrics);
    ASSERT_EQ(expectType, rd.GetReopenType());
    if (!newSchemaVersion)
    {
        ASSERT_EQ(expectIncVersion, rd.GetReopenIncVersion());
    }
}

IE_NAMESPACE_END(partition);

