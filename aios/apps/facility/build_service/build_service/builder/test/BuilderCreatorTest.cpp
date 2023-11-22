#include "build_service/builder/BuilderCreator.h"

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/jsonizable.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/builder/Builder.h"
#include "build_service/builder/SortedBuilder.h"
#include "build_service/common/Locator.h"
#include "build_service/common_define.h"
#include "build_service/config/BuildRuleConfig.h"
#include "build_service/config/BuilderClusterConfig.h"
#include "build_service/config/BuilderConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/CheckpointList.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/test/unittest.h"
#include "build_service/util/ErrorLogCollector.h"
#include "build_service/util/IndexPathConstructor.h"
#include "build_service/util/RangeUtil.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/FileLock.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/SortDescription.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_partition_schema_maker.h"
#include "indexlib/config/load_config_list.h"
#include "indexlib/file_system/fslib/FenceContext.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/index_base/index_meta/parallel_build_info.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/builder_branch_hinter.h"
#include "indexlib/partition/index_builder.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/partition/table_reader_container_updater.h"
#include "indexlib/util/EpochIdUtil.h"
#include "indexlib/util/ErrorLogCollector.h"
#include "indexlib/util/memory_control/QuotaControl.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;
using namespace autil::legacy;
using testing::ElementsAre;

using namespace indexlib::index_base;
using namespace indexlib::partition;
using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::util;

using namespace build_service::util;
using namespace build_service::proto;
using namespace build_service::config;
using namespace build_service::common;
namespace build_service { namespace builder {

class MockBuilderCreator : public BuilderCreator
{
public:
    MockBuilderCreator(const IndexPartitionPtr& indexPart = IndexPartitionPtr()) : BuilderCreator(indexPart) {}
    ~MockBuilderCreator() {}

public:
    MOCK_METHOD5(createIndexBuilder,
                 indexlib::partition::IndexBuilderPtr(const config::ResourceReaderPtr&, const BuilderClusterConfig&,
                                                      const QuotaControlPtr&,
                                                      const indexlib::partition::BuilderBranchHinter::Option&,
                                                      indexlib::util::MetricProviderPtr));
    MOCK_METHOD3(createSortedBuilder, Builder*(const IndexBuilderPtr&, int64_t, fslib::fs::FileLock*));
    MOCK_METHOD2(createNormalBuilder, Builder*(const IndexBuilderPtr&, fslib::fs::FileLock*));
    MOCK_METHOD3(initBuilder, bool(Builder*, const BuilderConfig&, indexlib::util::MetricProviderPtr));
};

class BuilderCreatorTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp() override;
    void tearDown() override;

private:
    string _configPath;
    string _indexPath;
    ResourceReaderPtr _resourceReader;
    KeyValueMap _kvMap;
};

void BuilderCreatorTest::setUp()
{
    _configPath = GET_TEST_DATA_PATH() + "/buildCreatorTest/";
    _indexPath = GET_TEMP_DATA_PATH();
    _resourceReader.reset(new ResourceReader(_configPath));
    _kvMap.clear();
    _kvMap[INDEX_ROOT_PATH] = _indexPath;
    _kvMap[BUILDER_EPOCH_ID] = EpochIdUtil::TEST_GenerateEpochId();
}

void BuilderCreatorTest::tearDown() {}

TEST_F(BuilderCreatorTest, testCreateForIncParallelBuild)
{
    BuilderCreator creator;
    PartitionId partId;
    *partId.add_clusternames() = "parallel";
    partId.mutable_buildid()->set_datatable("simple");
    partId.mutable_buildid()->set_generationid(0);

    BuildRuleConfig buildRuleConfig;
    _resourceReader->getClusterConfigWithJsonPath("parallel", "cluster_config.builder_rule_config", buildRuleConfig);
    uint32_t workerPathVersion = 2;
    KeyValueMap kvMap;
    kvMap[INDEX_ROOT_PATH] = GET_TEMP_DATA_PATH();
    kvMap[ZOOKEEPER_ROOT] = GET_TEMP_DATA_PATH();
    kvMap[WORKER_PATH_VERSION] = StringUtil::toString(workerPathVersion);
    kvMap[OPERATION_IDS] = "1,2,3";
    kvMap[BUILDER_EPOCH_ID] = EpochIdUtil::TEST_GenerateEpochId();

    // full build
    Builder* builder = creator.create(_resourceReader, kvMap, partId);
    ASSERT_TRUE(builder);
    IndexPartitionOptions options = builder->_indexBuilder->TEST_GET_INDEX_PARTITION_OPTIONS();
    ASSERT_THAT(options.GetOngoingModifyOperationIds(), ElementsAre(1, 2, 3));

    string indexPath = IndexPathConstructor::constructIndexPath(_indexPath, partId);
    ASSERT_EQ(EC_TRUE, FileSystem::isExist(indexPath));
    DELETE_AND_SET_NULL(builder);

    {
        kvMap[OPERATION_IDS] = "3,5";
        vector<proto::Range> ranges =
            util::RangeUtil::splitRange(0, 65535, buildRuleConfig.partitionCount, buildRuleConfig.incBuildParallelNum);
        // inc build
        // partitioncount = 1, prallelNum = 2
        *partId.mutable_range() = *ranges.rbegin();
        partId.set_step(BUILD_STEP_INC);
        builder = creator.create(_resourceReader, kvMap, partId);
        ASSERT_TRUE(builder);
        IndexPartitionOptions options = builder->_indexBuilder->TEST_GET_INDEX_PARTITION_OPTIONS();
        ASSERT_THAT(options.GetOngoingModifyOperationIds(), ElementsAre(3, 5));

        ParallelBuildInfo expectParallInfo(buildRuleConfig.incBuildParallelNum, workerPathVersion,
                                           buildRuleConfig.incBuildParallelNum - 1);
        string parallelInstancePath = expectParallInfo.GetParallelInstancePath(indexPath);
        ASSERT_EQ(EC_TRUE, FileSystem::isExist(parallelInstancePath));
        DELETE_AND_SET_NULL(builder);
    }
    {
        // test builder range does not match inc_build_parallel_num
        vector<proto::Range> ranges = util::RangeUtil::splitRange(0, 65535, buildRuleConfig.partitionCount, 1);
        // inc build
        // partitioncount = 1, inc_build_prallelNum = 2
        *partId.mutable_range() = *ranges.begin();
        partId.set_step(BUILD_STEP_INC);
        builder = creator.create(_resourceReader, kvMap, partId);
        ASSERT_TRUE(builder);
        ParallelBuildInfo expectParallInfo(1u, workerPathVersion, 0);
        string parallelInstancePath = expectParallInfo.GetParallelInstancePath(indexPath);
        ASSERT_EQ(EC_TRUE, FileSystem::isExist(parallelInstancePath));
        DELETE_AND_SET_NULL(builder);
    }
    { // test partition_count = 2, inc_build_parallel_num = 2
        _resourceReader->getClusterConfigWithJsonPath("parallel1", "cluster_config.builder_rule_config",
                                                      buildRuleConfig);
        vector<proto::Range> ranges = util::RangeUtil::splitRange(0, 65535, buildRuleConfig.partitionCount, 1);
        *partId.mutable_clusternames(0) = "parallel1";
        *partId.mutable_range() = *ranges.rbegin();

        partId.set_step(BUILD_STEP_INC);
        builder = creator.create(_resourceReader, kvMap, partId);
        ASSERT_TRUE(builder);
        ParallelBuildInfo expectParallInfo(1u, workerPathVersion, 0);
        string parallelInstancePath = expectParallInfo.GetParallelInstancePath(indexPath);
        ASSERT_EQ(EC_TRUE, FileSystem::isExist(parallelInstancePath));
        DELETE_AND_SET_NULL(builder);
    }
}

TEST_F(BuilderCreatorTest, testCreateSuccess)
{
    BuilderCreator creator;
    PartitionId partId;
    *partId.add_clusternames() = "simple";
    partId.mutable_buildid()->set_datatable("simple");
    partId.mutable_buildid()->set_generationid(0);

    KeyValueMap kvMap;
    kvMap[INDEX_ROOT_PATH] = GET_TEMP_DATA_PATH();
    kvMap[ZOOKEEPER_ROOT] = GET_TEMP_DATA_PATH();
    kvMap[BUILDER_EPOCH_ID] = EpochIdUtil::TEST_GenerateEpochId();
    Builder* builder = creator.create(_resourceReader, kvMap, partId);
    ASSERT_TRUE(builder);
    ASSERT_EQ(EC_TRUE, FileSystem::isExist(GET_TEMP_DATA_PATH() + "/generation..simple.0.simple.0.65535.__lock__"));

    string indexPath = IndexPathConstructor::constructIndexPath(_indexPath, partId);
    indexlib::index_base::PartitionMeta meta;
    EXPECT_NO_THROW(meta.Load(indexPath));
    ASSERT_EQ(size_t(1), meta.Size());
    indexlibv2::config::SortDescription sortDesc("id", indexlibv2::config::sp_desc);
    EXPECT_EQ(sortDesc, meta.GetSortDescription(0));

    DELETE_AND_SET_NULL(builder);
}

TEST_F(BuilderCreatorTest, testAddBuildPhase)
{
    BuilderCreator creator;
    PartitionId partId;
    partId.set_step(BUILD_STEP_INC);
    *partId.add_clusternames() = "simple";
    partId.mutable_buildid()->set_datatable("simple");
    partId.mutable_buildid()->set_generationid(0);
    KeyValueMap kvMap;
    kvMap[INDEX_ROOT_PATH] = GET_TEMP_DATA_PATH();
    kvMap[ZOOKEEPER_ROOT] = GET_TEMP_DATA_PATH();
    kvMap[BUILDER_EPOCH_ID] = EpochIdUtil::TEST_GenerateEpochId();
    Builder* builder = creator.create(_resourceReader, kvMap, partId);
    ASSERT_TRUE(builder);
    // ASSERT_EQ(BuildConfig::BP_INC, builder->TEST_GET_BUILD_STEP());
}

TEST_F(BuilderCreatorTest, testBuilderClusterInitFailed)
{
    BuilderCreator creator;
    PartitionId partId;
    string configPath = GET_TEST_DATA_PATH() + "/buildCreatorTestFail";
    ResourceReaderPtr resourceReader(new ResourceReader(configPath));
    *partId.add_clusternames() = "simple";
    ASSERT_FALSE(creator.create(resourceReader, _kvMap, partId));
}

TEST_F(BuilderCreatorTest, testSortBuild)
{
    IndexPartitionPtr indexPart = IndexPartitionPtr();
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(schema, "int:INT32", "pk:primarykey64:int", "int", "");

    QuotaControlPtr quotaControl(new QuotaControl(1024));
    IndexPartitionPtr indexPart2(new OnlinePartition());
    indexPart2->mSchema = schema; // IndexBuilder constructor need schema;
    IndexBuilderPtr indexBuilder(new IndexBuilder(indexPart2, quotaControl));
    MockBuilderCreator creator(indexPart);
    PartitionId partId;
    *partId.add_clusternames() = "simple";
    EXPECT_CALL(creator, createIndexBuilder(_, _, _, _, _)).WillOnce(Return(indexBuilder));
    EXPECT_CALL(creator, createSortedBuilder(_, _, _));
    EXPECT_CALL(creator, initBuilder(_, _, _)).WillOnce(Return(true));
    Builder* builder = creator.create(_resourceReader, _kvMap, partId);
    DELETE_AND_SET_NULL(builder);
}

TEST_F(BuilderCreatorTest, testRealtimeBuild)
{
    IndexPartitionPtr indexPart(new OnlinePartition());
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(schema, "int:INT32", "pk:primarykey64:int", "int", "");
    indexPart->mSchema = schema; // IndexBuilder constructor need schema;
    QuotaControlPtr quotaControl(new QuotaControl(1024));
    IndexBuilderPtr indexBuilder(new IndexBuilder(indexPart, quotaControl));
    MockBuilderCreator creator(indexPart);
    PartitionId partId;
    *partId.add_clusternames() = "simple";
    EXPECT_CALL(creator, createIndexBuilder(_, _, _, _, _)).WillOnce(Return(indexBuilder));
    EXPECT_CALL(creator, createNormalBuilder(_, _));
    EXPECT_CALL(creator, initBuilder(_, _, _)).WillOnce(Return(true));
    Builder* builder = creator.create(_resourceReader, _kvMap, partId);
    DELETE_AND_SET_NULL(builder);
}

TEST_F(BuilderCreatorTest, testIllegalClusterConfig)
{
    BuilderCreator creator;
    PartitionId partId;
    *partId.add_clusternames() = "simple1";
    ASSERT_FALSE(creator.create(_resourceReader, _kvMap, partId));
}

TEST_F(BuilderCreatorTest, testIllegalSchema)
{
    BuilderCreator creator;
    PartitionId partId;
    *partId.add_clusternames() = "simple2";
    ASSERT_FALSE(creator.create(_resourceReader, _kvMap, partId));
}

TEST_F(BuilderCreatorTest, testNoIndexPath)
{
    BuilderCreator creator;
    KeyValueMap kvMap;
    PartitionId partId;
    *partId.add_clusternames() = "simple";
    ASSERT_FALSE(creator.create(_resourceReader, kvMap, partId));
}

TEST_F(BuilderCreatorTest, testPartitionMetaInCompatible)
{
    BuilderCreator creator;

    PartitionId partId;
    *partId.add_clusternames() = "simple";

    string indexPath = IndexPathConstructor::constructIndexPath(_indexPath, partId);
    fslib::util::FileUtil::mkDir(indexPath, true);
    indexlib::index_base::PartitionMeta meta;
    meta.Store(indexPath, indexlib::file_system::FenceContext::NoFence());

    Builder* builder = creator.create(_resourceReader, _kvMap, partId);
    ASSERT_FALSE(builder);
}

TEST_F(BuilderCreatorTest, testCreateLockWithNotExistPath)
{
    BuilderCreator creator;
    KeyValueMap kvMap;
    kvMap[INDEX_ROOT_PATH] = GET_TEMP_DATA_PATH();
    PartitionId partId;
    *partId.add_clusternames() = "simple";
    string zkRootPath = "unknownfsType://not_exist_path";
    kvMap[ZOOKEEPER_ROOT] = zkRootPath;
    kvMap[BUILDER_EPOCH_ID] = EpochIdUtil::TEST_GenerateEpochId();
    Builder* builder = creator.create(_resourceReader, kvMap, partId);
    ASSERT_FALSE(builder);
}

TEST_F(BuilderCreatorTest, testZkRootIsEmpty)
{
    BuilderCreator creator;
    KeyValueMap kvMap;
    kvMap[INDEX_ROOT_PATH] = GET_TEMP_DATA_PATH();
    PartitionId partId;
    *partId.add_clusternames() = "simple";
    kvMap[ZOOKEEPER_ROOT] = "";
    kvMap[BUILDER_EPOCH_ID] = EpochIdUtil::TEST_GenerateEpochId();
    Builder* builder = creator.create(_resourceReader, kvMap, partId);
    ASSERT_TRUE(builder);
}

TEST_F(BuilderCreatorTest, testCreateMemoryQuotaControl)
{
    {
        // test for offline
        BuilderCreator creator;
        creator._indexPart.reset();

        int64_t buildTotalMemory = 1000;
        BuilderConfig builderConfig;

        // sort build
        builderConfig.sortBuild = true;
        QuotaControlPtr quotaControl = creator.createMemoryQuotaControl(buildTotalMemory, builderConfig);
        ASSERT_TRUE(quotaControl);
        ASSERT_EQ((size_t)1000 * 1024 * 1024, quotaControl->GetTotalQuota());
        ASSERT_EQ((size_t)400 * 1024 * 1024, quotaControl->GetLeftQuota());

        // not sort build
        builderConfig.sortBuild = false;
        quotaControl = creator.createMemoryQuotaControl(buildTotalMemory, builderConfig);
        ASSERT_TRUE(quotaControl);
        ASSERT_EQ((size_t)1000 * 1024 * 1024, quotaControl->GetTotalQuota());
        ASSERT_EQ((size_t)1000 * 1024 * 1024, quotaControl->GetLeftQuota());
    }
    {
        // test for online
        BuilderCreator creator;
        creator._indexPart.reset(new OnlinePartition());

        int64_t buildTotalMemory = 1000;
        BuilderConfig builderConfig;

        QuotaControlPtr quotaControl = creator.createMemoryQuotaControl(buildTotalMemory, builderConfig);
        ASSERT_TRUE(quotaControl);
        ASSERT_EQ((size_t)1000 * 1024 * 1024, quotaControl->GetTotalQuota());
        ASSERT_EQ((size_t)1000 * 1024 * 1024, quotaControl->GetLeftQuota());
    }
}

TEST_F(BuilderCreatorTest, testCreateWithReservedCheckpoints)
{
    BuilderCreator creator;
    PartitionId partId;
    *partId.add_clusternames() = "simple";
    partId.mutable_buildid()->set_datatable("simple");
    partId.mutable_buildid()->set_generationid(0);

    KeyValueMap kvMap;
    kvMap[INDEX_ROOT_PATH] = GET_TEMP_DATA_PATH();
    kvMap[ZOOKEEPER_ROOT] = GET_TEMP_DATA_PATH();
    kvMap[BUILDER_EPOCH_ID] = EpochIdUtil::TEST_GenerateEpochId();

    // prepare checkpoints
    CheckpointList ccpList;
    ccpList.add(1);
    ccpList.add(3);
    ccpList.add(5);
    string ccpJsonStr = ToJsonString(ccpList);
    kvMap[RESERVED_CLUSTER_CHECKPOINT_LIST] = ccpJsonStr;

    {
        Builder* builder = creator.create(_resourceReader, kvMap, partId);
        ASSERT_TRUE(builder);
        IndexPartitionOptions options = builder->_indexBuilder->mOptions;
        ASSERT_EQ(ccpList.getIdSet(), options.GetReservedVersions());
        DELETE_AND_SET_NULL(builder);
    }

    kvMap[RESERVED_VERSION_LIST] = "1 2 3";
    {
        Builder* builder = creator.create(_resourceReader, kvMap, partId);
        ASSERT_TRUE(builder);
        IndexPartitionOptions options = builder->_indexBuilder->mOptions;
        set<versionid_t> reservedVersions = {1, 2, 3};
        ASSERT_EQ(reservedVersions, options.GetReservedVersions());
        DELETE_AND_SET_NULL(builder);
    }
}

}} // namespace build_service::builder
