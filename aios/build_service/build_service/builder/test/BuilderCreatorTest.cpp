#include "build_service/test/unittest.h"
#include "build_service/builder/BuilderCreator.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/BuildRuleConfig.h"
#include "build_service/util/test/FileUtilForTest.h"
#include "build_service/util/IndexPathConstructor.h"
#include "build_service/util/RangeUtil.h"
#include "build_service/builder/SortedBuilder.h"
#include "build_service/common/CheckpointList.h"
#include <indexlib/partition/index_builder.h>
#include <indexlib/partition/index_partition.h>
#include <indexlib/partition/online_partition.h>
#include <indexlib/partition/offline_partition.h>
#include <indexlib/util/memory_control/quota_control.h>
#include <indexlib/config/index_partition_schema_maker.h>
#include <autil/legacy/any_jsonizable.h>

using namespace std;
using namespace autil;
using namespace autil::legacy;
using testing::ElementsAre;

IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(util);

using namespace build_service::util;
using namespace build_service::proto;
using namespace build_service::config;
using namespace build_service::common;
namespace build_service {
namespace builder {

class MockBuilderCreator : public BuilderCreator {
public:
    MockBuilderCreator(const IndexPartitionPtr &indexPart = IndexPartitionPtr())
        : BuilderCreator(indexPart)
    {
    }
    ~MockBuilderCreator() {}
public:
    MOCK_METHOD4(createIndexBuilder, IE_NAMESPACE(partition)::IndexBuilderPtr(
                    const config::ResourceReaderPtr &,
                    const BuilderClusterConfig &,
                    const QuotaControlPtr&,
                    IE_NAMESPACE(misc)::MetricProviderPtr));
    MOCK_METHOD3(createSortedBuilder, Builder*(const IndexBuilderPtr &, int64_t, fslib::fs::FileLock *));
    MOCK_METHOD2(createNormalBuilder, Builder*(const IndexBuilderPtr &, fslib::fs::FileLock *));
    MOCK_METHOD3(initBuilder, bool(Builder *, const BuilderConfig &,
                                   IE_NAMESPACE(misc)::MetricProviderPtr));
};

class BuilderCreatorTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
private:
    string _configPath;
    string _indexPath;
    ResourceReaderPtr _resourceReader;
    KeyValueMap _kvMap;
};

void BuilderCreatorTest::setUp() {
    _configPath = TEST_DATA_PATH "/buildCreatorTest/";
    _indexPath = GET_TEST_DATA_PATH();
    _resourceReader.reset(new ResourceReader(_configPath));
    _kvMap.clear();
    _kvMap[INDEX_ROOT_PATH] = _indexPath;
}

void BuilderCreatorTest::tearDown() {
}

TEST_F(BuilderCreatorTest, testCreateForIncParallelBuild) {
    BuilderCreator creator;
    PartitionId partId;
    *partId.add_clusternames() = "parallel";
    partId.mutable_buildid()->set_datatable("simple");
    partId.mutable_buildid()->set_generationid(0);

    BuildRuleConfig buildRuleConfig;
    _resourceReader->getClusterConfigWithJsonPath("parallel",
            "cluster_config.builder_rule_config", buildRuleConfig);
    uint32_t workerPathVersion = 2;
    KeyValueMap kvMap;
    kvMap[INDEX_ROOT_PATH] = GET_TEST_DATA_PATH();
    kvMap[ZOOKEEPER_ROOT] = GET_TEST_DATA_PATH();
    kvMap[WORKER_PATH_VERSION] = StringUtil::toString(workerPathVersion);
    kvMap[OPERATION_IDS] = "1,2,3";

    // full build
    Builder *builder = creator.create(_resourceReader, kvMap, partId);
    ASSERT_TRUE(builder);
    IndexPartitionOptions options = builder->_indexBuilder->TEST_GET_INDEX_PARTITION_OPTIONS();
    ASSERT_THAT(options.GetOngoingModifyOperationIds(), ElementsAre(1,2,3));

    string indexPath = IndexPathConstructor::constructIndexPath(_indexPath, partId);
    ASSERT_TRUE(FileUtilForTest::checkPathExist(indexPath));
    DELETE_AND_SET_NULL(builder);

    {
        kvMap[OPERATION_IDS] = "3,5";
        vector<proto::Range> ranges = util::RangeUtil::splitRange(
            0, 65535, buildRuleConfig.partitionCount, buildRuleConfig.incBuildParallelNum);
        // inc build
        // partitioncount = 1, prallelNum = 2
        *partId.mutable_range() = *ranges.rbegin();
        partId.set_step(BUILD_STEP_INC);
        builder = creator.create(_resourceReader, kvMap, partId);
        ASSERT_TRUE(builder);
        IndexPartitionOptions options =
            builder->_indexBuilder->TEST_GET_INDEX_PARTITION_OPTIONS();
        ASSERT_THAT(options.GetOngoingModifyOperationIds(), ElementsAre(3,5));

        ParallelBuildInfo expectParallInfo(buildRuleConfig.incBuildParallelNum,
                                           workerPathVersion, buildRuleConfig.incBuildParallelNum - 1);
        string parallelInstancePath = expectParallInfo.GetParallelInstancePath(indexPath);
        ASSERT_TRUE(FileUtilForTest::checkPathExist(parallelInstancePath));
        DELETE_AND_SET_NULL(builder);
    }
    {
        // test builder range does not match inc_build_parallel_num
        vector<proto::Range> ranges = util::RangeUtil::splitRange(
            0, 65535, buildRuleConfig.partitionCount, 1);
        // inc build
        // partitioncount = 1, inc_build_prallelNum = 2
        *partId.mutable_range() = *ranges.begin();
        partId.set_step(BUILD_STEP_INC);
        builder = creator.create(_resourceReader, kvMap, partId);
        ASSERT_TRUE(builder);
        ParallelBuildInfo expectParallInfo(1u, workerPathVersion, 0);
        string parallelInstancePath = expectParallInfo.GetParallelInstancePath(indexPath);
        ASSERT_TRUE(FileUtilForTest::checkPathExist(parallelInstancePath));
        DELETE_AND_SET_NULL(builder);
    }
    {   // test partition_count = 2, inc_build_parallel_num = 2
        _resourceReader->getClusterConfigWithJsonPath("parallel1",
                                                      "cluster_config.builder_rule_config", buildRuleConfig);
        vector<proto::Range> ranges = util::RangeUtil::splitRange(
            0, 65535, buildRuleConfig.partitionCount, 1);
        *partId.mutable_clusternames(0) = "parallel1";
        *partId.mutable_range() = *ranges.rbegin();

        partId.set_step(BUILD_STEP_INC);
        builder = creator.create(_resourceReader, kvMap, partId);
        ASSERT_TRUE(builder);
        ParallelBuildInfo expectParallInfo(1u, workerPathVersion, 0);
        string parallelInstancePath = expectParallInfo.GetParallelInstancePath(indexPath);
        ASSERT_TRUE(FileUtilForTest::checkPathExist(parallelInstancePath));
        DELETE_AND_SET_NULL(builder);
    }
}

TEST_F(BuilderCreatorTest, testCreateSuccess) {
    BuilderCreator creator;
    PartitionId partId;
    *partId.add_clusternames() = "simple";
    partId.mutable_buildid()->set_datatable("simple");
    partId.mutable_buildid()->set_generationid(0);

    KeyValueMap kvMap;
    kvMap[INDEX_ROOT_PATH] = GET_TEST_DATA_PATH();
    kvMap[ZOOKEEPER_ROOT] = GET_TEST_DATA_PATH();
    Builder *builder = creator.create(_resourceReader, kvMap, partId);
    ASSERT_TRUE(builder);
    ASSERT_TRUE(FileUtilForTest::checkPathExist(GET_TEST_DATA_PATH() + "/generation..simple.0.simple.0.65535.__lock__"));

    string indexPath = IndexPathConstructor::constructIndexPath(_indexPath, partId);
    IE_NAMESPACE(index_base)::PartitionMeta meta;
    EXPECT_NO_THROW(meta.Load(indexPath));
    ASSERT_EQ(size_t(1), meta.Size());
    IE_NAMESPACE(index_base)::SortDescription sortDesc;
    sortDesc.sortFieldName = "id";
    sortDesc.sortPattern = sp_desc;
    EXPECT_EQ(sortDesc, meta.GetSortDescription(0));

    DELETE_AND_SET_NULL(builder);
}

TEST_F(BuilderCreatorTest, testAddBuildPhase){
    BuilderCreator creator;
    PartitionId partId;
    partId.set_step(BUILD_STEP_INC);
    *partId.add_clusternames() = "simple";
    partId.mutable_buildid()->set_datatable("simple");
    partId.mutable_buildid()->set_generationid(0);
    KeyValueMap kvMap;
    kvMap[INDEX_ROOT_PATH] = GET_TEST_DATA_PATH();
    kvMap[ZOOKEEPER_ROOT] = GET_TEST_DATA_PATH();
    Builder *builder = creator.create(_resourceReader, kvMap, partId);
    ASSERT_TRUE(builder);
    //ASSERT_EQ(BuildConfig::BP_INC, builder->TEST_GET_BUILD_STEP());
}

TEST_F(BuilderCreatorTest, testBuilderClusterInitFailed) {
    BuilderCreator creator;
    PartitionId partId;
    string configPath = TEST_DATA_PATH"/buildCreatorTestFail";
    ResourceReaderPtr resourceReader(new ResourceReader(configPath));
    *partId.add_clusternames() = "simple";
    ASSERT_FALSE(creator.create(resourceReader, _kvMap, partId));
}

TEST_F(BuilderCreatorTest, testSortBuild) {
    IndexPartitionPtr indexPart = IndexPartitionPtr();
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(schema,
            "int:INT32", "pk:primarykey64:int", "int", "" );

    QuotaControlPtr quotaControl(new QuotaControl(1024));
    IndexPartitionPtr indexPart2(new OnlinePartition());
    indexPart2->mSchema = schema; //IndexBuilder constructor need schema;
    IndexBuilderPtr indexBuilder(new IndexBuilder(indexPart2,
                    quotaControl));
    MockBuilderCreator creator(indexPart);
    PartitionId partId;
    *partId.add_clusternames() = "simple";
    EXPECT_CALL(creator, createIndexBuilder(_, _, _, _)).WillOnce(Return(indexBuilder));
    EXPECT_CALL(creator, createSortedBuilder(_, _, _));
    EXPECT_CALL(creator, initBuilder(_, _, _)).WillOnce(Return(true));
    Builder *builder = creator.create(_resourceReader, _kvMap, partId);
    DELETE_AND_SET_NULL(builder);
}

TEST_F(BuilderCreatorTest, testRealtimeBuild) {
    IndexPartitionPtr indexPart(new OnlinePartition());
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(schema,
            "int:INT32", "pk:primarykey64:int", "int", "" );
    indexPart->mSchema = schema; //IndexBuilder constructor need schema;
    QuotaControlPtr quotaControl(new QuotaControl(1024));
    IndexBuilderPtr indexBuilder(new IndexBuilder(indexPart, quotaControl));
    MockBuilderCreator creator(indexPart);
    PartitionId partId;
    *partId.add_clusternames() = "simple";
    EXPECT_CALL(creator, createIndexBuilder(_, _, _, _)).WillOnce(Return(indexBuilder));
    EXPECT_CALL(creator, createNormalBuilder(_, _));
    EXPECT_CALL(creator, initBuilder(_, _, _)).WillOnce(Return(true));
    Builder *builder = creator.create(_resourceReader, _kvMap, partId);
    DELETE_AND_SET_NULL(builder);
}

TEST_F(BuilderCreatorTest, testIllegalClusterConfig) {
    BuilderCreator creator;
    PartitionId partId;
    *partId.add_clusternames() = "simple1";
    ASSERT_FALSE(creator.create(_resourceReader, _kvMap, partId));
}

TEST_F(BuilderCreatorTest, testIllegalSchema) {
    BuilderCreator creator;
    PartitionId partId;
    *partId.add_clusternames() = "simple2";
    ASSERT_FALSE(creator.create(_resourceReader, _kvMap, partId));
}

TEST_F(BuilderCreatorTest, testNoIndexPath) {
    BuilderCreator creator;
    KeyValueMap kvMap;
    PartitionId partId;
    *partId.add_clusternames() = "simple";
    ASSERT_FALSE(creator.create(_resourceReader, kvMap, partId));
}

TEST_F(BuilderCreatorTest, testPartitionMetaInCompatible) {
    BuilderCreator creator;

    PartitionId partId;
    *partId.add_clusternames() = "simple";

    string indexPath = IndexPathConstructor::constructIndexPath(_indexPath, partId);
    FileUtil::mkDir(indexPath, true);
    IE_NAMESPACE(index_base)::PartitionMeta meta;
    meta.Store(indexPath);

    Builder *builder = creator.create(_resourceReader, _kvMap, partId);
    ASSERT_FALSE(builder);
}

TEST_F(BuilderCreatorTest, testCreateLockWithNotExistPath) {
    BuilderCreator creator;
    KeyValueMap kvMap;
    kvMap[INDEX_ROOT_PATH] = GET_TEST_DATA_PATH();
    PartitionId partId;
    *partId.add_clusternames() = "simple";
    string zkRootPath = "unknownfsType://not_exist_path";
    kvMap[ZOOKEEPER_ROOT] = zkRootPath;
    Builder *builder = creator.create(_resourceReader, kvMap, partId);
    ASSERT_FALSE(builder);
}

TEST_F(BuilderCreatorTest, testZkRootIsEmpty) {
    BuilderCreator creator;
    KeyValueMap kvMap;
    kvMap[INDEX_ROOT_PATH] = GET_TEST_DATA_PATH();
    PartitionId partId;
    *partId.add_clusternames() = "simple";
    kvMap[ZOOKEEPER_ROOT] = "";
    Builder *builder = creator.create(_resourceReader, kvMap, partId);
    ASSERT_TRUE(builder);
}

TEST_F(BuilderCreatorTest, testCreateMemoryQuotaControl) {
    {
        // test for offline
        BuilderCreator creator;
        creator._indexPart.reset();

        int64_t buildTotalMemory = 1000;
        BuilderConfig builderConfig;

        // sort build
        builderConfig.sortBuild = true;
        QuotaControlPtr quotaControl = creator.createMemoryQuotaControl(
                buildTotalMemory, builderConfig);
        ASSERT_TRUE(quotaControl);
        ASSERT_EQ((size_t)1000 * 1024 * 1024, quotaControl->GetTotalQuota());
        ASSERT_EQ((size_t)400 * 1024 * 1024, quotaControl->GetLeftQuota());

        // not sort build
        builderConfig.sortBuild = false;
        quotaControl = creator.createMemoryQuotaControl(
                buildTotalMemory, builderConfig);
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

        QuotaControlPtr quotaControl = creator.createMemoryQuotaControl(
                buildTotalMemory, builderConfig);
        ASSERT_TRUE(quotaControl);
        ASSERT_EQ((size_t)1000 * 1024 * 1024, quotaControl->GetTotalQuota());
        ASSERT_EQ((size_t)1000 * 1024 * 1024, quotaControl->GetLeftQuota());
    }
}

TEST_F(BuilderCreatorTest, testCreateWithReservedCheckpoints) {
    BuilderCreator creator;
    PartitionId partId;
    *partId.add_clusternames() = "simple";
    partId.mutable_buildid()->set_datatable("simple");
    partId.mutable_buildid()->set_generationid(0);

    KeyValueMap kvMap;
    kvMap[INDEX_ROOT_PATH] = GET_TEST_DATA_PATH();
    kvMap[ZOOKEEPER_ROOT] = GET_TEST_DATA_PATH();

    //prepare checkpoints
    CheckpointList ccpList;
    ccpList.add(1);
    ccpList.add(3);
    ccpList.add(5);
    string ccpJsonStr = ToJsonString(ccpList);
    kvMap[RESERVED_CLUSTER_CHECKPOINT_LIST] = ccpJsonStr;

    {
        Builder *builder = creator.create(_resourceReader, kvMap, partId);
        ASSERT_TRUE(builder);
        IndexPartitionOptions options = builder->_indexBuilder->mOptions;
        ASSERT_EQ(ccpList.getIdSet(), options.GetReservedVersions());
        DELETE_AND_SET_NULL(builder);
    }

    kvMap[RESERVED_VERSION_LIST] = "1 2 3";
    {
        Builder *builder = creator.create(_resourceReader, kvMap, partId);
        ASSERT_TRUE(builder);
        IndexPartitionOptions options = builder->_indexBuilder->mOptions;
        set<versionid_t> reservedVersions = {1,2,3};
        ASSERT_EQ(reservedVersions, options.GetReservedVersions());
        DELETE_AND_SET_NULL(builder);
    }
}

}
}
