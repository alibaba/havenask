#include "build_service/builder/SortedBuilder.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <ostream>
#include <stddef.h>
#include <string>
#include <unistd.h>
#include <vector>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "build_service/builder/NormalSortDocConvertor.h"
#include "build_service/builder/SortDocumentContainer.h"
#include "build_service/builder/SortDocumentConverter.h"
#include "build_service/builder/test/IndexBuilderCreator.h"
#include "build_service/builder/test/MockIndexBuilder.h"
#include "build_service/config/BuilderConfig.h"
#include "build_service/document/DocumentDefine.h"
#include "build_service/document/test/DocumentTestHelper.h"
#include "build_service/test/unittest.h"
#include "build_service/util/ErrorLogCollector.h"
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
#include "indexlib/config/region_schema.h"
#include "indexlib/document/index_locator.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/attribute/Constant.h"
#include "indexlib/index/kv/Constant.h"
#include "indexlib/index/primary_key/PrimaryKeyIndexReader.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/builder_branch_hinter.h"
#include "indexlib/partition/index_builder.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/partition/table_reader_container_updater.h"
#include "indexlib/util/ErrorLogCollector.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/memory_control/QuotaControl.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace testing;
using namespace fslib;
using namespace fslib::fs;
using namespace indexlib::document;
using namespace indexlib::index_base;
using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::partition;
using namespace indexlib::util;
using namespace indexlib::index;
using namespace build_service::document;
using namespace build_service::util;
using namespace build_service::config;

namespace build_service { namespace builder {

class SortedBuilderTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();

private:
    SortedBuilderPtr createSortedBuilder(bool asyncBuild = false, bool useMock = false) const;
    static DocumentPtr createDocument(uint32_t sortField, DocOperateType opType = ADD_DOC, bool isNull = false);
    static IndexBuilderPtr createIndexBuilder(const string& rootDir, bool useMock = false);
    void CheckSegment(uint32_t segmentId, uint32_t docCount);

private:
    string _rootDir;
    IndexPartitionSchemaPtr _schema;
};

void SortedBuilderTest::setUp()
{
    _rootDir = GET_TEMP_DATA_PATH();
    _schema.reset(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(_schema,
                                          // Field schema
                                          "int:INT32",
                                          // Primary key index schema
                                          "pk:primarykey64:int",
                                          // Attribute schema
                                          "int",
                                          // Summary schema
                                          "");
}

void SortedBuilderTest::tearDown() {}

DocumentPtr SortedBuilderTest::createDocument(uint32_t sortField, DocOperateType opType, bool isNull)
{
    FakeDocument fakeDoc(StringUtil::toString(sortField), opType, true, true, 0, sortField, 0, 0, isNull);
    return DocumentTestHelper::createDocument(fakeDoc);
}

void SortedBuilderTest::CheckSegment(uint32_t segmentId, uint32_t docCount)
{
    SegmentInfo segInfo;
    stringstream segmentPath;
    segmentPath << _rootDir << "/segment_" << segmentId << "_level_0";
    auto segmentDir = indexlib::file_system::Directory::GetPhysicalDirectory(segmentPath.str());
    segInfo.Load(segmentDir);
    ASSERT_EQ(docCount, segInfo.docCount);
}

SortedBuilderPtr SortedBuilderTest::createSortedBuilder(bool asyncBuild, bool useMock) const
{
    SortedBuilderPtr builder(new SortedBuilder(createIndexBuilder(_rootDir, useMock), 1024));
    BuilderConfig config;
    config.sortBuild = true;
    config.sortQueueSize = 3;
    config.asyncQueueSize = 3;
    config.asyncBuild = asyncBuild;
    config.sortDescriptions.push_back(indexlibv2::config::SortDescription("int", indexlibv2::config::sp_desc));
    if (!builder->init(config)) {
        return SortedBuilderPtr();
    }
    return builder;
}

IndexBuilderPtr SortedBuilderTest::createIndexBuilder(const string& rootDir, bool useMock)
{
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(schema,
                                          // Field schema
                                          "int:INT32",
                                          // Primary key index schema
                                          "pk:primarykey64:int",
                                          // Attribute schema
                                          "int",
                                          // Summary schema
                                          "");
    if (useMock) {
        IndexPartitionOptions options;
        MockIndexBuilder* mockIndexBuilder = new NiceMockIndexBuilder(options, rootDir, schema);
        IndexBuilderPtr indexBuilder(mockIndexBuilder);
        indexBuilder->Init();
        return indexBuilder;
    }
    return IndexBuilderCreator::CreateIndexBuilder(rootDir, schema);
}

TEST_F(SortedBuilderTest, testSimpleSortBuildSpeedUp)
{
    SortedBuilderPtr builder = createSortedBuilder(true, true);
    IndexBuilderPtr indexBuilder = builder->_indexBuilder;
    MockIndexBuilder* mockIndexBuilder = dynamic_cast<MockIndexBuilder*>(indexBuilder.get());
    EXPECT_CALL(*mockIndexBuilder, Build(_)).Times(4);
    ASSERT_TRUE(builder);
    builder->build(createDocument(1));
    builder->build(createDocument(0));
    builder->build(createDocument(3));
    builder->build(createDocument(2));
    builder->stop();
    EXPECT_FALSE(builder->hasFatalError());
}

TEST_F(SortedBuilderTest, testBug8214135)
{
    SortedBuilderPtr builder(new SortedBuilder(createIndexBuilder(_rootDir, false), 1024));
    BuilderConfig config;
    config.sortBuild = true;
    config.sortQueueSize = 2;
    config.asyncQueueSize = 2;
    config.asyncBuild = true;
    config.sleepPerdoc = 1;
    config.sortDescriptions.push_back(indexlibv2::config::SortDescription("int", indexlibv2::config::sp_desc));
    builder->init(config, NULL);

    builder->build(createDocument(1));
    builder->build(createDocument(0));

    usleep(6 * 1000 * 1000);
    builder->stop();
    builder->_indexBuilder->TEST_BranchFSMoveToMainRoad();
    CheckSegment(0, 2);
}

TEST_F(SortedBuilderTest, testBuildWithNull)
{
    SortedBuilderPtr builder(new SortedBuilder(createIndexBuilder(_rootDir, false), 1024));
    BuilderConfig config;
    config.sortBuild = true;
    config.sortQueueSize = 4;
    config.asyncQueueSize = 4;
    config.asyncBuild = true;
    config.sleepPerdoc = 1;
    config.sortDescriptions.push_back(indexlibv2::config::SortDescription("int", indexlibv2::config::sp_desc));
    builder->init(config, NULL);

    builder->build(createDocument(1, ADD_DOC, true));
    builder->build(createDocument(3, ADD_DOC, true));
    builder->build(createDocument(2, ADD_DOC, false));
    builder->build(createDocument(4, ADD_DOC, false));
    builder->stop();

    builder->_indexBuilder->TEST_BranchFSMoveToMainRoad();
    CheckSegment(0, 4);
    IndexPartitionPtr indexPartition(new OnlinePartition());
    IndexPartitionOptions options;
    indexPartition->Open(_rootDir, "", _schema, options);
    auto indexReader = indexPartition->GetReader()->GetPrimaryKeyReader();
    EXPECT_EQ(0, indexReader->Lookup("4"));
    EXPECT_EQ(1, indexReader->Lookup("2"));
    EXPECT_EQ(2, indexReader->Lookup("1"));
    EXPECT_EQ(3, indexReader->Lookup("3"));
}

TEST_F(SortedBuilderTest, testBuildWithEmpty)
{
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(schema,
                                          // Field schema
                                          "int:INT32;string:STRING",
                                          // Primary key index schema
                                          "pk:primarykey64:string",
                                          // Attribute schema
                                          "int",
                                          // Summary schema
                                          "");
    auto indexBuilder = IndexBuilderCreator::CreateIndexBuilder(_rootDir, schema);
    SortedBuilderPtr builder(new SortedBuilder(indexBuilder, 1024));
    BuilderConfig config;
    config.sortBuild = true;
    config.sortQueueSize = 4;
    config.asyncQueueSize = 4;
    config.asyncBuild = true;
    config.sleepPerdoc = 1;
    config.sortDescriptions.push_back(indexlibv2::config::SortDescription("int", indexlibv2::config::sp_asc));
    builder->init(config, NULL);

    auto innerCreateDocument = [](string pk, std::optional<uint32_t> sortField, bool isNull) -> DocumentPtr {
        uint32_t sortValue = sortField == nullopt ? 0 : sortField.value();
        FakeDocument fakeDoc(pk, ADD_DOC, true, true, 0, sortValue, 0, 0, isNull);
        if (sortField == nullopt && !isNull) {
            fakeDoc._attributes.back().second = "";
        }
        return DocumentTestHelper::createDocument(fakeDoc);
    };
    // null < empty < 10 < 20
    builder->build(innerCreateDocument("1", 10, false));
    builder->build(innerCreateDocument("2", 20, false));
    builder->build(innerCreateDocument("3", nullopt, false));
    builder->build(innerCreateDocument("4", 30, true));
    builder->stop();

    builder->_indexBuilder->TEST_BranchFSMoveToMainRoad();
    CheckSegment(0, 4);
    IndexPartitionPtr indexPartition(new OnlinePartition());
    IndexPartitionOptions options;
    indexPartition->Open(_rootDir, "", schema, options);
    auto indexReader = indexPartition->GetReader()->GetPrimaryKeyReader();
    EXPECT_EQ(2, indexReader->Lookup("1"));
    EXPECT_EQ(3, indexReader->Lookup("2"));
    EXPECT_EQ(1, indexReader->Lookup("3"));
    EXPECT_EQ(0, indexReader->Lookup("4"));
}

TEST_F(SortedBuilderTest, testInitBuilder)
{
    {
        BuilderConfig builderConfig;
        indexlibv2::config::SortDescription desc1;
        builderConfig.sortDescriptions.push_back(
            indexlibv2::config::SortDescription("not_exist", indexlibv2::config::sp_desc));
        SortedBuilderPtr builder(new SortedBuilder(createIndexBuilder(_rootDir, true), 1024));
        ASSERT_TRUE(builder->initBuilder(builderConfig));
        ASSERT_FALSE(builder->_asyncBuilder);
    }

    {
        BuilderConfig builderConfig;
        builderConfig.asyncBuild = true;
        indexlibv2::config::SortDescription desc1;
        builderConfig.sortDescriptions.push_back(
            indexlibv2::config::SortDescription("not_exist", indexlibv2::config::sp_desc));
        SortedBuilderPtr builder(new SortedBuilder(createIndexBuilder(_rootDir, true), 1024));
        ASSERT_TRUE(builder->initBuilder(builderConfig));
        ASSERT_TRUE(builder->_asyncBuilder);
    }
}

TEST_F(SortedBuilderTest, testInitConfig)
{
    {
        // use default sortQueueMem ( buildTotalMem * 0.3 )
        BuilderConfig builderConfig;
        int64_t buildTotalMem = 1000;
        builderConfig.sortQueueSize = 20;
        SortedBuilderPtr builder(new SortedBuilder(createIndexBuilder(_rootDir, true), buildTotalMem));
        builder->initConfig(builderConfig);
        ASSERT_EQ((int64_t)300, builder->_sortQueueMem);
        ASSERT_EQ((uint32_t)20, builder->_sortQueueSize);
    }
    {
        // use customized sortQueueMem
        BuilderConfig builderConfig;
        int64_t buildTotalMem = 1000;
        builderConfig.sortQueueMem = 100;
        builderConfig.sortQueueSize = 20;
        SortedBuilderPtr builder(new SortedBuilder(createIndexBuilder(_rootDir, true), buildTotalMem));
        builder->initConfig(builderConfig);
        ASSERT_EQ((int64_t)50, builder->_sortQueueMem);
        ASSERT_EQ((uint32_t)20, builder->_sortQueueSize);
    }
}

TEST_F(SortedBuilderTest, testNeedFlush)
{
    {
        SortedBuilderPtr builder(new SortedBuilder(createIndexBuilder(_rootDir, true), 1024));
        builder->_sortQueueMem = 100;
        builder->_sortQueueSize = 1000;
        ASSERT_TRUE(builder->needFlush(100, 10));
        ASSERT_TRUE(builder->needFlush(101, 10));
        ASSERT_TRUE(builder->needFlush(10, 1000));
        ASSERT_TRUE(builder->needFlush(10, 1001));
        ASSERT_FALSE(builder->needFlush(10, 10));
    }
}

TEST_F(SortedBuilderTest, testBuildOneDocWithAsyncBuilder)
{
    SortedBuilderPtr builder = createSortedBuilder(true, true);
    IndexBuilderPtr indexBuilder = builder->_indexBuilder;
    MockIndexBuilder* mockIndexBuilder = dynamic_cast<MockIndexBuilder*>(indexBuilder.get());
    EXPECT_CALL(*mockIndexBuilder, Build(_)).WillOnce(Return(true));
    ASSERT_TRUE(builder->buildOneDoc(createDocument(0)));
    sleep(1);
    EXPECT_CALL(*mockIndexBuilder, Build(_)).WillOnce(Throw(indexlib::util::FileIOException()));
    ASSERT_TRUE(builder->buildOneDoc(createDocument(1)));
    sleep(1);
    ASSERT_TRUE(builder->hasFatalError());
    ASSERT_FALSE(builder->buildOneDoc(createDocument(2)));
    ASSERT_TRUE(builder->hasFatalError());
}

TEST_F(SortedBuilderTest, testInitSortDocumentContainer)
{
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(schema,
                                          // Field schema
                                          "int:INT32;uint:UINT32;text1:string",
                                          // Primary key index schema
                                          "pk:primarykey64:text1",
                                          // Attribute schema
                                          "int;uint",
                                          // Summary schema
                                          "");
    IndexPartitionOptions options;
    IndexBuilderPtr indexBuilder(new NiceMockIndexBuilder(options, _rootDir, schema));
    indexBuilder->Init();
    {
        SortedBuilder builder(indexBuilder, 1024);
        BuilderConfig builderConfig;
        indexlibv2::config::SortDescription desc1;
        builderConfig.sortDescriptions.push_back(
            indexlibv2::config::SortDescription("not_exist", indexlibv2::config::sp_desc));
        EXPECT_FALSE(builder.initSortContainers(builderConfig, schema));
    }
    {
        SortedBuilder builder(indexBuilder, 1024);
        BuilderConfig builderConfig;
        builderConfig.sortDescriptions.push_back(
            indexlibv2::config::SortDescription("int", indexlibv2::config::sp_desc));
        builderConfig.sortDescriptions.push_back(
            indexlibv2::config::SortDescription("uint", indexlibv2::config::sp_asc));
        EXPECT_TRUE(builder.initSortContainers(builderConfig, schema));

        NormalSortDocConvertor* sortDocConvertor =
            dynamic_cast<NormalSortDocConvertor*>(builder._buildContainer._converter.get());
        ASSERT_TRUE(sortDocConvertor);
        EXPECT_EQ(size_t(2), sortDocConvertor->_fieldIds.size());
        EXPECT_TRUE(builder._buildContainer._hasPrimaryKey);
        EXPECT_FALSE(builder._buildContainer._hasSub);
    }
    {
        IndexPartitionSchemaPtr subSchema(schema->Clone());
        schema->SetSubIndexPartitionSchema(subSchema);
        schema->GetRegionSchema(DEFAULT_REGIONID)->ResetIndexSchema();
        SortedBuilder builder(indexBuilder, 1024);
        BuilderConfig builderConfig;
        builderConfig.sortDescriptions.push_back(
            indexlibv2::config::SortDescription("uint", indexlibv2::config::sp_asc));
        EXPECT_TRUE(builder.initSortContainers(builderConfig, schema));

        NormalSortDocConvertor* sortDocConvertor =
            dynamic_cast<NormalSortDocConvertor*>(builder._buildContainer._converter.get());
        ASSERT_TRUE(sortDocConvertor);
        EXPECT_EQ(size_t(1), sortDocConvertor->_fieldIds.size());
        EXPECT_FALSE(builder._buildContainer._hasPrimaryKey);
        EXPECT_TRUE(builder._buildContainer._hasSub);
    }
}

TEST_F(SortedBuilderTest, testStop)
{
    SortedBuilderPtr builder = createSortedBuilder();
    ASSERT_TRUE(builder);
    builder->build(createDocument(0));
    builder->stop();
    builder->stop();
    EXPECT_FALSE(builder->hasFatalError());
}

TEST_F(SortedBuilderTest, testTryDump) { SortedBuilderPtr builder = createSortedBuilder(); }

ACTION_P(SLEEP_MS, ms)
{
    usleep(ms * 1000);
    return true;
}

TEST_F(SortedBuilderTest, testBuildWhenQueueNotFull)
{
    SortedBuilderPtr builder = createSortedBuilder();
    ASSERT_TRUE(builder);
    builder->build(createDocument(0));
    builder->build(createDocument(1));
    ASSERT_FALSE(builder->_collectContainer.empty());
    ASSERT_TRUE(builder->_buildContainer.empty());
    builder->flush();
    ASSERT_TRUE(builder->_collectContainer.empty());
    builder->stop();
    sleep(2);
    builder->_indexBuilder->TEST_BranchFSMoveToMainRoad();
    while (EC_TRUE != FileSystem::isDirectory(FileSystem::joinFilePath(_rootDir, "segment_0_level_0")))
        ;
    ASSERT_EQ(EC_TRUE, FileSystem::isFile(FileSystem::joinFilePath(_rootDir, "version.0")));
}

TEST_F(SortedBuilderTest, testBuildWhenQueueFull)
{
    // sortQueueSize == 3
    SortedBuilderPtr builder = createSortedBuilder();
    ASSERT_TRUE(builder);
    // one segment
    builder->build(createDocument(0));
    ASSERT_FALSE(builder->_collectContainer.empty());
    builder->build(createDocument(1));
    ASSERT_FALSE(builder->_collectContainer.empty());
    builder->build(createDocument(2));
    ASSERT_TRUE(builder->_collectContainer.empty());
    string branchName = builder->_indexBuilder->GetBranchFs()->GetBranchName();
    string rootDir = FileSystem::joinFilePath(_rootDir, branchName);
    while (EC_TRUE != FileSystem::isDirectory(FileSystem::joinFilePath(rootDir, "segment_0_level_0")))
        ;

    // two segment
    builder->build(createDocument(3));
    ASSERT_FALSE(builder->_collectContainer.empty());
    builder->build(createDocument(4));
    ASSERT_FALSE(builder->_collectContainer.empty());
    builder->build(createDocument(5));
    ASSERT_TRUE(builder->_collectContainer.empty());

    while (EC_TRUE != FileSystem::isDirectory(FileSystem::joinFilePath(rootDir, "segment_1_level_0")))
        ;

    // segment size reach mem limit
    builder->_sortQueueMem = 1;
    builder->_sortQueueSize = 1000000;
    for (size_t i = 10; i < 1000 * 10; i++) {
        builder->build(createDocument(i));
    }
    ASSERT_FALSE(builder->_collectContainer.empty());
    while (EC_TRUE != FileSystem::isDirectory(FileSystem::joinFilePath(rootDir, "segment_2_level_0")))
        ;

    for (size_t i = 10000; i < 2000 * 10; i++) {
        builder->build(createDocument(i));
    }
    ASSERT_FALSE(builder->_collectContainer.empty());
    while (EC_TRUE != FileSystem::isDirectory(FileSystem::joinFilePath(rootDir, "segment_3_level_0")))
        ;

    builder->stop();
    builder->_indexBuilder->TEST_BranchFSMoveToMainRoad();
    ASSERT_EQ(EC_TRUE, FileSystem::isDirectory(FileSystem::joinFilePath(_rootDir, "segment_4_level_0")));
    ASSERT_EQ(EC_TRUE, FileSystem::isFile(FileSystem::joinFilePath(_rootDir, "version.0")));
}

TEST_F(SortedBuilderTest, testBuildException)
{
    {
        SortedBuilderPtr builder = createSortedBuilder();
        builder->setFatalError();
        ASSERT_FALSE(builder->build(createDocument(0)));
        ASSERT_TRUE(builder->_collectContainer.empty());
    }
    {
        SortedBuilderPtr builder = createSortedBuilder();
        ASSERT_TRUE(fslib::util::FileUtil::remove(_rootDir));
        ASSERT_TRUE(builder->build(createDocument(0)));
        ASSERT_TRUE(builder->build(createDocument(1)));
        ASSERT_TRUE(builder->build(createDocument(2)));
        sleep(1);
        ASSERT_TRUE(builder->build(createDocument(3)));
        EXPECT_FALSE(builder->hasFatalError());
    }
    {
        ASSERT_TRUE(fslib::util::FileUtil::remove(_rootDir));
        SortedBuilderPtr builder = createSortedBuilder();
        ASSERT_TRUE(builder->build(createDocument(0)));
        ASSERT_TRUE(builder->build(createDocument(1)));
        ASSERT_TRUE(fslib::util::FileUtil::remove(_rootDir));
        builder->stop();
        EXPECT_FALSE(builder->hasFatalError());
    }
}

TEST_F(SortedBuilderTest, testReserveMemoryQuota)
{
    {
        // no sortQueueMem in builderConfig, calculate from buildTotalMemMB
        BuilderConfig builderConfig;
        size_t buildTotalMemMB = 1000;
        QuotaControlPtr quotaControl(new QuotaControl(buildTotalMemMB * 1024 * 1024));
        ASSERT_TRUE(SortedBuilder::reserveMemoryQuota(builderConfig, buildTotalMemMB, quotaControl));
        ASSERT_EQ((size_t)400 * 1024 * 1024, quotaControl->GetLeftQuota());
    }
    {
        // has sortQueueMem in builderConfig,
        BuilderConfig builderConfig;
        builderConfig.sortQueueMem = 400;
        size_t buildTotalMemMB = 1000;
        QuotaControlPtr quotaControl(new QuotaControl(buildTotalMemMB * 1024 * 1024));
        ASSERT_TRUE(SortedBuilder::reserveMemoryQuota(builderConfig, buildTotalMemMB, quotaControl));
        ASSERT_EQ((size_t)600 * 1024 * 1024, quotaControl->GetLeftQuota());
    }
    {
        // exception : not enough quota in QuotaControl
        BuilderConfig builderConfig;
        size_t buildTotalMemMB = 1000;
        QuotaControlPtr quotaControl(new QuotaControl(buildTotalMemMB * 1024 * 1024));
        quotaControl->AllocateQuota(900 * 1024 * 1024);
        ASSERT_FALSE(SortedBuilder::reserveMemoryQuota(builderConfig, buildTotalMemMB, quotaControl));
        ASSERT_EQ((size_t)100 * 1024 * 1024, quotaControl->GetLeftQuota());
    }
}

}} // namespace build_service::builder
