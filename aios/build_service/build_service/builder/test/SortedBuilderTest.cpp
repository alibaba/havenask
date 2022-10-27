#include "build_service/test/unittest.h"
#include "build_service/builder/SortedBuilder.h"
#include "build_service/builder/test/MockIndexBuilder.h"
#include "build_service/document/test/DocumentTestHelper.h"
#include "build_service/util/test/FileUtilForTest.h"
#include "build_service/builder/test/IndexBuilderCreator.h"
#include <indexlib/config/index_partition_schema_maker.h>
#include <indexlib/misc/exception.h>
#include <indexlib/util/memory_control/quota_control.h>
#include <indexlib/util/counter/counter_map.h>
#include <indexlib/index_base/index_meta/segment_info.h>
#include <autil/StringUtil.h>
#include "FakeThread.h"

using namespace std;
using namespace autil;
using namespace testing;
using namespace fslib::fs;
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index);
using namespace build_service::document;
using namespace build_service::util;
using namespace build_service::config;

namespace build_service {
namespace builder {

class SortedBuilderTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
private:
    SortedBuilderPtr createSortedBuilder(
            bool asyncBuild = false, bool useMock = false) const;
    static DocumentPtr createDocument(uint32_t sortField, DocOperateType opType = ADD_DOC);
    static IndexBuilderPtr createIndexBuilder(const string &rootDir, bool useMock = false);
    void CheckSegment(uint32_t segmentId, uint32_t docCount);

private:
    string _rootDir;
    IndexPartitionSchemaPtr _schema;
};

void SortedBuilderTest::setUp() {
    _rootDir = GET_TEST_DATA_PATH();
    _schema.reset(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(_schema,
            //Field schema
            "int:INT32",
            //Primary key index schema
            "pk:primarykey64:int",
            //Attribute schema
            "int",
            //Summary schema
            "" );
}

void SortedBuilderTest::tearDown() {
}

DocumentPtr SortedBuilderTest::createDocument(uint32_t sortField, DocOperateType opType)
{
    FakeDocument fakeDoc(StringUtil::toString(sortField), opType,
                         true, true, 0, sortField);
    return DocumentTestHelper::createDocument(fakeDoc);
}

void SortedBuilderTest::CheckSegment(uint32_t segmentId, uint32_t docCount)
{
    SegmentInfo segInfo;
    stringstream segmentPath;
    segmentPath << _rootDir << "/segment_" << segmentId << "_level_0" << "/segment_info";
    segInfo.Load(segmentPath.str());
    ASSERT_EQ(docCount, segInfo.docCount);
}

SortedBuilderPtr SortedBuilderTest::createSortedBuilder(bool asyncBuild, bool useMock) const {
    SortedBuilderPtr builder(new SortedBuilder(createIndexBuilder(_rootDir, useMock), 1024));
    BuilderConfig config;
    config.sortBuild = true;
    config.sortQueueSize = 3;
    config.asyncQueueSize = 3;
    config.asyncBuild = asyncBuild;
    config.sortDescriptions.push_back(SortDescription("int", sp_desc));
    if (!builder->init(config)) {
        return SortedBuilderPtr();
    }
    return builder;
}

IndexBuilderPtr SortedBuilderTest::createIndexBuilder(const string &rootDir, bool useMock) {
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(schema,
            //Field schema
            "int:INT32",
            //Primary key index schema
            "pk:primarykey64:int",
            //Attribute schema
            "int",
            //Summary schema
            "" );
    if (useMock)
    {
        IndexPartitionOptions options;
        MockIndexBuilder* mockIndexBuilder = new NiceMockIndexBuilder(options, rootDir, schema);
        IndexBuilderPtr indexBuilder(mockIndexBuilder);
        indexBuilder->Init();
        return indexBuilder;
    }
    return IndexBuilderCreator::CreateIndexBuilder(rootDir, schema);
 }

TEST_F(SortedBuilderTest, testSimpleSortBuildSpeedUp) {
    SortedBuilderPtr builder = createSortedBuilder(true, true);
    IndexBuilderPtr indexBuilder = builder->_indexBuilder;
    MockIndexBuilder* mockIndexBuilder = dynamic_cast<MockIndexBuilder*>(indexBuilder.get());
    EXPECT_CALL(*mockIndexBuilder, Build(_))
        .Times(4);
    ASSERT_TRUE(builder);
    builder->build(createDocument(1));
    builder->build(createDocument(0));
    builder->build(createDocument(3));
    builder->build(createDocument(2));
    builder->stop();
    EXPECT_FALSE(builder->hasFatalError());
}

TEST_F(SortedBuilderTest, testBug8214135) {
    SortedBuilderPtr builder(new SortedBuilder(
                    createIndexBuilder(_rootDir, false), 1024));
    BuilderConfig config;
    config.sortBuild = true;
    config.sortQueueSize = 2;
    config.asyncQueueSize = 2;
    config.asyncBuild = true;
    config.sleepPerdoc = 1;
    config.sortDescriptions.push_back(SortDescription("int", sp_desc));
    builder->init(config, NULL);

    builder->build(createDocument(1));
    builder->build(createDocument(0));

    usleep(4 * 1000 * 1000);
    CheckSegment(0, 2);
    builder->stop();
}

TEST_F(SortedBuilderTest, testInitBuilder) {
    {
        BuilderConfig builderConfig;
        SortDescription desc1;
        builderConfig.sortDescriptions.push_back(SortDescription("not_exist", sp_desc));
        SortedBuilderPtr builder(new SortedBuilder(createIndexBuilder(_rootDir, true), 1024));
        ASSERT_TRUE(builder->initBuilder(builderConfig));
        ASSERT_FALSE(builder->_asyncBuilder);
    }

    {
        BuilderConfig builderConfig;
        builderConfig.asyncBuild = true;
        SortDescription desc1;
        builderConfig.sortDescriptions.push_back(SortDescription("not_exist", sp_desc));
        SortedBuilderPtr builder(new SortedBuilder(createIndexBuilder(_rootDir, true), 1024));
        ASSERT_TRUE(builder->initBuilder(builderConfig));
        ASSERT_TRUE(builder->_asyncBuilder);
    }
}

TEST_F(SortedBuilderTest, testInitConfig) {
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

TEST_F(SortedBuilderTest, testNeedFlush) {
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

TEST_F(SortedBuilderTest, testBuildOneDocWithAsyncBuilder) {
    SortedBuilderPtr builder = createSortedBuilder(true, true);
    IndexBuilderPtr indexBuilder = builder->_indexBuilder;
    MockIndexBuilder* mockIndexBuilder = dynamic_cast<MockIndexBuilder*>(indexBuilder.get());
    EXPECT_CALL(*mockIndexBuilder, Build(_))
        .WillOnce(Return(true));
    ASSERT_TRUE(builder->buildOneDoc(createDocument(0)));
    sleep(1);
    EXPECT_CALL(*mockIndexBuilder, Build(_))
        .WillOnce(Throw(IE_NAMESPACE(misc)::FileIOException()));
    ASSERT_TRUE(builder->buildOneDoc(createDocument(1)));
    sleep(1);
    ASSERT_TRUE(builder->hasFatalError());
    ASSERT_FALSE(builder->buildOneDoc(createDocument(2)));
    ASSERT_TRUE(builder->hasFatalError());
}

TEST_F(SortedBuilderTest, testInitSortDocumentContainer) {
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(schema,
            //Field schema
            "int:INT32;uint:UINT32;text1:string",
            //Primary key index schema
            "pk:primarykey64:text1",
            //Attribute schema
            "int;uint",
            //Summary schema
            "" );
    IndexPartitionOptions options;
    IndexBuilderPtr indexBuilder(new NiceMockIndexBuilder(options, _rootDir, schema));
    indexBuilder->Init();
    {
        SortedBuilder builder(indexBuilder, 1024);
        BuilderConfig builderConfig;
        SortDescription desc1;
        builderConfig.sortDescriptions.push_back(SortDescription("not_exist", sp_desc));
        EXPECT_FALSE(builder.initSortContainers(builderConfig, schema));
    }
    {
        SortedBuilder builder(indexBuilder, 1024);
        BuilderConfig builderConfig;
        builderConfig.sortDescriptions.push_back(SortDescription("int", sp_desc));
        builderConfig.sortDescriptions.push_back(SortDescription("uint", sp_asc));
        EXPECT_TRUE(builder.initSortContainers(builderConfig, schema));

        NormalSortDocConvertor* sortDocConvertor =
            dynamic_cast<NormalSortDocConvertor*>(
                builder._buildContainer._converter.get());
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
        builderConfig.sortDescriptions.push_back(SortDescription("uint", sp_asc));
        EXPECT_TRUE(builder.initSortContainers(builderConfig, schema));

        NormalSortDocConvertor* sortDocConvertor =
            dynamic_cast<NormalSortDocConvertor*>(
                builder._buildContainer._converter.get());
        ASSERT_TRUE(sortDocConvertor);
        EXPECT_EQ(size_t(1), sortDocConvertor ->_fieldIds.size());
        EXPECT_FALSE(builder._buildContainer._hasPrimaryKey);
        EXPECT_TRUE(builder._buildContainer._hasSub);
    }
}

TEST_F(SortedBuilderTest, testStop) {
    SortedBuilderPtr builder = createSortedBuilder();
    ASSERT_TRUE(builder);
    builder->build(createDocument(0));
    builder->stop();
    builder->stop();
    EXPECT_FALSE(builder->hasFatalError());
}

TEST_F(SortedBuilderTest, testTryDump) {
    SortedBuilderPtr builder = createSortedBuilder();
}

ACTION_P(SLEEP_MS, ms) {
    usleep(ms * 1000);
    return true;
}

TEST_F(SortedBuilderTest, testBuildWhenQueueNotFull) {
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
    while(!FileUtilForTest::checkIsDir(FileUtil::joinFilePath(_rootDir, "segment_0_level_0")));
    ASSERT_TRUE(FileUtilForTest::checkIsFile(FileUtil::joinFilePath(_rootDir, "version.0")));
}

TEST_F(SortedBuilderTest, testBuildWhenQueueFull) {
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
    while(!FileUtilForTest::checkIsDir(FileUtil::joinFilePath(_rootDir, "segment_0_level_0")));
    // two segment
    builder->build(createDocument(3));
    ASSERT_FALSE(builder->_collectContainer.empty());
    builder->build(createDocument(4));
    ASSERT_FALSE(builder->_collectContainer.empty());
    builder->build(createDocument(5));
    ASSERT_TRUE(builder->_collectContainer.empty());
    while(!FileUtilForTest::checkIsDir(FileUtil::joinFilePath(_rootDir, "segment_1_level_0")));

    // segment size reach mem limit
    builder->_sortQueueMem = 1;
    builder->_sortQueueSize = 1000000;
    for (size_t i = 10; i < 1000 * 10; i++) {
        builder->build(createDocument(i));
    }
    ASSERT_FALSE(builder->_collectContainer.empty());
    while(!FileUtilForTest::checkIsDir(FileUtil::joinFilePath(_rootDir, "segment_2_level_0")));

    for (size_t i = 10000; i < 2000 * 10; i++) {
        builder->build(createDocument(i));
    }
    ASSERT_FALSE(builder->_collectContainer.empty());
    while(!FileUtilForTest::checkIsDir(FileUtil::joinFilePath(_rootDir, "segment_3_level_0")));

    builder->stop();
    ASSERT_TRUE(FileUtilForTest::checkIsDir(FileUtil::joinFilePath(_rootDir, "segment_4_level_0")));
    ASSERT_TRUE(FileUtilForTest::checkIsFile(FileUtil::joinFilePath(_rootDir, "version.0")));
}

TEST_F(SortedBuilderTest, testBuildException) {
    {
        SortedBuilderPtr builder = createSortedBuilder();
        builder->setFatalError();
        ASSERT_FALSE(builder->build(createDocument(0)));
        ASSERT_TRUE(builder->_collectContainer.empty());
    }
    {
        SortedBuilderPtr builder = createSortedBuilder();
        ASSERT_TRUE(FileUtil::remove(_rootDir));
        ASSERT_TRUE(builder->build(createDocument(0)));
        ASSERT_TRUE(builder->build(createDocument(1)));
        ASSERT_TRUE(builder->build(createDocument(2)));
        sleep(1);
        ASSERT_TRUE(builder->build(createDocument(3)));
        EXPECT_FALSE(builder->hasFatalError());
    }
    {
        ASSERT_TRUE(FileUtil::remove(_rootDir));
        SortedBuilderPtr builder = createSortedBuilder();
        ASSERT_TRUE(builder->build(createDocument(0)));
        ASSERT_TRUE(builder->build(createDocument(1)));
        ASSERT_TRUE(FileUtil::remove(_rootDir));
        builder->stop();
        EXPECT_FALSE(builder->hasFatalError());
    }
}

TEST_F(SortedBuilderTest, testReserveMemoryQuota) {
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

}
}
