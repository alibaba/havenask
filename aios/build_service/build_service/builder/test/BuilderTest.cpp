#include "build_service/test/unittest.h"
#include "build_service/util/FileUtil.h"
#include "build_service/builder/Builder.h"
#include "build_service/config/BuilderConfig.h"
#include "build_service/document/test/DocumentTestHelper.h"
#include <indexlib/partition/index_builder.h>
#include <indexlib/util/memory_control/quota_control.h>
#include <indexlib/config/index_partition_schema.h>
#include <indexlib/config/index_partition_schema_maker.h>
#include <indexlib/document/index_document/normal_document/normal_document.h>
#include "build_service/builder/test/MockBuilder.h"
using namespace std;
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(misc);

using namespace build_service::util;
using namespace build_service::document;
using namespace build_service::proto;
using namespace build_service::config;

namespace build_service {
namespace builder {

class MockIndexBuilder : public IndexBuilder {
public:
    MockIndexBuilder(const QuotaControlPtr &quotaControl)
        : IndexBuilder("", IndexPartitionOptions(), IndexPartitionSchemaPtr(),
                       quotaControl, NULL)
    {}
public:
    MOCK_METHOD1(Build, bool(const IE_NAMESPACE(document)::DocumentPtr &));
    MOCK_METHOD1(EndIndex, void(int64_t));
    MOCK_METHOD0(DumpSegment, void());
    MOCK_METHOD0(IsDumping, bool());        
    MOCK_METHOD2(Merge, versionid_t(const IE_NAMESPACE(config)::IndexPartitionOptions&, bool));
    MOCK_CONST_METHOD0(GetLastLocator, string());
    MOCK_CONST_METHOD0(GetLocatorInLatestVersion, string());
};
typedef ::testing::StrictMock<MockIndexBuilder> StrictMockIndexBuilder;
typedef ::testing::NiceMock<MockIndexBuilder> NiceMockIndexBuilder;

class BuilderTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
protected:
    IndexBuilderPtr createIndexBuilder();

    template<typename _Ex>
    void testBuildException(bool fatal, ServiceErrorCode ec);

    template<typename _Ex>
    void testMergeException(ServiceErrorCode ec);
protected:
    string _rootDir;
};

void BuilderTest::setUp() {
    _rootDir = GET_TEST_DATA_PATH();
}

void BuilderTest::tearDown() {
}

IndexBuilderPtr BuilderTest::createIndexBuilder() {
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(schema,
            //Field schema
            "text1:string;",
            //Primary key index schema
            "pk:primarykey64:text1",
            //Attribute schema
            "",
            //Summary schema
            "" );
    IndexPartitionOptions options;
    IndexBuilderPtr indexBuilder;
    QuotaControlPtr quotaControl(new QuotaControl(1024*1024*1024));
    indexBuilder.reset(new IndexBuilder(_rootDir, options, schema, quotaControl));
    indexBuilder->Init();
    return indexBuilder;
}

TEST_F(BuilderTest, testBuildFail) {
    MockBuilder builder(createIndexBuilder());
    config::BuilderConfig builderConfig;
    ASSERT_TRUE(builder.init(builderConfig, NULL));
    EXPECT_CALL(builder, doBuild(_))
        .WillOnce(Return(false))
        .WillOnce(Return(true));
    
    DocumentPtr doc(new NormalDocument);
    EXPECT_TRUE(builder.build(doc));
    EXPECT_TRUE(builder.build(doc));
    EXPECT_TRUE(builder._errorInfos.empty());
    EXPECT_FALSE(builder.hasFatalError());
}

TEST_F(BuilderTest, testTryDumpWithException) {
    QuotaControlPtr quotaControl(new QuotaControl(1024*1024*1024));
    StrictMockIndexBuilder *indexBuilder = new StrictMockIndexBuilder(quotaControl);
    Builder builder(IndexBuilderPtr(indexBuilder), NULL);

    EXPECT_CALL(*indexBuilder, IsDumping())
        .WillOnce(Return(false));
    
    EXPECT_CALL(*indexBuilder, DumpSegment())
        .WillOnce(Throw(FileIOException()));

    EXPECT_FALSE(builder.tryDump());
    EXPECT_EQ(1u, builder._errorInfos.size());
    EXPECT_EQ(BUILDER_ERROR_BUILD_FILEIO, builder._errorInfos.front().errorcode());
    EXPECT_TRUE(builder.hasFatalError()); 
}

template<typename _Ex>
void BuilderTest::testBuildException(bool fatal, ServiceErrorCode ec) {
    MockBuilder builder(createIndexBuilder());
    config::BuilderConfig builderConfig;
    ASSERT_TRUE(builder.init(builderConfig, NULL));
    
    EXPECT_CALL(builder, doBuild(_))
        .WillOnce(Throw(_Ex()));

    DocumentPtr doc(new NormalDocument);
    EXPECT_FALSE(builder.build(doc));

    if (fatal) {
        EXPECT_TRUE(builder.hasFatalError());
        EXPECT_FALSE(builder.build(doc));
    } else {
        EXPECT_CALL(builder, doBuild(_))
            .WillOnce(Return(true)); 
        EXPECT_TRUE(builder.build(doc));
    }

    if (SERVICE_ERROR_NONE != ec) {
        EXPECT_EQ((size_t)1, builder._errorInfos.size());
        EXPECT_EQ(ec, builder._errorInfos.front().errorcode());
    }
}

TEST_F(BuilderTest, testBuildFileIOException) {
    testBuildException<IE_NAMESPACE(misc)::FileIOException>(true, BUILDER_ERROR_BUILD_FILEIO);
}

TEST_F(BuilderTest, testBuildReachMaxResource) {
    testBuildException<IE_NAMESPACE(misc)::ReachMaxResourceException>(false, BUILDER_ERROR_REACH_MAX_RESOURCE);
}

TEST_F(BuilderTest, testBuildUnknownError) {
    testBuildException<IE_NAMESPACE(misc)::SchemaException>(true, BUILDER_ERROR_UNKNOWN);
}

template<typename _Ex>
void BuilderTest::testMergeException(ServiceErrorCode ec) {
    MockBuilder builder(createIndexBuilder());
    config::BuilderConfig builderConfig;
    ASSERT_TRUE(builder.init(builderConfig, NULL));    
    IndexPartitionOptions options;
    ON_CALL(builder, doMerge(_)).WillByDefault(Throw(_Ex()));
    EXPECT_FALSE(builder.merge(options));
    EXPECT_EQ((size_t)1, builder._errorInfos.size());
    EXPECT_EQ(ec, builder._errorInfos.front().errorcode());
    EXPECT_TRUE(builder.hasFatalError());
}

TEST_F(BuilderTest, testMergeFileIOError) {
    testMergeException<IE_NAMESPACE(misc)::FileIOException>(BUILDER_ERROR_MERGE_FILEIO);
}

TEST_F(BuilderTest, testMergeUnknownError) {
    testMergeException<IE_NAMESPACE(misc)::SchemaException>(BUILDER_ERROR_UNKNOWN);
}

TEST_F(BuilderTest, testBuildFlowLatencyMetricReport) {
    NiceMockBuilder builder(createIndexBuilder());
    config::BuilderConfig builderConfig;
    ASSERT_TRUE(builder.init(builderConfig, NULL));    
    DocumentPtr doc(new NormalDocument());
    doc->SetTimestamp(1234567);
    ON_CALL(builder, doBuild(_)).WillByDefault(Return(true));
    EXPECT_TRUE(builder.build(doc));
}

TEST_F(BuilderTest, testEndIndexFailed) {
    QuotaControlPtr quotaControl(new QuotaControl(1024*1024*1024));
    StrictMockIndexBuilder *indexBuilder = new StrictMockIndexBuilder(quotaControl);
    Builder *builder = new Builder(IndexBuilderPtr(indexBuilder), NULL);
    EXPECT_CALL(*indexBuilder, EndIndex(101)).WillOnce(Throw(FileIOException()));
    EXPECT_FALSE(builder->hasFatalError());
    builder->stop(101);
    EXPECT_TRUE(builder->hasFatalError());
    delete builder;
}

TEST_F(BuilderTest, testGetLastLocatorFailed) {
    QuotaControlPtr quotaControl(new QuotaControl(1024*1024*1024));
    StrictMockIndexBuilder *indexBuilder = new StrictMockIndexBuilder(quotaControl);
    Builder *builder = new Builder(IndexBuilderPtr(indexBuilder), NULL);

    EXPECT_CALL(*indexBuilder, GetLastLocator()).WillOnce(Throw(FileIOException()));
    EXPECT_FALSE(builder->hasFatalError());
    common::Locator locator;
    EXPECT_FALSE(builder->getLastLocator(locator));
    EXPECT_FALSE(builder->hasFatalError());
    delete builder;
}

TEST_F(BuilderTest, testGetLatestVersionLocatorFailed) {
    QuotaControlPtr quotaControl(new QuotaControl(1024*1024*1024));
    StrictMockIndexBuilder *indexBuilder = new StrictMockIndexBuilder(quotaControl);
    Builder *builder = new Builder(IndexBuilderPtr(indexBuilder), NULL);

    EXPECT_CALL(*indexBuilder, GetLocatorInLatestVersion()).WillOnce(Throw(FileIOException()));
    EXPECT_FALSE(builder->hasFatalError());
    common::Locator locator;
    EXPECT_FALSE(builder->getLatestVersionLocator(locator));
    EXPECT_FALSE(builder->hasFatalError());
    delete builder;
}

}
}
