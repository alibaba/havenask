#include "build_service/builder/Builder.h"

#include <cstdint>
#include <deque>
#include <iosfwd>
#include <map>
#include <memory>
#include <stdlib.h>
#include <string>

#include "autil/Span.h"
#include "autil/TimeUtility.h"
#include "build_service/builder/test/MockBuilder.h"
#include "build_service/common/Locator.h"
#include "build_service/common/PeriodDocCounter.h"
#include "build_service/common_define.h"
#include "build_service/config/BuilderConfig.h"
#include "build_service/document/DocumentDefine.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/test/unittest.h"
#include "build_service/util/ErrorLogCollector.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_partition_schema_maker.h"
#include "indexlib/config/load_config_list.h"
#include "indexlib/document/document.h"
#include "indexlib/document/index_document/normal_document/attribute_document.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/document/index_locator.h"
#include "indexlib/document/normal/AttributeDocument.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/builder_branch_hinter.h"
#include "indexlib/partition/index_builder.h"
#include "indexlib/util/ErrorLogCollector.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/memory_control/QuotaControl.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace indexlib::index_base;
using namespace indexlib::document;
using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::partition;
using namespace indexlib::util;

using namespace build_service::util;
using namespace build_service::document;
using namespace build_service::proto;
using namespace build_service::config;
using namespace build_service::common;

namespace build_service { namespace builder {

class MockIndexBuilder : public IndexBuilder
{
public:
    MockIndexBuilder(const QuotaControlPtr& quotaControl)
        : IndexBuilder("", IndexPartitionOptions(), IndexPartitionSchemaPtr(new IndexPartitionSchema), quotaControl,
                       BuilderBranchHinter::Option::Test(), NULL)
    {
    }

public:
    MOCK_METHOD1(Build, bool(const indexlib::document::DocumentPtr&));
    MOCK_METHOD1(EndIndex, void(int64_t));
    MOCK_METHOD0(DumpSegment, void());
    MOCK_METHOD0(IsDumping, bool());
    using IndexBuilder::Merge;
    MOCK_METHOD2(Merge, versionid_t(const indexlib::config::IndexPartitionOptions&, bool));
    MOCK_CONST_METHOD0(GetLastLocator, string());
    MOCK_CONST_METHOD0(GetLocatorInLatestVersion, string());
};
typedef ::testing::StrictMock<MockIndexBuilder> StrictMockIndexBuilder;
typedef ::testing::NiceMock<MockIndexBuilder> NiceMockIndexBuilder;

class BuilderTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();

protected:
    IndexBuilderPtr createIndexBuilder();

    template <typename _Ex>
    void testBuildException(bool fatal, ServiceErrorCode ec);

    template <typename _Ex>
    void testMergeException(ServiceErrorCode ec);

protected:
    string _rootDir;
};

void BuilderTest::setUp() { _rootDir = GET_TEMP_DATA_PATH(); }

void BuilderTest::tearDown() {}

IndexBuilderPtr BuilderTest::createIndexBuilder()
{
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(schema,
                                          // Field schema
                                          "text1:string;service_id:int64",
                                          // Primary key index schema
                                          "pk:primarykey64:text1",
                                          // Attribute schema
                                          "service_id",
                                          // Summary schema
                                          "");
    IndexPartitionOptions options;
    IndexBuilderPtr indexBuilder;
    QuotaControlPtr quotaControl(new QuotaControl(1024 * 1024 * 1024));
    indexBuilder.reset(new IndexBuilder(_rootDir, options, schema, quotaControl, BuilderBranchHinter::Option::Test()));
    indexBuilder->Init();
    return indexBuilder;
}

TEST_F(BuilderTest, testBuildFail)
{
    MockBuilder builder(createIndexBuilder());
    config::BuilderConfig builderConfig;
    ASSERT_TRUE(builder.init(builderConfig, NULL));
    EXPECT_CALL(builder, doBuild(_)).WillOnce(Return(false)).WillOnce(Return(true));

    DocumentPtr doc(new NormalDocument);
    EXPECT_TRUE(builder.build(doc));
    EXPECT_TRUE(builder.build(doc));
    EXPECT_TRUE(builder._errorInfos.empty());
    EXPECT_FALSE(builder.hasFatalError());
}

TEST_F(BuilderTest, testTryDumpWithException)
{
    QuotaControlPtr quotaControl(new QuotaControl(1024 * 1024 * 1024));
    StrictMockIndexBuilder* indexBuilder = new StrictMockIndexBuilder(quotaControl);
    Builder builder(IndexBuilderPtr(indexBuilder), NULL);

    EXPECT_CALL(*indexBuilder, IsDumping()).WillOnce(Return(false));

    EXPECT_CALL(*indexBuilder, DumpSegment()).WillOnce(Throw(FileIOException()));

    EXPECT_FALSE(builder.tryDump());
    EXPECT_EQ(1u, builder._errorInfos.size());
    EXPECT_EQ(BUILDER_ERROR_BUILD_FILEIO, builder._errorInfos.front().errorcode());
    EXPECT_TRUE(builder.hasFatalError());
}

template <typename _Ex>
void BuilderTest::testBuildException(bool fatal, ServiceErrorCode ec)
{
    MockBuilder builder(createIndexBuilder());
    config::BuilderConfig builderConfig;
    ASSERT_TRUE(builder.init(builderConfig, NULL));

    EXPECT_CALL(builder, doBuild(_)).WillOnce(Throw(_Ex()));

    DocumentPtr doc(new NormalDocument);
    EXPECT_FALSE(builder.build(doc));

    if (fatal) {
        EXPECT_TRUE(builder.hasFatalError());
        EXPECT_FALSE(builder.build(doc));
    } else {
        EXPECT_CALL(builder, doBuild(_)).WillOnce(Return(true));
        EXPECT_TRUE(builder.build(doc));
    }

    if (SERVICE_ERROR_NONE != ec) {
        EXPECT_EQ((size_t)1, builder._errorInfos.size());
        EXPECT_EQ(ec, builder._errorInfos.front().errorcode());
    }
}

TEST_F(BuilderTest, testBuildFileIOException)
{
    testBuildException<indexlib::util::FileIOException>(true, BUILDER_ERROR_BUILD_FILEIO);
}

TEST_F(BuilderTest, testBuildReachMaxResource)
{
    testBuildException<indexlib::util::ReachMaxResourceException>(false, BUILDER_ERROR_REACH_MAX_RESOURCE);
}

TEST_F(BuilderTest, testBuildUnknownError)
{
    testBuildException<indexlib::util::SchemaException>(true, BUILDER_ERROR_UNKNOWN);
}

template <typename _Ex>
void BuilderTest::testMergeException(ServiceErrorCode ec)
{
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

TEST_F(BuilderTest, testMergeFileIOError)
{
    testMergeException<indexlib::util::FileIOException>(BUILDER_ERROR_MERGE_FILEIO);
}

TEST_F(BuilderTest, testMergeUnknownError)
{
    testMergeException<indexlib::util::SchemaException>(BUILDER_ERROR_UNKNOWN);
}

TEST_F(BuilderTest, testBuildFlowLatencyMetricReport)
{
    NiceMockBuilder builder(createIndexBuilder());
    config::BuilderConfig builderConfig;
    ASSERT_TRUE(builder.init(builderConfig, NULL));
    DocumentPtr doc(new NormalDocument());
    doc->SetTimestamp(1234567);
    ON_CALL(builder, doBuild(_)).WillByDefault(Return(true));
    EXPECT_TRUE(builder.build(doc));
}

TEST_F(BuilderTest, testEndIndexFailed)
{
    QuotaControlPtr quotaControl(new QuotaControl(1024 * 1024 * 1024));
    StrictMockIndexBuilder* indexBuilder = new StrictMockIndexBuilder(quotaControl);
    Builder* builder = new Builder(IndexBuilderPtr(indexBuilder), NULL);
    EXPECT_CALL(*indexBuilder, EndIndex(101)).WillOnce(Throw(FileIOException()));
    EXPECT_FALSE(builder->hasFatalError());
    builder->stop(101);
    EXPECT_TRUE(builder->hasFatalError());
    delete builder;
}

TEST_F(BuilderTest, testGetLastLocatorFailed)
{
    QuotaControlPtr quotaControl(new QuotaControl(1024 * 1024 * 1024));
    StrictMockIndexBuilder* indexBuilder = new StrictMockIndexBuilder(quotaControl);
    Builder* builder = new Builder(IndexBuilderPtr(indexBuilder), NULL);

    EXPECT_CALL(*indexBuilder, GetLastLocator()).WillOnce(Throw(FileIOException()));
    EXPECT_FALSE(builder->hasFatalError());
    common::Locator locator;
    EXPECT_FALSE(builder->getLastLocator(locator));
    EXPECT_FALSE(builder->hasFatalError());
    delete builder;
}

TEST_F(BuilderTest, testGetLatestVersionLocatorFailed)
{
    QuotaControlPtr quotaControl(new QuotaControl(1024 * 1024 * 1024));
    StrictMockIndexBuilder* indexBuilder = new StrictMockIndexBuilder(quotaControl);
    Builder* builder = new Builder(IndexBuilderPtr(indexBuilder), NULL);

    EXPECT_CALL(*indexBuilder, GetLocatorInLatestVersion()).WillOnce(Throw(FileIOException()));
    EXPECT_FALSE(builder->hasFatalError());
    common::Locator locator;
    EXPECT_FALSE(builder->getLatestVersionLocator(locator));
    EXPECT_FALSE(builder->hasFatalError());
    delete builder;
}

TEST_F(BuilderTest, DISABLED_testBuildWithPeriodDocCounter)
{
    // dont let it flush
    setenv(BS_ENV_DOC_TRACE_INTERVAL.c_str(), "100", true);
    setenv(BS_ENV_DOC_TRACE_FIELD.c_str(), "service_id", true);
    MockBuilder builder(createIndexBuilder());
    config::BuilderConfig builderConfig;
    ASSERT_TRUE(builder.init(builderConfig, NULL));
    EXPECT_CALL(builder, doBuild(_)).WillRepeatedly(Return(true));

    PeriodDocCounter<int64_t>* counter = PeriodDocCounter<int64_t>::GetInstance();
    counter->reset();

    AttributeDocumentPtr attrDoc(new AttributeDocument);
    int64_t serviceId = 1;
    StringView value((char*)(&serviceId), sizeof(int64_t));
    attrDoc->SetField(1, value);
    NormalDocumentPtr doc(new NormalDocument);
    doc->SetAttributeDocument(attrDoc);

    EXPECT_TRUE(builder.build(doc));
    EXPECT_TRUE(builder.build(doc));

    int64_t serviceId2 = 2;
    StringView value2((char*)(&serviceId2), sizeof(int64_t));
    attrDoc->SetField(1, value2);
    EXPECT_TRUE(builder.build(doc));
    EXPECT_TRUE(builder.build(doc));
    EXPECT_TRUE(builder.build(doc));

    EXPECT_TRUE(builder._errorInfos.empty());
    EXPECT_FALSE(builder.hasFatalError());

    int32_t typeId = (int32_t)PeriodDocCounterType::Builder;
    EXPECT_EQ(2, counter->_counter[typeId][serviceId]);
    EXPECT_EQ(3, counter->_counter[typeId][serviceId2]);
    unsetenv(BS_ENV_DOC_TRACE_INTERVAL.c_str());
    unsetenv(BS_ENV_DOC_TRACE_FIELD.c_str());
}

}} // namespace build_service::builder
