#include "build_service/builder/LineDataBuilder.h"

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "autil/Span.h"
#include "autil/TimeUtility.h"
#include "build_service/config/BuilderConfig.h"
#include "build_service/test/unittest.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/load_config_list.h"
#include "indexlib/document/index_document/normal_document/attribute_document.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/document/index_locator.h"
#include "indexlib/document/normal/AttributeDocument.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/fslib/FenceContext.h"
#include "indexlib/file_system/load_config/LoadStrategy.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/partition/index_builder.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace testing;
using namespace build_service::config;

using namespace indexlib::file_system;
using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::document;

class MockBufferedFileWriter : public BufferedFileWriter
{
public:
    MOCK_METHOD(ErrorCode, DoOpen, (), (noexcept, override));
    MOCK_METHOD(FSResult<size_t>, Write, (const void*, size_t), (noexcept, override));
    MOCK_METHOD(size_t, GetLength, (), (const, noexcept, override));
    MOCK_METHOD(ErrorCode, DoClose, (), (noexcept, override));
    MOCK_METHOD(FSResult<void>, ReserveFile, (size_t), (noexcept, override));
    MOCK_METHOD(FSResult<void>, Truncate, (size_t), (noexcept, override));

public:
    ErrorCode InnerClose() noexcept
    {
        _isClosed = true;
        return FSEC_OK;
    }
};

typedef ::testing::StrictMock<MockBufferedFileWriter> StrictMockBufferedFileWriter;
typedef ::testing::NiceMock<MockBufferedFileWriter> NiceMockBufferedFileWriter;

namespace build_service { namespace builder {

class LineDataBuilderTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();
    static DocumentPtr createDoc()
    {
        NormalDocumentPtr doc(new NormalDocument);
        AttributeDocumentPtr attrDoc(new AttributeDocument);
        attrDoc->SetField(0, StringView("1"));
        attrDoc->SetField(1, StringView("company"));
        doc->SetAttributeDocument(attrDoc);
        return doc;
    }
    IndexPartitionSchemaPtr schema1;
    IndexPartitionSchemaPtr schema2;
};

void LineDataBuilderTest::setUp()
{
    schema1 = IndexPartitionSchema::Load(GET_TEST_DATA_PATH() + "/line_data_builder_test/" + "1_schema.json");
    ASSERT_TRUE(schema1);
    schema2 = IndexPartitionSchema::Load(GET_TEST_DATA_PATH() + "/line_data_builder_test/" + "2_schema.json");
    ASSERT_TRUE(schema2);
}

void LineDataBuilderTest::tearDown() {}

TEST_F(LineDataBuilderTest, testPrepareOne)
{
    LineDataBuilder builder(GET_TEMP_DATA_PATH(), schema1, NULL, "");
    EXPECT_TRUE(builder.init(BuilderConfig(), NULL));

    string oneLine;
    EXPECT_TRUE(builder.prepareOne(createDoc(), oneLine));
    EXPECT_EQ("1sepcompany\n", oneLine);

    builder._schema = schema2;
    EXPECT_TRUE(builder.prepareOne(createDoc(), oneLine));
    EXPECT_EQ("1\n", oneLine);

    builder.stop();
}

TEST_F(LineDataBuilderTest, testBuild)
{
    shared_ptr<NiceMockBufferedFileWriter> writer(new NiceMockBufferedFileWriter);
    LineDataBuilder builder(GET_TEMP_DATA_PATH(), schema1, NULL, "");
    builder.setFileWriter(writer);
    EXPECT_CALL(*writer, Write(_, _))
        .WillOnce(Return(FSResult<size_t> {FSEC_OK, 1}))
        .WillOnce(Return(FSResult<size_t> {FSEC_ERROR, 0}));
    EXPECT_CALL(*writer, DoClose()).Times(1).WillOnce(Invoke(writer.get(), &NiceMockBufferedFileWriter::InnerClose));
    EXPECT_FALSE(builder._fatalError);
    EXPECT_TRUE(builder.build(createDoc()));
    EXPECT_FALSE(builder._fatalError);
    EXPECT_FALSE(builder.build(createDoc()));
    EXPECT_TRUE(builder._fatalError);
    builder.stop();
}

TEST_F(LineDataBuilderTest, testClose)
{
    shared_ptr<NiceMockBufferedFileWriter> writer(new NiceMockBufferedFileWriter);
    LineDataBuilder builder(GET_TEMP_DATA_PATH(), schema1, NULL, "");
    builder.setFileWriter(writer);
    EXPECT_CALL(*writer, DoClose())
        .WillOnce(DoAll(Invoke(writer.get(), &NiceMockBufferedFileWriter::InnerClose), Return(FSEC_ERROR)));
    EXPECT_FALSE(builder._fatalError);
    builder.stop();
    EXPECT_TRUE(builder._fatalError);
}

TEST_F(LineDataBuilderTest, testInit)
{
    shared_ptr<NiceMockBufferedFileWriter> writer(new NiceMockBufferedFileWriter);
    LineDataBuilder builder(GET_TEMP_DATA_PATH(), schema1, NULL, "");
    builder.setFileWriter(writer);
    EXPECT_CALL(*writer, DoOpen())
        .WillOnce(DoAll(InvokeWithoutArgs(writer.get(), &NiceMockBufferedFileWriter::InnerClose), Return(FSEC_ERROR)));
    EXPECT_FALSE(builder.init(BuilderConfig(), NULL));
    builder.stop();
}

TEST_F(LineDataBuilderTest, testStoreSchema)
{
    LineDataBuilder::storeSchema(GET_TEMP_DATA_PATH(), schema1, FenceContext::NoFence());
    LineDataBuilder::storeSchema(GET_TEMP_DATA_PATH(), schema1, FenceContext::NoFence());
}

}} // namespace build_service::builder
