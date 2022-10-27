#include "build_service/test/unittest.h"
#include "build_service/builder/LineDataBuilder.h"
#include <indexlib/index_base/schema_adapter.h>
#include <indexlib/document/index_document/normal_document/normal_document.h>

using namespace std;
using namespace autil;
using namespace testing;
using namespace build_service::config;

IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(document);

class MockBufferedFileWriter : public BufferedFileWriter {
public:
    MOCK_METHOD1(Open, void(const string&));
    MOCK_METHOD2(Write, size_t(const void*, size_t));
    MOCK_CONST_METHOD0(GetLength, size_t());
    MOCK_CONST_METHOD0(GetPath, const string &());
    MOCK_METHOD0(Close, void());
public:
    void DoClose() { mIsClosed = true; }
};

typedef ::testing::StrictMock<MockBufferedFileWriter> StrictMockBufferedFileWriter;
typedef ::testing::NiceMock<MockBufferedFileWriter> NiceMockBufferedFileWriter;

namespace build_service {
namespace builder {

class LineDataBuilderTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
    static DocumentPtr createDoc() {
        NormalDocumentPtr doc(new NormalDocument);
        AttributeDocumentPtr attrDoc(new AttributeDocument);
        attrDoc->SetField(0, ConstString("1"));
        attrDoc->SetField(1, ConstString("company"));
        doc->SetAttributeDocument(attrDoc);
        return doc;
    }
    IndexPartitionSchemaPtr schema1;
    IndexPartitionSchemaPtr schema2;
};

void LineDataBuilderTest::setUp() {
    schema1 = SchemaAdapter::LoadSchema(
            TEST_DATA_PATH"/line_data_builder_test/", "1_schema.json");
    ASSERT_TRUE(schema1);
    schema2 = SchemaAdapter::LoadSchema(
            TEST_DATA_PATH"/line_data_builder_test/", "2_schema.json");
    ASSERT_TRUE(schema2);
}

void LineDataBuilderTest::tearDown() {
}

TEST_F(LineDataBuilderTest, testPrepareOne) {
    LineDataBuilder builder("", schema1, NULL);
    EXPECT_TRUE(builder.init(BuilderConfig(), NULL));

    string oneLine;
    EXPECT_TRUE(builder.prepareOne(createDoc(), oneLine));
    EXPECT_EQ("1sepcompany\n", oneLine);

    builder._schema = schema2;
    EXPECT_TRUE(builder.prepareOne(createDoc(), oneLine));
    EXPECT_EQ("1\n", oneLine);

    builder.stop();
}

TEST_F(LineDataBuilderTest, testBuild) {
    tr1::shared_ptr<NiceMockBufferedFileWriter> writer(new NiceMockBufferedFileWriter);
    LineDataBuilder builder("", schema1, NULL);
    builder.setFileWriter(writer);
    EXPECT_CALL(*writer, Write(_, _))
        .WillOnce(Return(true))
        .WillOnce(Throw(std::exception()));
    EXPECT_CALL(*writer, Close()).Times(1).WillOnce(Invoke(writer.get(), &NiceMockBufferedFileWriter::DoClose));
    EXPECT_FALSE(builder._fatalError);
    EXPECT_TRUE(builder.build(createDoc()));
    EXPECT_FALSE(builder._fatalError);
    EXPECT_FALSE(builder.build(createDoc()));
    EXPECT_TRUE(builder._fatalError);
    builder.stop();
}

TEST_F(LineDataBuilderTest, testClose) {
    tr1::shared_ptr<NiceMockBufferedFileWriter> writer(new NiceMockBufferedFileWriter);
    LineDataBuilder builder("", schema1, NULL);
    builder.setFileWriter(writer);
    EXPECT_CALL(*writer, Close())
        .WillOnce(DoAll(Invoke(writer.get(), &NiceMockBufferedFileWriter::DoClose), Throw(std::exception() )));
    EXPECT_FALSE(builder._fatalError);
    builder.stop();
    EXPECT_TRUE(builder._fatalError);
}

TEST_F(LineDataBuilderTest, testInit) {
    tr1::shared_ptr<NiceMockBufferedFileWriter> writer(new NiceMockBufferedFileWriter);
    LineDataBuilder builder("", schema1, NULL);
    builder.setFileWriter(writer);
    EXPECT_CALL(*writer, Open(_))
        .WillOnce(DoAll(InvokeWithoutArgs(writer.get(), &NiceMockBufferedFileWriter::DoClose), Throw(std::exception())));
    EXPECT_FALSE(builder.init(BuilderConfig(), NULL));
    builder.stop();
}

TEST_F(LineDataBuilderTest, testStoreSchema) {
    LineDataBuilder::storeSchema("", schema1);
    LineDataBuilder::storeSchema("", schema1);
}

}
}
