#include "build_service/processor/SubDocumentExtractorProcessor.h"
#include "build_service/test/unittest.h"
#include <indexlib/config/index_partition_schema_maker.h>
#include <indexlib/document/raw_document/default_raw_document.h>

using namespace std;
IE_NAMESPACE_USE(config);
using namespace build_service::document;

namespace build_service {
namespace processor {

class SubDocumentExtractorProcessorTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
protected:
    IE_NAMESPACE(config)::IndexPartitionSchemaPtr _schema;
};

void SubDocumentExtractorProcessorTest::setUp() {
    _schema.reset(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(_schema,
            "title:string;", //Field schema
            "",//index schema
            "",//attribute schema
            "");//Summary schema
    IndexPartitionSchemaPtr subSchema(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(subSchema,
            "sub_title:text;int_field:int8:true;string_field:string;",
            "", "", "");
   _schema->SetSubIndexPartitionSchema(subSchema);
}

void SubDocumentExtractorProcessorTest::tearDown() {
}

TEST_F(SubDocumentExtractorProcessorTest, testSimpleProcess) {
    BS_LOG(DEBUG, "Begin Test!");

    RawDocumentPtr rawDoc(new IE_NAMESPACE(document)::DefaultRawDocument());
    rawDoc->setField("title", "main title");
    rawDoc->setField("sub_title", "firstsecond");

    ExtendDocumentPtr extendDoc(new ExtendDocument);
    extendDoc->setProperty(ExtendDocument::DOC_OPERATION_TYPE, ADD_DOC);
    extendDoc->setRawDocument(rawDoc);

    SubDocumentExtractorProcessor processor;
    KeyValueMap kvMap;
    processor.init(kvMap, _schema, NULL);
    processor.process(extendDoc);
    ASSERT_EQ((size_t)2, extendDoc->getSubDocumentsCount());
    RawDocumentPtr subRawDoc = extendDoc->getSubDocument(0)->getRawDocument();
    ASSERT_EQ(string("first"), subRawDoc->getField("sub_title"));
    subRawDoc = extendDoc->getSubDocument(1)->getRawDocument();
    ASSERT_EQ(string("second"), subRawDoc->getField("sub_title"));
}

TEST_F(SubDocumentExtractorProcessorTest, testProcessType) {
    SubDocumentExtractorProcessorPtr processor(new SubDocumentExtractorProcessor);
    DocumentProcessorPtr clonedProcessor(processor->clone());
    EXPECT_TRUE(clonedProcessor->needProcess(ADD_DOC));
    EXPECT_TRUE(clonedProcessor->needProcess(DELETE_DOC));
    EXPECT_TRUE(clonedProcessor->needProcess(UPDATE_FIELD));
    EXPECT_TRUE(clonedProcessor->needProcess(DELETE_SUB_DOC));

    EXPECT_TRUE(!clonedProcessor->needProcess(SKIP_DOC));
}

}
}
