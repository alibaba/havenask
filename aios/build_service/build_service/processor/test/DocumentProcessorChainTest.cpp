#include "build_service/test/unittest.h"
#include "build_service/processor/DocumentProcessorChain.h"
#include <indexlib/document/document_parser.h>
#include <indexlib/document/raw_document/default_raw_document.h>
#include <indexlib/document/index_document/normal_document/normal_document.h>

using namespace std;
using namespace build_service::document;

IE_NAMESPACE_USE(document);

namespace build_service {
namespace processor {

class FakeDocumentProcessorChain : public DocumentProcessorChain {
public:
    DocumentProcessorChain* clone()
    {
        return new FakeDocumentProcessorChain();
    }

    bool processExtendDocument(const document::ExtendDocumentPtr &extendDocument)
    {
        return true;
    }

    void batchProcessExtendDocs(
            const std::vector<document::ExtendDocumentPtr>& extDocVec)
    {}
};

class DocumentProcessorChainTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
};

class FakeDocumentParser : public IE_NAMESPACE(document)::DocumentParser
{
public:
    FakeDocumentParser()
        : DocumentParser(IE_NAMESPACE(config)::IndexPartitionSchemaPtr())
    {}
    ~FakeDocumentParser() {}
public:
    bool DoInit() { return true; }
    IE_NAMESPACE(document)::DocumentPtr Parse(
        const IndexlibExtendDocumentPtr& extendDoc)
    { return DocumentPtr(); }
    // dataStr: serialized data which from call document serialize interface
    IE_NAMESPACE(document)::DocumentPtr Parse(
        const autil::ConstString& serializedData)
    { return DocumentPtr(); }
};


class FakeActualDocumentParser : public IE_NAMESPACE(document)::DocumentParser
{
public:
    FakeActualDocumentParser()
        : DocumentParser(IE_NAMESPACE(config)::IndexPartitionSchemaPtr())
    {}
    ~FakeActualDocumentParser() {}
public:
    bool DoInit() { return true; }
    IE_NAMESPACE(document)::DocumentPtr Parse(
        const IndexlibExtendDocumentPtr& extendDoc)
    {
        IE_NAMESPACE(document)::DocumentPtr document;
        document.reset(new IE_NAMESPACE(document)::NormalDocument);
        return document;
    }
    // dataStr: serialized data which from call document serialize interface
    IE_NAMESPACE(document)::DocumentPtr Parse(
        const autil::ConstString& serializedData)
    {
        IE_NAMESPACE(document)::DocumentPtr document;
        document.reset(new IE_NAMESPACE(document)::NormalDocument);
        return document;
    }
};

    
void DocumentProcessorChainTest::setUp() {
}

void DocumentProcessorChainTest::tearDown() {
}
    
TEST_F(DocumentProcessorChainTest, testProcessError) {
    FakeDocumentProcessorChain chain;
    chain._docParser.reset(new FakeDocumentParser);

    RawDocumentPtr rawDoc(new DefaultRawDocument);
    ASSERT_FALSE(chain.process(rawDoc));
}

TEST_F(DocumentProcessorChainTest, testUseRawDocAsDoc) {
    FakeDocumentProcessorChain chain;
    chain._docParser.reset(new FakeDocumentParser);
    RawDocumentPtr rawDoc(new DefaultRawDocument);
    ASSERT_FALSE(chain.process(rawDoc));
    
    chain._useRawDocAsDoc = true;
    rawDoc->setField("id", "100");
    ProcessedDocumentPtr processedDoc = chain.process(rawDoc);
    ASSERT_TRUE(processedDoc);
    IE_NAMESPACE(document)::DocumentPtr indexDocument = processedDoc->getDocument();
    ASSERT_EQ(rawDoc, indexDocument);
}

TEST_F(DocumentProcessorChainTest, testSkipDoc) {
    FakeDocumentProcessorChain chain;
    chain._docParser.reset(new FakeDocumentParser);
    RawDocumentPtr rawDoc(new DefaultRawDocument);
    rawDoc->setDocOperateType(SKIP_DOC);
    ProcessedDocumentPtr processedDoc = chain.process(rawDoc);
    ASSERT_TRUE(processedDoc);
    ASSERT_FALSE(processedDoc->getDocument());
    ASSERT_TRUE(processedDoc->needSkip());
}

TEST_F(DocumentProcessorChainTest, testTraceFlag) {
    FakeDocumentProcessorChain chain;
    chain._docParser.reset(new FakeActualDocumentParser);
    {
        RawDocumentPtr rawDoc(new DefaultRawDocument);
        rawDoc->setField(BUILTIN_KEY_TRACE_ID, "100");
        ProcessedDocumentPtr processedDoc = chain.process(rawDoc);
        ASSERT_TRUE(processedDoc);
        IE_NAMESPACE(document)::DocumentPtr indexDocument = processedDoc->getDocument();
        ASSERT_TRUE(indexDocument->NeedTrace());
    }
    //test false
    {
        RawDocumentPtr rawDoc(new DefaultRawDocument);
        rawDoc->setField("id", "100");
        ProcessedDocumentPtr processedDoc = chain.process(rawDoc);
        ASSERT_TRUE(processedDoc);
        IE_NAMESPACE(document)::DocumentPtr indexDocument = processedDoc->getDocument();
        ASSERT_FALSE(indexDocument->NeedTrace());
    }
}


}
}
