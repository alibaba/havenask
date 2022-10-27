#include "build_service/test/unittest.h"
#include "build_service/processor/DocTraceProcessor.h"
#include <indexlib/document/raw_document/default_raw_document.h>

using namespace std;
using namespace testing;
using namespace build_service::document;

namespace build_service {
namespace processor {

class DocTraceProcessorTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();

private:
    ExtendDocumentPtr createDoc(
            const KeyValueMap &kvMap,
            DocOperateType docType);
};

void DocTraceProcessorTest::setUp() {
}

void DocTraceProcessorTest::tearDown() {
}

TEST_F(DocTraceProcessorTest, testSimple) {
    KeyValueMap kvMap;
    kvMap["match_rule"] = "f1=123,f2=456;f3=3";
    kvMap["sample_freq"] = "1";
    kvMap["trace_field"] = "f1";

    DocTraceProcessor processor;
    ASSERT_TRUE(processor.init(kvMap, IE_NAMESPACE(config)::IndexPartitionSchemaPtr(), NULL));

    KeyValueMap doc1;
    doc1["f1"] = "123";
    doc1["f2"] = "456";
    doc1["f3"] = "4";
    ExtendDocumentPtr doc = createDoc(doc1, DELETE_DOC);
    ASSERT_TRUE(processor.process(doc));

    auto rawDoc = doc->getRawDocument();
    ASSERT_TRUE(rawDoc->NeedTrace());
    ASSERT_EQ("123", rawDoc->GetTraceId());

    doc1.clear();
    doc1["f1"] = "124";
    doc1["f2"] = "456";
    doc1["f3"] = "4";
    ExtendDocumentPtr doc2 = createDoc(doc1, DELETE_DOC);
    ASSERT_TRUE(processor.process(doc2));
    rawDoc = doc2->getRawDocument();
    ASSERT_FALSE(rawDoc->NeedTrace());
}

TEST_F(DocTraceProcessorTest, testSample) {
    KeyValueMap kvMap;
    kvMap["sample_freq"] = "2";
    kvMap["trace_field"] = "f1";

    DocTraceProcessor processor;
    ASSERT_TRUE(processor.init(kvMap, IE_NAMESPACE(config)::IndexPartitionSchemaPtr(), NULL));

    KeyValueMap doc1;
    doc1["f1"] = "123";
    doc1["f2"] = "456";
    doc1["f3"] = "4";
    ExtendDocumentPtr doc = createDoc(doc1, DELETE_DOC);
    ASSERT_TRUE(processor.process(doc));

    auto rawDoc = doc->getRawDocument();
    ASSERT_FALSE(rawDoc->NeedTrace());
    ExtendDocumentPtr doc2 = createDoc(doc1, DELETE_DOC);
    ASSERT_TRUE(processor.process(doc2));
    rawDoc = doc2->getRawDocument();
    ASSERT_TRUE(rawDoc->NeedTrace());
    ASSERT_EQ("123", rawDoc->GetTraceId());

    ExtendDocumentPtr doc3 = createDoc(doc1, DELETE_DOC);
    ASSERT_TRUE(processor.process(doc3));
    rawDoc = doc3->getRawDocument();
    ASSERT_FALSE(rawDoc->NeedTrace());
}

ExtendDocumentPtr DocTraceProcessorTest::createDoc(
        const KeyValueMap &kvMap,
        DocOperateType docType)
{
    RawDocumentPtr rawDoc(new IE_NAMESPACE(document)::DefaultRawDocument);
    rawDoc->setDocOperateType(docType);
    for (KeyValueMap::const_iterator it = kvMap.begin();
         it != kvMap.end(); it++)
    {
        rawDoc->setField(it->first, it->second);
    }
    ExtendDocumentPtr extendDoc(new ExtendDocument);
    extendDoc->setRawDocument(rawDoc);
    return extendDoc;
}

}
}
