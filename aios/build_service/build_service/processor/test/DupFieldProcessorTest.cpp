#include "build_service/test/unittest.h"
#include "build_service/processor/DupFieldProcessor.h"
#include <indexlib/document/raw_document/default_raw_document.h>

using namespace std;
using namespace testing;

using namespace build_service::document;

namespace build_service {
namespace processor {

class DupFieldProcessorTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
protected:
    ExtendDocumentPtr createDoc(const KeyValueMap &kvMap, DocOperateType docType = ADD_DOC);
};

void DupFieldProcessorTest::setUp() {
}

void DupFieldProcessorTest::tearDown() {
}

TEST_F(DupFieldProcessorTest, testSimple) {
    KeyValueMap kvMap;
    kvMap["f1_dup"] = "f1";
    kvMap["f2_dup"] = "f2";
    kvMap["f2_dup2"] = "f2";

    DupFieldProcessor processor;
    ASSERT_TRUE(processor.init(kvMap, IE_NAMESPACE(config)::IndexPartitionSchemaPtr(), NULL));

    KeyValueMap doc1;
    doc1["f1"] = "123";
    doc1["f2"] = "312";
    ExtendDocumentPtr doc = createDoc(doc1);
    ASSERT_TRUE(processor.process(doc));
    RawDocumentPtr rawDoc = doc->getRawDocument();
    EXPECT_EQ("123", rawDoc->getField("f1_dup"));
    EXPECT_EQ("312", rawDoc->getField("f2_dup"));
    EXPECT_EQ("312", rawDoc->getField("f2_dup2"));
}

TEST_F(DupFieldProcessorTest, testDeleteDoc) {
    KeyValueMap kvMap;
    kvMap["f1_dup"] = "f1";

    DupFieldProcessor processor;
    ASSERT_TRUE(processor.init(kvMap, IE_NAMESPACE(config)::IndexPartitionSchemaPtr(), NULL));

    ASSERT_TRUE(processor.needProcess(DELETE_DOC));

    KeyValueMap doc1;
    doc1["f1"] = "123";
    ExtendDocumentPtr doc = createDoc(doc1, DELETE_DOC);
    ASSERT_TRUE(processor.process(doc));
    RawDocumentPtr rawDoc = doc->getRawDocument();
    EXPECT_EQ("123", rawDoc->getField("f1_dup"));
}

ExtendDocumentPtr DupFieldProcessorTest::createDoc(
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
