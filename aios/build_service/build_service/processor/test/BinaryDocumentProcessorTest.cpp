#include "build_service/processor/BinaryDocumentProcessor.h"
#include "build_service/test/unittest.h"
#include <indexlib/document/raw_document/default_raw_document.h>

using namespace std;

IE_NAMESPACE_USE(config);
using namespace build_service::document;

namespace build_service {
namespace processor {

class BinaryDocumentProcessorTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();

};


void BinaryDocumentProcessorTest::setUp() {
}

void BinaryDocumentProcessorTest::tearDown() {
}

TEST_F(BinaryDocumentProcessorTest, testSimpleProcess) {
    BS_LOG(DEBUG, "Begin Test!");

    string fieldName1 = "binary_field_name1";
    string fieldName2 = "binary_field_name2";
    string fieldName3 = "binary_field_name3";

    string fieldValueRaw = string(100, 0);
    istringstream is(fieldValueRaw);
    ostringstream os;
    autil::legacy::Base64Encoding(is, os);

    string errorMsg = "%^&^&^*&^*&^x";

    string fieldContent = os.str();
    RawDocumentPtr rawDoc(new IE_NAMESPACE(document)::DefaultRawDocument());
    rawDoc->setField(fieldName1, fieldContent);
    rawDoc->setField(fieldName2, fieldContent);
    rawDoc->setField(fieldName3, errorMsg);

    ExtendDocumentPtr extendDoc(new ExtendDocument);
    extendDoc->setRawDocument(rawDoc);

    BinaryDocumentProcessor processor;
    KeyValueMap kv;
    kv[BinaryDocumentProcessor::BINARY_FIELD_NAMES] =
        fieldName1 + "  ;  not_exist_field;" + fieldName2 + ";" + fieldName3;

    processor.init(kv, IndexPartitionSchemaPtr(), NULL);
    bool ret = processor.process(extendDoc);
    ASSERT_TRUE(ret);
    ASSERT_EQ(fieldValueRaw,
                         extendDoc->getRawDocument()->getField(fieldName1));
    ASSERT_EQ(fieldValueRaw,
                         extendDoc->getRawDocument()->getField(fieldName2));
    ASSERT_EQ(errorMsg,
                         extendDoc->getRawDocument()->getField(fieldName3));
}


}
}
