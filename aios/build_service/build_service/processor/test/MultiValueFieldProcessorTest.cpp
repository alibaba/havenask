#include "build_service/test/unittest.h"
#include "build_service/processor/MultiValueFieldProcessor.h"
#include <indexlib/document/raw_document/default_raw_document.h>

using namespace std;
using namespace testing;

IE_NAMESPACE_USE(config);
using namespace build_service::document;

namespace build_service {
namespace processor {

class MultiValueFieldProcessorTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
};

void MultiValueFieldProcessorTest::setUp() {
}

void MultiValueFieldProcessorTest::tearDown() {
}

TEST_F(MultiValueFieldProcessorTest, testSimple) {
    string fieldName1 = "field_name_1";
    string fieldName2 = "field_name_2";

    string fieldValueRaw1 = "0.1,0.2";
    string fieldValue1 = "0.10.2";
    string fieldValueRaw2 = "0.3;0.5;0.2";
    string fieldValue2 = "0.30.50.2";

    RawDocumentPtr rawDoc(new IE_NAMESPACE(document)::DefaultRawDocument);
    rawDoc->setField(fieldName1, fieldValueRaw1);
    rawDoc->setField(fieldName2, fieldValueRaw2);
    
    ExtendDocumentPtr extendDoc(new ExtendDocument);
    extendDoc->setRawDocument(rawDoc);

    MultiValueFieldProcessor processor;
    KeyValueMap kv;
    kv["field_separator_description"] = "{\"field_name_1\":\",\",\"field_name_2\":\";\"}";
    
    processor.init(kv, IndexPartitionSchemaPtr(), NULL);
    bool ret = processor.process(extendDoc);
    ASSERT_TRUE(ret);
    
    ASSERT_EQ(fieldValue1,
                         extendDoc->getRawDocument()->getField(fieldName1));
    ASSERT_EQ(fieldValue2,
                         extendDoc->getRawDocument()->getField(fieldName2));

}

}
}
