#include "build_service/test/unittest.h"
#include "build_service/processor/DefaultValueProcessor.h"
#include "build_service/config/ResourceReader.h"
#include <indexlib/document/raw_document/default_raw_document.h>
#include <indexlib/index_base/schema_adapter.h>

using namespace std;
using namespace testing;

using namespace build_service::config;
using namespace build_service::document;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);

namespace build_service {
namespace processor {

class DefaultValueProcessorTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
protected:
    ExtendDocumentPtr createDoc(const KeyValueMap &kvMap, DocOperateType docType = ADD_DOC);
};

void DefaultValueProcessorTest::setUp() {
}

void DefaultValueProcessorTest::tearDown() {
}

TEST_F(DefaultValueProcessorTest, testSimple) {
    string configPath = TEST_DATA_PATH"default_value_processor_test/config/";
    ResourceReaderPtr resourceReader(new ResourceReader(configPath));

    string schemaFileName = ResourceReader::getSchemaConfRelativePath("default_value");
    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadSchema(
            configPath, schemaFileName);
    DefaultValueProcessor processor;
    ASSERT_TRUE(processor.init(KeyValueMap(), schema, NULL));

    KeyValueMap doc1;
    doc1["s_nid_gmv30_c2c"] = "";
    doc1["title_c2c"] = "";
    ExtendDocumentPtr doc = createDoc(doc1);
    ASSERT_TRUE(processor.process(doc));
    RawDocumentPtr rawDoc = doc->getRawDocument();
    EXPECT_EQ(";", rawDoc->getField("s_nid_gmv30_c2c"));
    EXPECT_EQ("", rawDoc->getField("title_c2c"));
    EXPECT_EQ("50000", rawDoc->getField("pricerank_c2c"));
}

ExtendDocumentPtr DefaultValueProcessorTest::createDoc(
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
