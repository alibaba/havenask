#include "indexlib/index/normal/test/ttl_decoder_unittest.h"

#include <fstream>

#include "autil/legacy/any.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace indexlib::document;
using namespace indexlib::config;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, TtlDecoderTest);

TtlDecoderTest::TtlDecoderTest() {}

TtlDecoderTest::~TtlDecoderTest() {}

void TtlDecoderTest::CaseSetUp() {}

void TtlDecoderTest::CaseTearDown() {}

void TtlDecoderTest::TestSetDocumentTTL()
{
    // read config file
    string confFile = GET_PRIVATE_TEST_DATA_PATH() + "general_schema.json";
    ifstream in(confFile.c_str());
    string line;
    string jsonString;
    while (getline(in, line)) {
        jsonString += line;
    }

    Any any = ParseJson(jsonString);
    string jsonString1 = ToString(any);
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
    FromJson(*schema, any);

    TTLDecoder ttlDecoder(schema);
    AttributeDocumentPtr attributeDocument(new AttributeDocument());
    NormalDocumentPtr doc(new NormalDocument());
    doc->SetAttributeDocument(attributeDocument);
    ttlDecoder.SetDocumentTTL(doc);
    INDEXLIB_TEST_TRUE(0u == doc->GetTTL());
}
}} // namespace indexlib::index
