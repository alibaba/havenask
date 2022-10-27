#include "indexlib/index/normal/test/ttl_decoder_unittest.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include <autil/legacy/any.h>

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, TtlDecoderTest);

TtlDecoderTest::TtlDecoderTest()
{
}

TtlDecoderTest::~TtlDecoderTest()
{
}

void TtlDecoderTest::CaseSetUp()
{
}

void TtlDecoderTest::CaseTearDown()
{
}

void TtlDecoderTest::TestSetDocumentTTL()
{
    // read config file
    string confFile = string(TEST_DATA_PATH) + "general_schema.json";
    ifstream in(confFile.c_str());
    string line;
    string jsonString;
    while(getline(in, line))
    {
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

IE_NAMESPACE_END(index);

