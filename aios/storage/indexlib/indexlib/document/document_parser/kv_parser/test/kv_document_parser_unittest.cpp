#include "autil/StringUtil.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema_maker.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/config/test/schema_loader.h"
#include "indexlib/document/document_factory_wrapper.h"
#include "indexlib/document/document_parser/kv_parser/kv_document_parser.h"
#include "indexlib/document/document_parser/kv_parser/multi_region_kv_key_extractor.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/document/raw_document/default_raw_document.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::common;

namespace indexlib { namespace document {

class KvDocumentParserTest : public INDEXLIB_TESTBASE
{
public:
    void CaseSetUp();
    void CaseTearDown();

private:
    IndexlibExtendDocumentPtr createExtendDoc(const std::string& fieldMap, regionid_t regionId = DEFAULT_REGIONID);

protected:
    IndexlibExtendDocumentPtr _extendDoc;
    config::IndexPartitionSchemaPtr _schemaPtr;
};

void KvDocumentParserTest::CaseSetUp()
{
    _extendDoc.reset(new IndexlibExtendDocument());
    RawDocumentPtr rawDoc(new DefaultRawDocument());
    _extendDoc->setRawDocument(rawDoc);
}

void KvDocumentParserTest::CaseTearDown() {}

TEST_F(KvDocumentParserTest, testKVAdd)
{
    _extendDoc->getRawDocument()->setDocOperateType(ADD_DOC);
    _schemaPtr = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "/document_parser_kv_schema.json");

    RawDocumentPtr rawDoc = _extendDoc->getRawDocument();
    rawDoc->setField("usernick", "jack");
    rawDoc->setField("userid", "1");

    DocumentFactoryWrapper wrapper(_schemaPtr);
    wrapper.Init();
    DocumentParserPtr parser(wrapper.CreateDocumentParser());

    ASSERT_TRUE(dynamic_cast<KvDocumentParser*>(parser.get()));
    auto document = DYNAMIC_POINTER_CAST(KVDocument, parser->Parse(_extendDoc));
    ASSERT_TRUE(document);

    const IndexSchemaPtr& indexSchema = _schemaPtr->GetIndexSchema();
    KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());

    KVKeyExtractorPtr kvKeyExtractor(new KVKeyExtractor(kvConfig));
    uint64_t keyHash;
    kvKeyExtractor->HashKey("jack", keyHash);

    auto& kvIndexDoc = document->back();
    ASSERT_EQ(keyHash, kvIndexDoc.GetPKeyHash());
}

TEST_F(KvDocumentParserTest, testKVUpdate)
{
    _extendDoc->getRawDocument()->setDocOperateType(UPDATE_FIELD);
    _schemaPtr = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "/document_parser_kv_schema.json");

    RawDocumentPtr rawDoc = _extendDoc->getRawDocument();
    rawDoc->setField("usernick", "jack");
    rawDoc->setField("userid", "1");

    DocumentFactoryWrapper wrapper(_schemaPtr);
    wrapper.Init();
    DocumentParserPtr parser(wrapper.CreateDocumentParser());

    ASSERT_TRUE(dynamic_cast<KvDocumentParser*>(parser.get()));
    auto document = DYNAMIC_POINTER_CAST(KVDocument, parser->Parse(_extendDoc));
    ASSERT_TRUE(document);

    const IndexSchemaPtr& indexSchema = _schemaPtr->GetIndexSchema();
    KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());

    KVKeyExtractorPtr kvKeyExtractor(new KVKeyExtractor(kvConfig));
    uint64_t keyHash;
    kvKeyExtractor->HashKey("jack", keyHash);

    auto& kvIndexDoc = document->back();
    ASSERT_EQ(keyHash, kvIndexDoc.GetPKeyHash());
}

TEST_F(KvDocumentParserTest, testKVWithMultiRegion)
{
    _schemaPtr = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "/document_parser_multi_region_kv_schema.json");

    MultiRegionKVKeyExtractorPtr keysExtractor;
    keysExtractor.reset(new MultiRegionKVKeyExtractor(_schemaPtr));
    auto checkDoc = [this, &keysExtractor](const string& docStr, const string& pkey, regionid_t regionId) {
        IndexlibExtendDocumentPtr extendDoc = createExtendDoc(docStr, regionId);
        DocumentFactoryWrapper wrapper(_schemaPtr);
        wrapper.Init();
        DocumentParserPtr parser(wrapper.CreateDocumentParser());

        ASSERT_TRUE(dynamic_cast<KvDocumentParser*>(parser.get()));
        auto document = DYNAMIC_POINTER_CAST(KVDocument, parser->Parse(extendDoc));
        ASSERT_TRUE(document);

        // check hash
        uint64_t keyHash;
        keysExtractor->HashKey(pkey, keyHash, regionId);

        KVIndexDocument& kvIndexDoc = document->back();
        ASSERT_EQ(keyHash, kvIndexDoc.GetPKeyHash());

        ASSERT_FALSE(kvIndexDoc.GetValue().empty());
        ASSERT_EQ(regionId, kvIndexDoc.GetRegionId());
    };
    checkDoc("usernick:jack;userid:1;region_name:region1", "jack", 0);
    checkDoc("usernick:me;userid:2;region_name:region2", "2", 1);
}

TEST_F(KvDocumentParserTest, testKVAddFieldLatencyTag)
{
    _extendDoc->getRawDocument()->setDocOperateType(ADD_DOC);
    _schemaPtr = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "/document_parser_kv_schema.json");

    RawDocumentPtr rawDoc = _extendDoc->getRawDocument();
    rawDoc->setField("usernick", "jack");
    rawDoc->setField("userid", "123");
    rawDoc->setField("timestamp", "10000");

    DocumentFactoryWrapper wrapper(_schemaPtr);
    wrapper.Init();
    DocumentParserPtr parser(wrapper.CreateDocumentParser());

    ASSERT_TRUE(dynamic_cast<KvDocumentParser*>(parser.get()));
    auto document = DYNAMIC_POINTER_CAST(KVDocument, parser->Parse(_extendDoc));
    ASSERT_TRUE(document);
    ASSERT_EQ("source_1|10000;", document->GetTag("_source_timestamp_tag_"));
}

IndexlibExtendDocumentPtr KvDocumentParserTest::createExtendDoc(const string& fieldMap, regionid_t regionId)
{
    IndexlibExtendDocumentPtr extendDoc(new IndexlibExtendDocument());
    RawDocumentPtr rawDoc(new DefaultRawDocument);
    vector<string> fields = StringTokenizer::tokenize(StringView(fieldMap), ";");
    for (size_t i = 0; i < fields.size(); ++i) {
        vector<string> kv = StringTokenizer::tokenize(StringView(fields[i]), ":");
        rawDoc->setField(kv[0], kv[1]);
    }
    rawDoc->setDocOperateType(ADD_DOC);
    extendDoc->setRawDocument(rawDoc);
    extendDoc->setRegionId(regionId);
    return extendDoc;
}

}} // namespace indexlib::document
