#include <autil/StringUtil.h>
#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/config/index_partition_schema_maker.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/config/test/schema_loader.h"
#include "indexlib/testlib/schema_maker.h"
#include "indexlib/document/document_parser/kv_parser/multi_region_kv_key_extractor.h"
#include "indexlib/document/document_parser/kv_parser/kv_document_parser.h"
#include "indexlib/document/raw_document/default_raw_document.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/document/document_factory_wrapper.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace autil;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(document);

class KvDocumentParserTest : public INDEXLIB_TESTBASE {
public:
    void CaseSetUp();
    void CaseTearDown();

private:
    IndexlibExtendDocumentPtr createExtendDoc(
            const std::string &fieldMap,
            regionid_t regionId = DEFAULT_REGIONID);
protected:
    IndexlibExtendDocumentPtr _extendDoc;
    config::IndexPartitionSchemaPtr _schemaPtr;
};


void KvDocumentParserTest::CaseSetUp() {
    _extendDoc.reset(new IndexlibExtendDocument());
    RawDocumentPtr rawDoc(new DefaultRawDocument());
    _extendDoc->setRawDocument(rawDoc);
    _extendDoc->getRawDocument()->setDocOperateType(ADD_DOC);
}

void KvDocumentParserTest::CaseTearDown() {}

TEST_F(KvDocumentParserTest, testKV) {
    _schemaPtr = SchemaLoader::LoadSchema(TEST_DATA_PATH,
            "/document_parser_kv_schema.json");

    RawDocumentPtr rawDoc = _extendDoc->getRawDocument();
    rawDoc->setField("usernick", "jack");
    rawDoc->setField("userid", "1");

    DocumentFactoryWrapper wrapper(_schemaPtr);
    wrapper.Init();
    DocumentParserPtr parser(wrapper.CreateDocumentParser());

    ASSERT_TRUE(dynamic_cast<KvDocumentParser*>(parser.get()));
    NormalDocumentPtr document =
        DYNAMIC_POINTER_CAST(NormalDocument, parser->Parse(_extendDoc));
    ASSERT_TRUE(document);

    const AttributeDocumentPtr& attrDoc = document->GetAttributeDocument();
    ASSERT_TRUE(attrDoc);
    ASSERT_EQ(0, attrDoc->GetPackFieldCount());

    const IndexSchemaPtr& indexSchema = _schemaPtr->GetIndexSchema();
    KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig,
            indexSchema->GetPrimaryKeyIndexConfig());

    KVKeyExtractorPtr kvKeyExtractor(new KVKeyExtractor(kvConfig));
    uint64_t keyHash;
    kvKeyExtractor->HashKey("jack", keyHash);

    const KVIndexDocumentPtr& kvIndexDoc = document->GetKVIndexDocument();
    ASSERT_TRUE(kvIndexDoc);
    ASSERT_EQ(keyHash, kvIndexDoc->GetPkeyHash());
}

TEST_F(KvDocumentParserTest, testKVWithMultiRegion) {
    _schemaPtr = SchemaLoader::LoadSchema(TEST_DATA_PATH,
            "/document_parser_multi_region_kv_schema.json");

    MultiRegionKVKeyExtractorPtr keysExtractor;
    keysExtractor.reset(new MultiRegionKVKeyExtractor(_schemaPtr));
    auto checkDoc = [this, &keysExtractor] (
            const string &docStr, const string &pkey, regionid_t regionId)
    {
        IndexlibExtendDocumentPtr extendDoc = createExtendDoc(docStr, regionId);
        DocumentFactoryWrapper wrapper(_schemaPtr);
        wrapper.Init();
        DocumentParserPtr parser(wrapper.CreateDocumentParser());

        ASSERT_TRUE(dynamic_cast<KvDocumentParser*>(parser.get()));
        NormalDocumentPtr document = DYNAMIC_POINTER_CAST(NormalDocument, parser->Parse(extendDoc));
        ASSERT_TRUE(document);

        //check hash
        uint64_t keyHash;
        keysExtractor->HashKey(pkey, keyHash, regionId);
        
        const KVIndexDocumentPtr& kvIndexDoc = document->GetKVIndexDocument();
        ASSERT_TRUE(kvIndexDoc);
        ASSERT_EQ(keyHash, kvIndexDoc->GetPkeyHash());

        //check attribute
        const AttributeDocumentPtr& attrDoc = document->GetAttributeDocument();
        ASSERT_TRUE(attrDoc);
        ASSERT_EQ(1ul, attrDoc->GetPackFieldCount());
        const ConstString& packField = attrDoc->GetPackField(0);
        ASSERT_FALSE(packField.empty());
        ASSERT_EQ(regionId, document->GetRegionId());
    };
    checkDoc("usernick:jack;userid:1;region_name:region1", "jack", 0);
    checkDoc("usernick:me;userid:2;region_name:region2", "2", 1);
}

IndexlibExtendDocumentPtr KvDocumentParserTest::createExtendDoc(
        const string &fieldMap, regionid_t regionId)
{
    IndexlibExtendDocumentPtr extendDoc(new IndexlibExtendDocument());
    RawDocumentPtr rawDoc(new DefaultRawDocument);
    vector<string> fields = StringTokenizer::tokenize(ConstString(fieldMap), ";");
    for (size_t i = 0; i < fields.size(); ++i) {
        vector<string> kv = StringTokenizer::tokenize(ConstString(fields[i]), ":");
        rawDoc->setField(kv[0], kv[1]);
    }
    rawDoc->setDocOperateType(ADD_DOC);
    extendDoc->setRawDocument(rawDoc);
    extendDoc->setRegionId(regionId);
    return extendDoc;
}

IE_NAMESPACE_END(document);
