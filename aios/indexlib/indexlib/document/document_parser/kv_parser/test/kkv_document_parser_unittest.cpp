#include <autil/StringUtil.h>
#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/config/index_partition_schema_maker.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/config/test/schema_loader.h"
#include "indexlib/testlib/schema_maker.h"
#include "indexlib/document/document_parser/kv_parser/kv_key_extractor.h"
#include "indexlib/document/document_parser/kv_parser/kkv_document_parser.h"
#include "indexlib/document/document_factory_wrapper.h"
#include "indexlib/document/document_parser/kv_parser/multi_region_kkv_keys_extractor.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/document/raw_document/default_raw_document.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace autil;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(testlib);

IE_NAMESPACE_BEGIN(document);

class KkvDocumentParserTest : public INDEXLIB_TESTBASE {
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


void KkvDocumentParserTest::CaseSetUp() {
    _extendDoc.reset(new IndexlibExtendDocument());
    RawDocumentPtr rawDoc(new DefaultRawDocument());
    _extendDoc->setRawDocument(rawDoc);
    _extendDoc->getRawDocument()->setDocOperateType(ADD_DOC);
}

void KkvDocumentParserTest::CaseTearDown() {}

TEST_F(KkvDocumentParserTest, testKKV) {
    _schemaPtr = SchemaLoader::LoadSchema(TEST_DATA_PATH,
            "/document_parser_kkv_schema.json");
    RawDocumentPtr rawDoc = _extendDoc->getRawDocument();
    rawDoc->setField("nick", "jack");
    rawDoc->setField("nid", "1");
    rawDoc->setField("uid", "hello");
    rawDoc->setField("stay_time", "123");

    DocumentFactoryWrapper wrapper(_schemaPtr);
    wrapper.Init();
    DocumentParserPtr parser(wrapper.CreateDocumentParser());

    ASSERT_TRUE(dynamic_cast<KkvDocumentParser*>(parser.get()));
    NormalDocumentPtr document = DYNAMIC_POINTER_CAST(
        NormalDocument, parser->Parse(_extendDoc));
    ASSERT_TRUE(document);
    ASSERT_TRUE(document->HasFormatError());

    KVIndexDocumentPtr indexDoc = document->GetKVIndexDocument();
    ASSERT_TRUE(indexDoc);
    ASSERT_TRUE(indexDoc->GetPkeyHash() != 0);
    ASSERT_TRUE(indexDoc->GetSkeyHash() != 0);

    const AttributeDocumentPtr& attrDoc = document->GetAttributeDocument();
    ASSERT_TRUE(attrDoc);
    ASSERT_EQ(1ul, attrDoc->GetPackFieldCount());
    const ConstString& packField = attrDoc->GetPackField(0);
    ASSERT_FALSE(packField.empty());
}

TEST_F(KkvDocumentParserTest, testKKVWithMultiRegion) {
    _schemaPtr = SchemaLoader::LoadSchema(TEST_DATA_PATH,
            "/document_parser_multi_region_kkv_schema.json");

    MultiRegionKKVKeysExtractorPtr keysExtractor;
    keysExtractor.reset(new MultiRegionKKVKeysExtractor(_schemaPtr));
    auto checkDoc = [this, &keysExtractor] (
            const string &docStr, const string &pkey,
            const string &skey, regionid_t regionId)
    {
        IndexlibExtendDocumentPtr extendDoc = createExtendDoc(docStr, regionId);
        DocumentFactoryWrapper wrapper(_schemaPtr);
        wrapper.Init();
        DocumentParserPtr parser(wrapper.CreateDocumentParser());

        ASSERT_TRUE(dynamic_cast<KkvDocumentParser*>(parser.get()));
        NormalDocumentPtr document = DYNAMIC_POINTER_CAST(
            NormalDocument, parser->Parse(extendDoc));
        ASSERT_TRUE(document);
        ASSERT_FALSE(document->HasFormatError());

        KVIndexDocumentPtr indexDoc = document->GetKVIndexDocument();
        ASSERT_TRUE(indexDoc);
        //check hash
        uint64_t pkeyHash;
        keysExtractor->HashPrefixKey(pkey, pkeyHash, regionId);
        uint64_t skeyHash;
        keysExtractor->HashSuffixKey(skey, skeyHash, regionId);
        ASSERT_TRUE(indexDoc->GetPkeyHash() == pkeyHash);
        ASSERT_TRUE(indexDoc->GetSkeyHash() == skeyHash);
        //check attribute
        const AttributeDocumentPtr& attrDoc = document->GetAttributeDocument();
        ASSERT_TRUE(attrDoc);
        ASSERT_EQ(1ul, attrDoc->GetPackFieldCount());
        const ConstString& packField = attrDoc->GetPackField(0);
        ASSERT_FALSE(packField.empty());
        ASSERT_EQ(regionId, document->GetRegionId());
    };
    checkDoc("nick:jack;id:1;nid:2;region_name:region1", "1", "2", 0);
    checkDoc("timestamp:123455;id:1;nid:2;region_name:region2", "2", "1", 1);
}

TEST_F(KkvDocumentParserTest, testSetPrimaryKeyFieldForKKV) {
    string fields = "pkey:int32;skey:int32;value:int32";
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeKKVSchema(fields, "pkey", "skey", "value");

    RawDocumentPtr rawDoc = _extendDoc->getRawDocument();
    rawDoc->setField("pkey", "1");
    rawDoc->setField("skey", "3");

    DocumentFactoryWrapper wrapper(schema);
    wrapper.Init();
    DocumentParserPtr parser(wrapper.CreateDocumentParser());

    ASSERT_TRUE(dynamic_cast<KkvDocumentParser*>(parser.get()));
    NormalDocumentPtr document = DYNAMIC_POINTER_CAST(
        NormalDocument, parser->Parse(_extendDoc));
    ASSERT_TRUE(document);

    KVIndexDocumentPtr indexDoc = document->GetKVIndexDocument();
    ASSERT_TRUE(indexDoc);
    ASSERT_EQ((uint64_t)1, indexDoc->GetPkeyHash());
    ASSERT_EQ((uint64_t)3, indexDoc->GetSkeyHash());
}


IndexlibExtendDocumentPtr KkvDocumentParserTest::createExtendDoc(
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
