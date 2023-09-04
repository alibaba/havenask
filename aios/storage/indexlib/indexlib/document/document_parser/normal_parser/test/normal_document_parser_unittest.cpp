#include "autil/StringUtil.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/common/field_format/attribute/line_attribute_convertor.h"
#include "indexlib/common/field_format/attribute/location_attribute_convertor.h"
#include "indexlib/common/field_format/attribute/polygon_attribute_convertor.h"
#include "indexlib/common/field_format/customized_index/customized_index_field_encoder.h"
#include "indexlib/common_define.h"
#include "indexlib/config/impl/region_schema_impl.h"
#include "indexlib/config/index_partition_schema_maker.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/config/spatial_index_config.h"
#include "indexlib/config/test/schema_loader.h"
#include "indexlib/document/builtin_parser_init_param.h"
#include "indexlib/document/document_factory_wrapper.h"
#include "indexlib/document/document_parser/kv_parser/kv_key_extractor.h"
#include "indexlib/document/document_parser/normal_parser/normal_document_parser.h"
#include "indexlib/document/document_parser/normal_parser/test/tokenize_helper.h"
#include "indexlib/document/index_document/normal_document/summary_formatter.h"
#include "indexlib/document/raw_document.h"
#include "indexlib/document/raw_document/default_raw_document.h"
#include "indexlib/document/raw_document/raw_document_define.h"
#include "indexlib/index/common/field_format/spatial/SpatialFieldEncoder.h"
#include "indexlib/index/common/field_format/spatial/geo_hash/GeoHashUtil.h"
#include "indexlib/test/modify_schema_maker.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/KeyHasherTyped.h"
#include "indexlib/util/counter/AccumulativeCounter.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/counter/StateCounter.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::test;

using namespace indexlib::util;
namespace indexlib { namespace document {

class NormalDocumentParserTest : public INDEXLIB_TESTBASE
{
public:
    void CaseSetUp();
    void CaseTearDown();

private:
    enum HasherType {
        text_hasher,
        uint64_number_hasher,
    };

protected:
    void checkResult(indexlib::document::Field* field, std::string answer[], size_t secTokenCounts[],
                     section_len_t sectionLens[], pos_t posVec[], HasherType hasherType = text_hasher);

    void checkTokens(indexlib::document::Field* field, const vector<dictkey_t>& tokens);

    void checkSectionAttribute(const indexlib::document::IndexDocumentPtr& indexDocument);

    void prepare(const std::string& fieldValue);

    const StringView* getSummaryField(fieldid_t fid, const DocumentPtr& doc);

    IndexlibExtendDocumentPtr makeExtendDoc(const IndexPartitionSchemaPtr& schema, const string& docValues,
                                            string traceId = "");

private:
    IndexlibExtendDocumentPtr createExtendDoc(const std::string& fieldMap, regionid_t regionId = DEFAULT_REGIONID);
    void CheckModifiedToken(const std::pair<uint64_t, ModifiedTokens::Operation>& actualModifiedToken,
                            ModifiedTokens::Operation expectedOp, const std::string& expectedUnhashedToken);

protected:
    ClassifiedDocumentPtr _classifiedDocument;
    IndexlibExtendDocumentPtr _extendDoc;
    config::IndexPartitionSchemaPtr _schemaPtr;
    TokenizeHelperPtr _tokenizeHelper;
    SearchSummaryDocumentPtr _searchSummaryDoc;

private:
    KeyMapManagerPtr _hashMapManager;
};

void NormalDocumentParserTest::CaseSetUp()
{
    _hashMapManager.reset(new KeyMapManager());
    _schemaPtr = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "/document_parser_simple_schema.json");

    _extendDoc.reset(new IndexlibExtendDocument());
    RawDocumentPtr rawDoc(new DefaultRawDocument(_hashMapManager));
    _extendDoc->setRawDocument(rawDoc);
    _extendDoc->getRawDocument()->setDocOperateType(ADD_DOC);
    _classifiedDocument = _extendDoc->getClassifiedDocument();

    _tokenizeHelper.reset(new TokenizeHelper);
    _tokenizeHelper->init(_schemaPtr);
}

void NormalDocumentParserTest::CaseTearDown() {}

TEST_F(NormalDocumentParserTest, testSimpleProcess)
{
    string fieldValue = "title:A b# c d e,nid:1";
    prepare(fieldValue);

    string answer[] = {"a", "b", "c", "d", "e"};
    section_len_t sectionLens[] = {7};
    size_t secTokenCounts[] = {5};
    pos_t posVec[] = {0, 1, 2, 1, 1};

    DocumentFactoryWrapper wrapper(_schemaPtr);
    wrapper.Init();
    DocumentParserPtr parser(wrapper.CreateDocumentParser());

    ASSERT_TRUE(dynamic_cast<NormalDocumentParser*>(parser.get()));
    DocumentPtr doc = parser->Parse(_extendDoc);
    ASSERT_TRUE(doc);

    NormalDocumentPtr document = DYNAMIC_POINTER_CAST(NormalDocument, doc);
    fieldid_t fieldId = _schemaPtr->GetFieldId("title");
    Field* resultField = document->GetIndexDocument()->GetField(fieldId);
    checkResult(resultField, answer, secTokenCounts, sectionLens, posVec);
    checkSectionAttribute(document->GetIndexDocument());
    ASSERT_TRUE(document->GetSummaryDocument());
    ASSERT_FALSE(document->NeedTrace());
}

TEST_F(NormalDocumentParserTest, testConvertError)
{
    string fieldValue = "title:A b# c d e,nid:a";
    prepare(fieldValue);

    CounterMapPtr counterMap(new CounterMap);
    BuiltInParserInitResource resource;
    resource.counterMap = counterMap;
    BuiltInParserInitParamPtr initParam(new BuiltInParserInitParam);
    initParam->SetResource(resource);

    IndexPartitionSchemaPtr schema =
        SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "/schema/document_parser_simple_schema.json");

    DocumentFactoryWrapper wrapper(schema);
    wrapper.Init();
    DocumentParserPtr parser(wrapper.CreateDocumentParser(initParam));

    NormalDocumentParserPtr normalParser = DYNAMIC_POINTER_CAST(NormalDocumentParser, parser);
    ASSERT_TRUE(normalParser);
    {
        DocumentPtr doc = parser->Parse(_extendDoc);
        ASSERT_TRUE(doc);
        EXPECT_EQ(1, normalParser->mAttributeConvertErrorCounter->Get());
    }

    // clear
    {
        _extendDoc.reset(new IndexlibExtendDocument());
        _extendDoc->setRawDocument(RawDocumentPtr(new DefaultRawDocument(_hashMapManager)));
        string fieldValue = "title:A b# c d e,nid:1,user_id:1";
        prepare(fieldValue);
        DocumentPtr doc = parser->Parse(_extendDoc);
        ASSERT_TRUE(doc);
        EXPECT_EQ(1, normalParser->mAttributeConvertErrorCounter->Get());
    }

    // clear
    {
        _extendDoc.reset(new IndexlibExtendDocument());
        _extendDoc->setRawDocument(RawDocumentPtr(new DefaultRawDocument(_hashMapManager)));
        string fieldValue = "title:A b# c d e,nid:c,user_id:1";
        prepare(fieldValue);
        DocumentPtr doc = parser->Parse(_extendDoc);
        ASSERT_TRUE(doc);
        EXPECT_EQ(2, normalParser->mAttributeConvertErrorCounter->Get());
    }

    // clear
    {
        // pk is invalid
        _extendDoc.reset(new IndexlibExtendDocument());
        _extendDoc->setRawDocument(RawDocumentPtr(new DefaultRawDocument(_hashMapManager)));
        string fieldValue = "title:A b# c d e,user_id:c";
        prepare(fieldValue);
        DocumentPtr doc = parser->Parse(_extendDoc);
        ASSERT_FALSE(doc);
        EXPECT_EQ(3, normalParser->mAttributeConvertErrorCounter->Get());
    }
}

TEST_F(NormalDocumentParserTest, testShapeIndex)
{
    RawDocumentPtr rawDoc = _extendDoc->getRawDocument();
    string lineStr = "0.1 30.1,30.1 30.1,30.1 0.1";
    string polygonStr = "0.1 30.1,30.1 30.1,30.1 0.1,0.1 0.1,0.1 30.1";
    rawDoc->setField("line", lineStr);
    rawDoc->setField("polygon", polygonStr);
    rawDoc->setField("nid", "1");
    ASSERT_TRUE(_tokenizeHelper->process(_extendDoc));

    DocumentFactoryWrapper wrapper(_schemaPtr);
    wrapper.Init();
    DocumentParserPtr parser(wrapper.CreateDocumentParser());
    ASSERT_TRUE(dynamic_cast<NormalDocumentParser*>(parser.get()));
    DocumentPtr doc = parser->Parse(_extendDoc);
    ASSERT_TRUE(doc);

    NormalDocumentPtr document = DYNAMIC_POINTER_CAST(NormalDocument, doc);

    fieldid_t lineFieldId = _schemaPtr->GetFieldConfig("line")->GetFieldId();
    fieldid_t polygonFieldId = _schemaPtr->GetFieldConfig("polygon")->GetFieldId();

    {
        // check index tokens
        index::SpatialFieldEncoder encoder;
        auto indexConfigIter = _schemaPtr->GetIndexSchema()->CreateIterator();
        std::vector<std::shared_ptr<indexlibv2::config::IIndexConfig>> indexConfigs(indexConfigIter->Begin(),
                                                                                    indexConfigIter->End());
        encoder.Init<config::SpatialIndexConfig>(indexConfigs);

        vector<dictkey_t> lineIndexTokens;
        vector<dictkey_t> polygonIndexTokens;
        encoder.Encode(lineFieldId, lineStr, lineIndexTokens);
        encoder.Encode(polygonFieldId, polygonStr, polygonIndexTokens);
        auto* field = document->GetIndexDocument()->GetField(lineFieldId);
        checkTokens(field, lineIndexTokens);
        field = document->GetIndexDocument()->GetField(polygonFieldId);
        checkTokens(field, polygonIndexTokens);
    }
    {
        // check attribute
        StringView value = document->GetAttributeDocument()->GetField(lineFieldId);
        string fieldValue(value.data(), value.size());
        LineAttributeConvertor lineConvertor;
        string expectResult = lineConvertor.Encode(lineStr);
        ASSERT_EQ(expectResult, fieldValue);

        value = document->GetAttributeDocument()->GetField(polygonFieldId);
        fieldValue = string(value.data(), value.size());
        PolygonAttributeConvertor polygonConvertor;
        expectResult = lineConvertor.Encode(polygonStr);
        ASSERT_EQ(expectResult, fieldValue);
    }
}

TEST_F(NormalDocumentParserTest, testSpatialIndex)
{
    RawDocumentPtr rawDoc = _extendDoc->getRawDocument();
    rawDoc->setField("location", "180.0 30.0140.1 31.1");
    rawDoc->setField("nid", "1");
    ASSERT_TRUE(_tokenizeHelper->process(_extendDoc));
    DocumentFactoryWrapper wrapper(_schemaPtr);
    wrapper.Init();
    DocumentParserPtr parser(wrapper.CreateDocumentParser());
    ASSERT_TRUE(dynamic_cast<NormalDocumentParser*>(parser.get()));
    DocumentPtr doc = parser->Parse(_extendDoc);
    ASSERT_TRUE(doc);
    NormalDocumentPtr document = DYNAMIC_POINTER_CAST(NormalDocument, doc);

    int8_t detailLevel = index::GeoHashUtil::DistanceToLevel(20);
    int8_t topLevel = index::GeoHashUtil::DistanceToLevel(10000);
    std::vector<uint64_t> terms = index::GeoHashUtil::Encode(180.0, 30.0, topLevel, detailLevel);

    string answer[10];
    ASSERT_EQ((size_t)5, terms.size());
    for (size_t i = 0; i < terms.size(); i++) {
        answer[i] = StringUtil::toString(terms[i]);
    }

    terms = index::GeoHashUtil::Encode(140.1, 31.1, topLevel, detailLevel);
    ASSERT_EQ((size_t)5, terms.size());
    for (size_t i = 0; i < terms.size(); i++) {
        answer[5 + i] = StringUtil::toString(terms[i]);
    }

    section_len_t sectionLens[] = {0};
    size_t secTokenCounts[] = {10};
    pos_t posVec[] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    fieldid_t fieldId = _schemaPtr->GetFieldId("location");
    Field* resultField = document->GetIndexDocument()->GetField(fieldId);

    checkResult(resultField, answer, secTokenCounts, sectionLens, posVec, /*hasher_type=*/uint64_number_hasher);
    fieldid_t locationFieldId = _schemaPtr->GetFieldId("location");
    StringView value = document->GetAttributeDocument()->GetField(locationFieldId);
    string fieldValue(value.data(), value.size());
    LocationAttributeConvertor convertor;
    string expectResult = convertor.Encode("180.0 30.0140.1 31.1");
    ASSERT_EQ(expectResult, fieldValue);
}

TEST_F(NormalDocumentParserTest, testCustomizedIndex)
{
    RawDocumentPtr rawDoc = _extendDoc->getRawDocument();
    rawDoc->setField("nid", "1");
    rawDoc->setField("tags", "13");
    rawDoc->setField("embedding", "0.3210.1230.999");

    ASSERT_TRUE(_tokenizeHelper->process(_extendDoc));
    DocumentFactoryWrapper wrapper(_schemaPtr);
    wrapper.Init();
    DocumentParserPtr parser(wrapper.CreateDocumentParser());
    ASSERT_TRUE(dynamic_cast<NormalDocumentParser*>(parser.get()));
    DocumentPtr doc = parser->Parse(_extendDoc);
    ASSERT_TRUE(doc);
    NormalDocumentPtr document = DYNAMIC_POINTER_CAST(NormalDocument, doc);

    fieldid_t tagsId = _schemaPtr->GetFieldSchema()->GetFieldId("tags");
    Field* tagsField = document->GetIndexDocument()->GetField(tagsId);
    vector<dictkey_t> expectTags = {1, 3};
    checkTokens(tagsField, expectTags);

    CustomizedIndexFieldEncoder encoder(_schemaPtr);
    fieldid_t embId = _schemaPtr->GetFieldSchema()->GetFieldId("embedding");
    Field* embField = document->GetIndexDocument()->GetField(embId);

    vector<dictkey_t> expectEmb;
    vector<dictkey_t> dictKeys;
    encoder.Encode(embId, "0.321", dictKeys);
    expectEmb.push_back(dictKeys[0]);
    encoder.Encode(embId, "0.123", dictKeys);
    expectEmb.push_back(dictKeys[0]);
    encoder.Encode(embId, "0.999", dictKeys);
    expectEmb.push_back(dictKeys[0]);
    checkTokens(embField, expectEmb);
}

TEST_F(NormalDocumentParserTest, testSectionAttributeForNonAddDocType)
{
    string fieldValue = "title:A b# c d e,nid:1";
    {
        tearDown();
        setUp();
        _extendDoc->getRawDocument()->setDocOperateType(UPDATE_FIELD);
        prepare(fieldValue);

        DocumentFactoryWrapper wrapper(_schemaPtr);
        wrapper.Init();
        DocumentParserPtr parser(wrapper.CreateDocumentParser());
        ASSERT_TRUE(dynamic_cast<NormalDocumentParser*>(parser.get()));
        DocumentPtr doc = parser->Parse(_extendDoc);
        ASSERT_TRUE(doc);
        NormalDocumentPtr document = DYNAMIC_POINTER_CAST(NormalDocument, doc);
        ASSERT_EQ((indexid_t)INVALID_INDEXID, document->GetIndexDocument()->GetMaxIndexIdInSectionAttribute());
    }

    {
        tearDown();
        setUp();
        _extendDoc->getRawDocument()->setDocOperateType(DELETE_DOC);
        prepare(fieldValue);

        DocumentFactoryWrapper wrapper(_schemaPtr);
        wrapper.Init();
        DocumentParserPtr parser(wrapper.CreateDocumentParser());
        ASSERT_TRUE(dynamic_cast<NormalDocumentParser*>(parser.get()));
        DocumentPtr doc = parser->Parse(_extendDoc);
        ASSERT_TRUE(doc);
        NormalDocumentPtr document = DYNAMIC_POINTER_CAST(NormalDocument, doc);
        ASSERT_EQ((indexid_t)INVALID_INDEXID, document->GetIndexDocument()->GetMaxIndexIdInSectionAttribute());
    }

    {
        tearDown();
        setUp();
        _extendDoc->getRawDocument()->setDocOperateType(DELETE_SUB_DOC);
        prepare(fieldValue);

        DocumentFactoryWrapper wrapper(_schemaPtr);
        wrapper.Init();
        DocumentParserPtr parser(wrapper.CreateDocumentParser());
        ASSERT_TRUE(dynamic_cast<NormalDocumentParser*>(parser.get()));
        DocumentPtr doc = parser->Parse(_extendDoc);
        ASSERT_TRUE(doc);
        NormalDocumentPtr document = DYNAMIC_POINTER_CAST(NormalDocument, doc);
        ASSERT_EQ((indexid_t)INVALID_INDEXID, document->GetIndexDocument()->GetMaxIndexIdInSectionAttribute());
    }

    {
        tearDown();
        setUp();
        string newFieldValue = "nid:1,user_id:12345";
        _extendDoc->getRawDocument()->setDocOperateType(DELETE_DOC);
        prepare(newFieldValue);

        DocumentFactoryWrapper wrapper(_schemaPtr);
        wrapper.Init();
        auto regionSchema = _schemaPtr->GetRegionSchema(DEFAULT_REGIONID);
        regionSchema->mImpl->mOrderPreservingField = "user_id";
        DocumentParserPtr parser(wrapper.CreateDocumentParser());
        ASSERT_TRUE(dynamic_cast<NormalDocumentParser*>(parser.get()));
        DocumentPtr doc = parser->Parse(_extendDoc);
        ASSERT_TRUE(doc);
        NormalDocumentPtr document = DYNAMIC_POINTER_CAST(NormalDocument, doc);
        const AttributeDocumentPtr& attrDoc = document->GetAttributeDocument();
        cerr << "size = " << attrDoc->GetNotEmptyFieldCount() << endl;
        ASSERT_NE(0, attrDoc->GetNotEmptyFieldCount());
    }
}

TEST_F(NormalDocumentParserTest, testNonUpdateField)
{
    _extendDoc->getRawDocument()->setDocOperateType(ADD_DOC);
    string fieldValue = "content:A b# c d e,user_id:1234,nid:123,multi_string:111222333,auction_type:sale";
    prepare(fieldValue);

    DocumentFactoryWrapper wrapper(_schemaPtr);
    wrapper.Init();
    DocumentParserPtr parser(wrapper.CreateDocumentParser());
    ASSERT_TRUE(dynamic_cast<NormalDocumentParser*>(parser.get()));
    DocumentPtr doc = parser->Parse(_extendDoc);
    ASSERT_TRUE(doc);
    NormalDocumentPtr document = DYNAMIC_POINTER_CAST(NormalDocument, doc);
    ASSERT_EQ(string("123"), document->GetIndexDocument()->GetPrimaryKey());

    fieldid_t fieldId = _schemaPtr->GetFieldId("content");
    const StringView* summaryFieldValue = getSummaryField(fieldId, document);
    EXPECT_TRUE(summaryFieldValue && !summaryFieldValue->empty());

    fieldid_t pkFieldId = _schemaPtr->GetFieldId("nid");
    EXPECT_TRUE(document->GetIndexDocument()->GetField(pkFieldId));
    EXPECT_FALSE(document->GetAttributeDocument()->GetField(pkFieldId).empty());

    fieldId = _schemaPtr->GetFieldId("user_id");
    EXPECT_FALSE(document->GetAttributeDocument()->GetField(fieldId).empty());

    fieldId = _schemaPtr->GetFieldId("multi_string");
    summaryFieldValue = getSummaryField(fieldId, document);
    EXPECT_TRUE(!summaryFieldValue || summaryFieldValue->empty());
    EXPECT_FALSE(document->GetAttributeDocument()->GetField(fieldId).empty());

    fieldId = _schemaPtr->GetFieldId("auction_type");
    summaryFieldValue = getSummaryField(fieldId, document);
    EXPECT_TRUE(summaryFieldValue && !summaryFieldValue->empty());
    EXPECT_TRUE(document->GetAttributeDocument()->GetField(fieldId).empty());
}

TEST_F(NormalDocumentParserTest, testUpdateField)
{
    _extendDoc->getRawDocument()->setDocOperateType(UPDATE_FIELD);

    string fieldValue = "content:A b# c d e,user_id:1234,nid:123,multi_string:111222333";
    prepare(fieldValue);

    DocumentFactoryWrapper wrapper(_schemaPtr);
    wrapper.Init();
    DocumentParserPtr parser(wrapper.CreateDocumentParser());
    ASSERT_TRUE(dynamic_cast<NormalDocumentParser*>(parser.get()));
    DocumentPtr doc = parser->Parse(_extendDoc);
    ASSERT_TRUE(doc);
    NormalDocumentPtr document = DYNAMIC_POINTER_CAST(NormalDocument, doc);

    fieldid_t fieldId = _schemaPtr->GetFieldId("content");
    const StringView* summaryFieldValue = getSummaryField(fieldId, document);
    EXPECT_TRUE(!summaryFieldValue || summaryFieldValue->empty());
    EXPECT_TRUE(document->GetAttributeDocument()->GetField(fieldId).empty());

    fieldid_t pkFieldId = _schemaPtr->GetFieldId("nid");
    EXPECT_TRUE(document->GetIndexDocument()->GetField(pkFieldId));
    EXPECT_TRUE(document->GetAttributeDocument()->GetField(pkFieldId).empty());

    StringView constStr;
    string fieldString;
    FieldConfigPtr fieldConfig;
    fieldId = _schemaPtr->GetFieldId("user_id");
    fieldConfig = _schemaPtr->GetFieldConfig(fieldId);
    AttributeConvertorPtr convertor;
    convertor.reset(AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(
        _schemaPtr->GetAttributeSchema()->GetAttributeConfigByFieldId(fieldId)));
    constStr = document->GetAttributeDocument()->GetField(fieldId);
    fieldString = string(constStr.data(), constStr.size());
    string expectedValue;
    expectedValue = convertor->Encode(string("1234"));
    EXPECT_EQ(expectedValue, fieldString);

    fieldId = _schemaPtr->GetFieldId("multi_string");
    fieldConfig = _schemaPtr->GetFieldConfig(fieldId);
    summaryFieldValue = getSummaryField(fieldId, document);
    EXPECT_TRUE(!summaryFieldValue || summaryFieldValue->empty());
    EXPECT_TRUE(document->GetAttributeDocument()->GetField(fieldId).empty());
}

TEST_F(NormalDocumentParserTest, testSetFieldValue)
{
    string fieldValue = "nid:1,content:A b# c d e,user_id:1234,multi_string:111222333";
    prepare(fieldValue);

    DocumentFactoryWrapper wrapper(_schemaPtr);
    wrapper.Init();
    DocumentParserPtr parser(wrapper.CreateDocumentParser());
    ASSERT_TRUE(dynamic_cast<NormalDocumentParser*>(parser.get()));
    DocumentPtr doc = parser->Parse(_extendDoc);
    ASSERT_TRUE(doc);
    NormalDocumentPtr document = DYNAMIC_POINTER_CAST(NormalDocument, doc);

    fieldid_t fieldId = _schemaPtr->GetFieldId("content");
    const StringView* summaryFieldValue = getSummaryField(fieldId, document);
    string fieldString = string(summaryFieldValue->data(), summaryFieldValue->size());
    ASSERT_EQ(string("A\t \tb\t#\t \tc\t \td\t \te"), fieldString);

    FieldConfigPtr fieldConfig;
    fieldId = _schemaPtr->GetFieldId("user_id");
    fieldConfig = _schemaPtr->GetFieldConfig(fieldId);
    AttributeConvertorPtr convertor;
    convertor.reset(AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(
        _schemaPtr->GetAttributeSchema()->GetAttributeConfigByFieldId(fieldId)));
    StringView constStr = document->GetAttributeDocument()->GetField(fieldId);
    fieldString = string(constStr.data(), constStr.size());
    string expectedValue;
    expectedValue = convertor->Encode(string("1234"));
    ASSERT_EQ(expectedValue, fieldString);

    fieldId = _schemaPtr->GetFieldId("multi_string");
    fieldConfig = _schemaPtr->GetFieldConfig(fieldId);
    summaryFieldValue = getSummaryField(fieldId, document);
    EXPECT_TRUE(!summaryFieldValue || summaryFieldValue->empty());

    convertor.reset(AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(
        _schemaPtr->GetAttributeSchema()->GetAttributeConfigByFieldId(fieldId)));
    constStr = document->GetAttributeDocument()->GetField(fieldId);
    fieldString = string(constStr.data(), constStr.size());
    expectedValue = convertor->Encode(string("111222333"));
    ASSERT_EQ(expectedValue, fieldString);

    string answer[] = {"111", "222", "333"};
    section_len_t sectionLens[] = {4};
    size_t secTokenCounts[] = {3};
    pos_t posVec[] = {0, 1, 1};
    Field* resultField = document->GetIndexDocument()->GetField(fieldId);
    checkResult(resultField, answer, secTokenCounts, sectionLens, posVec);
    checkSectionAttribute(document->GetIndexDocument());
}

void NormalDocumentParserTest::prepare(const std::string& fieldValue)
{
    RawDocumentPtr rawDoc = _extendDoc->getRawDocument();
    vector<vector<string>> splitValue;
    StringUtil::fromString(fieldValue, splitValue, ":", ",");
    for (size_t i = 0; i < splitValue.size(); ++i) {
        assert(splitValue[i].size() == 2);
        rawDoc->setField(splitValue[i][0], splitValue[i][1]);
    }
    ASSERT_TRUE(_tokenizeHelper->process(_extendDoc));
}

IndexlibExtendDocumentPtr NormalDocumentParserTest::makeExtendDoc(const IndexPartitionSchemaPtr& schema,
                                                                  const string& docValues, string traceId)
{
    IndexlibExtendDocumentPtr extDoc(new IndexlibExtendDocument);
    RawDocumentPtr rawDoc(new DefaultRawDocument(_hashMapManager));
    extDoc->setRawDocument(rawDoc);

    vector<vector<string>> splitValue;
    StringUtil::fromString(docValues, splitValue, ":", ",");
    for (size_t i = 0; i < splitValue.size(); ++i) {
        assert(splitValue[i].size() == 2);
        rawDoc->setField(splitValue[i][0], splitValue[i][1]);
    }
    if (!traceId.empty()) {
        rawDoc->setField(BUILTIN_KEY_TRACE_ID, traceId);
    }
    TokenizeHelper tokenizeHelper;
    tokenizeHelper.init(schema);
    bool ret = tokenizeHelper.process(extDoc);
    assert(ret);
    (void)ret;
    return extDoc;
}

void NormalDocumentParserTest::checkTokens(Field* field, const vector<dictkey_t>& tokens)
{
    IndexTokenizeField* indexTokenizeField = dynamic_cast<IndexTokenizeField*>(field);
    assert(indexTokenizeField);
    int tokenIdx = 0;
    for (auto secIt = indexTokenizeField->Begin(); secIt != indexTokenizeField->End(); ++secIt) {
        Section* section = *secIt;
        for (int32_t sectionTokenIdx = 0; sectionTokenIdx < (int32_t)section->GetTokenCount();
             ++sectionTokenIdx, ++tokenIdx) {
            const indexlib::document::Token* token = section->GetToken(sectionTokenIdx);
            ASSERT_EQ(tokens[tokenIdx], token->GetHashKey()) << tokenIdx;
        }
    }
    ASSERT_EQ(tokenIdx, tokens.size());
}

void NormalDocumentParserTest::checkResult(Field* field, string answer[], size_t secTokenCounts[],
                                           section_len_t sectionLens[], pos_t posVec[], HasherType hasherType)
{
    IndexTokenizeField* indexTokenizeField = dynamic_cast<IndexTokenizeField*>(field);
    assert(indexTokenizeField);
    int secIdx = 0;
    int tokenIdx = 0;
    for (auto secIt = indexTokenizeField->Begin(); secIt != indexTokenizeField->End(); ++secIt, ++secIdx) {
        Section* section = *secIt;
        ASSERT_EQ(sectionLens[secIdx], section->GetLength());
        ASSERT_EQ(secTokenCounts[secIdx], section->GetTokenCount());

        for (int32_t sectionTokenIdx = 0; sectionTokenIdx < (int32_t)section->GetTokenCount();
             ++sectionTokenIdx, ++tokenIdx) {
            const indexlib::document::Token* token = section->GetToken(sectionTokenIdx);
            dictkey_t hashValue;
            if (hasherType == text_hasher) {
                // DefaultHasher hasher;
                TextHasher::GetHashKey(answer[tokenIdx].c_str(), answer[tokenIdx].size(), hashValue);
            } else if (hasherType == uint64_number_hasher) {
                UInt64NumberHasher::GetHashKey(answer[tokenIdx].c_str(), answer[tokenIdx].size(), hashValue);
            }

            ASSERT_EQ(hashValue, token->GetHashKey());
            ASSERT_EQ(posVec[tokenIdx], token->GetPosIncrement());
        }
    }
}

void NormalDocumentParserTest::checkSectionAttribute(const IndexDocumentPtr& indexDocument)
{
    IndexSchemaPtr indexSchema = _schemaPtr->GetIndexSchema();
    IndexSchema::Iterator it;
    for (it = indexSchema->Begin(); it != indexSchema->End(); ++it) {
        const StringView& sectionAttr = indexDocument->GetSectionAttribute((*it)->GetIndexId());
        if ((*it)->GetInvertedIndexType() == it_expack || (*it)->GetInvertedIndexType() == it_pack) {
            PackageIndexConfigPtr packIndexConfig = std::dynamic_pointer_cast<PackageIndexConfig>(*it);

            if (packIndexConfig->HasSectionAttribute()) {
                ASSERT_FALSE(sectionAttr.empty());
                continue;
            }
        }
        ASSERT_EQ(StringView::empty_instance(), sectionAttr);
    }
}

TEST_F(NormalDocumentParserTest, testSummaryFieldWithOriginalValue)
{
    // BugID=61168
    string fieldValue = "content:I am coding with C language,nid:1";
    prepare(fieldValue);

    DocumentFactoryWrapper wrapper(_schemaPtr);
    wrapper.Init();
    DocumentParserPtr parser(wrapper.CreateDocumentParser());
    ASSERT_TRUE(dynamic_cast<NormalDocumentParser*>(parser.get()));
    DocumentPtr document = parser->Parse(_extendDoc);
    ASSERT_TRUE(document);

    fieldid_t fieldId = _schemaPtr->GetFieldId("content");
    const StringView* value = getSummaryField(fieldId, document);
    string fieldString = string(value->data(), value->size());
    ASSERT_EQ(string("I\t \tam\t \tcoding\t \twith\t \tC\t \tlanguage"), fieldString);
}

TEST_F(NormalDocumentParserTest, testSetPrimaryKeyField)
{
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(schema,
                                          "pk:int32;",          // Field schema
                                          "pk:primarykey64:pk", // index schema
                                          "",                   // attribute schema
                                          "");                  // Summary schema

    RawDocumentPtr rawDoc = _extendDoc->getRawDocument();
    rawDoc->setField("pk", "1");

    DocumentFactoryWrapper wrapper(schema);
    wrapper.Init();
    DocumentParserPtr parser(wrapper.CreateDocumentParser());
    ASSERT_TRUE(dynamic_cast<NormalDocumentParser*>(parser.get()));
    DocumentPtr doc = parser->Parse(_extendDoc);
    ASSERT_TRUE(doc);
    NormalDocumentPtr document = DYNAMIC_POINTER_CAST(NormalDocument, doc);
    ASSERT_EQ(string("1"), document->GetIndexDocument()->GetPrimaryKey());
}

// TEST_F(NormalDocumentParserTest, testProcessType) {
//     ClassifiedDocumentProcessorPtr processor(new ClassifiedDocumentProcessor);
//     DocumentProcessorPtr clonedProcessor(processor->clone());
//     EXPECT_TRUE(clonedProcessor->needProcess(ADD_DOC));
//     EXPECT_TRUE(clonedProcessor->needProcess(DELETE_DOC));
//     EXPECT_TRUE(clonedProcessor->needProcess(DELETE_SUB_DOC));
//     EXPECT_TRUE(clonedProcessor->needProcess(UPDATE_FIELD));
//     EXPECT_TRUE(!clonedProcessor->needProcess(SKIP_DOC));
// }

TEST_F(NormalDocumentParserTest, testAppendPackAttributes)
{
    string fieldValue = "nid:1,in_pack_1:23,in_pack_2:I-am-packed";
    prepare(fieldValue);

    DocumentFactoryWrapper wrapper(_schemaPtr);
    wrapper.Init();
    DocumentParserPtr parser(wrapper.CreateDocumentParser());
    ASSERT_TRUE(dynamic_cast<NormalDocumentParser*>(parser.get()));
    DocumentPtr doc = parser->Parse(_extendDoc);
    ASSERT_TRUE(doc);
    NormalDocumentPtr document = DYNAMIC_POINTER_CAST(NormalDocument, doc);

    const AttributeDocumentPtr& attrDoc = document->GetAttributeDocument();
    ASSERT_TRUE(attrDoc);
    ASSERT_EQ(size_t(1), attrDoc->GetPackFieldCount());
    const StringView& packField = attrDoc->GetPackField(0);
    ASSERT_FALSE(packField.empty());
}

TEST_F(NormalDocumentParserTest, testValidateWithPkEmpty)
{
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema("name"));
    string fieldNames = "f1:long;f2:long;f3:string";
    string indexSchemaStr = "index1:NUMBER:f1;pk:PRIMARYKEY64:f2";

    string attrSchemaStr = "f1";
    indexlib::config::IndexPartitionSchemaMaker::MakeSchema(schema, fieldNames, indexSchemaStr, attrSchemaStr, "");

    IndexlibExtendDocumentPtr extendDoc(new IndexlibExtendDocument);
    DocumentFactoryWrapper wrapper(schema);
    wrapper.Init();
    DocumentParserPtr parser(wrapper.CreateDocumentParser());
    ASSERT_TRUE(dynamic_cast<NormalDocumentParser*>(parser.get()));
    // no raw doc
    ASSERT_FALSE(parser->Parse(extendDoc));

    extendDoc->setRawDocument(RawDocumentPtr(new DefaultRawDocument(_hashMapManager)));
    extendDoc->getRawDocument()->setDocOperateType(ADD_DOC);
    TokenizeHelper tokenizeHelper;
    tokenizeHelper.init(schema);
    ASSERT_TRUE(tokenizeHelper.process(extendDoc));

    // pk empty
    ASSERT_FALSE(parser->Parse(extendDoc));

    // pk not empty
    extendDoc->getRawDocument()->setField("f2", "notempty");
    ASSERT_TRUE(parser->Parse(extendDoc));
}

TEST_F(NormalDocumentParserTest, testValidateNoIndexField)
{
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema("name"));
    string fieldNames = "f1:long;f2:long;f3:string";
    string indexSchemaStr = "index1:NUMBER:f1;";
    string attrSchemaStr = "f1";
    indexlib::config::IndexPartitionSchemaMaker::MakeSchema(schema, fieldNames, indexSchemaStr, attrSchemaStr, "");

    DocumentFactoryWrapper wrapper(schema);
    wrapper.Init();
    DocumentParserPtr parser(wrapper.CreateDocumentParser());
    ASSERT_TRUE(dynamic_cast<NormalDocumentParser*>(parser.get()));

    IndexlibExtendDocumentPtr document(new IndexlibExtendDocument);

    document->setRawDocument(RawDocumentPtr(new DefaultRawDocument(_hashMapManager)));
    document->getRawDocument()->setDocOperateType(ADD_DOC);
    TokenizeHelper tokenizeHelper;
    tokenizeHelper.init(schema);
    ASSERT_TRUE(tokenizeHelper.process(document));

    // no pk, no index field
    EXPECT_FALSE(parser->Parse(document));

    // no pk, with index field
    document.reset(new IndexlibExtendDocument);
    document->setRawDocument(RawDocumentPtr(new DefaultRawDocument(_hashMapManager)));
    document->getRawDocument()->setDocOperateType(ADD_DOC);
    document->getRawDocument()->setField("f1", "123");
    ASSERT_TRUE(tokenizeHelper.process(document));
    EXPECT_TRUE(parser->Parse(document));
}

TEST_F(NormalDocumentParserTest, testValidateNonAddDoc)
{
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema("name"));
    string fieldNames = "f1:long;f2:long;f3:string";
    string indexSchemaStr = "index1:NUMBER:f1;";
    string attrSchemaStr = "f1";
    {
        // test update doc without primary key
        indexlib::config::IndexPartitionSchemaMaker::MakeSchema(schema, fieldNames, indexSchemaStr, attrSchemaStr, "");

        DocumentFactoryWrapper wrapper(schema);
        wrapper.Init();
        DocumentParserPtr parser(wrapper.CreateDocumentParser());
        ASSERT_TRUE(dynamic_cast<NormalDocumentParser*>(parser.get()));

        IndexlibExtendDocumentPtr document(new IndexlibExtendDocument);
        document->setRawDocument(RawDocumentPtr(new DefaultRawDocument(_hashMapManager)));

        document->getRawDocument()->setDocOperateType(DELETE_DOC);
        EXPECT_FALSE(parser->Parse(document));

        document->getRawDocument()->setDocOperateType(DELETE_SUB_DOC);
        EXPECT_FALSE(parser->Parse(document));

        document->getRawDocument()->setDocOperateType(UPDATE_FIELD);
        EXPECT_FALSE(parser->Parse(document));
    }
    {
        // test update doc success
        schema.reset(new IndexPartitionSchema("name"));
        indexSchemaStr = "index1:NUMBER:f1;pk:PRIMARYKEY64:f2";
        indexlib::config::IndexPartitionSchemaMaker::MakeSchema(schema, fieldNames, indexSchemaStr, attrSchemaStr, "");
        IndexlibExtendDocumentPtr document(new IndexlibExtendDocument);
        document->setRawDocument(RawDocumentPtr(new DefaultRawDocument(_hashMapManager)));

        document->getRawDocument()->setField("f2", "1234");
        document->getRawDocument()->setDocOperateType(UPDATE_FIELD);

        TokenizeHelper tokenizeHelper;
        tokenizeHelper.init(schema);
        DocumentFactoryWrapper wrapper(schema);
        wrapper.Init();
        DocumentParserPtr parser(wrapper.CreateDocumentParser());
        ASSERT_TRUE(dynamic_cast<NormalDocumentParser*>(parser.get()));
        ASSERT_TRUE(tokenizeHelper.process(document));
        EXPECT_TRUE(parser->Parse(document));

        // test error OP Type
        document.reset(new IndexlibExtendDocument);
        document->setRawDocument(RawDocumentPtr(new DefaultRawDocument(_hashMapManager)));
        document->getRawDocument()->setField("f2", "1234");
        document->getRawDocument()->setDocOperateType(DocOperateType(123));
        ASSERT_TRUE(tokenizeHelper.process(document));
        EXPECT_FALSE(parser->Parse(document));
    }
}

TEST_F(NormalDocumentParserTest, testParseNullField)
{
    string fieldNames = "f1:long::::-1:true;f2:long;f3:text";
    string indexSchemaStr = "index1:NUMBER:f1;pk:PRIMARYKEY64:f2";
    string attrSchemaStr = "f1";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(fieldNames, indexSchemaStr, attrSchemaStr, "");
    IndexlibExtendDocumentPtr doc = makeExtendDoc(schema, "f2:2,f3:abc efg");
    DocumentFactoryWrapper wrapper(schema);
    wrapper.Init();
    DocumentParserPtr parser(wrapper.CreateDocumentParser());
    ASSERT_TRUE(dynamic_cast<NormalDocumentParser*>(parser.get()));
    NormalDocumentPtr document = DYNAMIC_POINTER_CAST(NormalDocument, parser->Parse(doc));
    set<fieldid_t> nullFields = {0};
    ASSERT_EQ(nullFields, document->GetAttributeDocument()->_nullFields);

    FieldConfigPtr fieldConfig = schema->GetFieldConfig("f1");
    fieldConfig->SetNullFieldLiteralString("NULL");
    IndexlibExtendDocumentPtr doc2 = makeExtendDoc(schema, "f1:NULL,f2:2,f3:abc efg");
    NormalDocumentPtr document2 = DYNAMIC_POINTER_CAST(NormalDocument, parser->Parse(doc2));
    ASSERT_EQ(nullFields, document2->GetAttributeDocument()->_nullFields);
}

TEST_F(NormalDocumentParserTest, testMainSubDoc)
{
    string fieldNames = "f1:long;f2:long;f3:text";
    string indexSchemaStr = "index1:NUMBER:f1;pk:PRIMARYKEY64:f2";
    string attrSchemaStr = "f1";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(fieldNames, indexSchemaStr, attrSchemaStr, "");

    string subFieldNames = "sub_f1:long;sub_f2:long;sub_f3:text";
    string subIndexSchemaStr = "sub_index1:NUMBER:sub_f1;sub_pk:PRIMARYKEY64:sub_f2:number_hash";
    string subAttrSchemaStr = "sub_f1";
    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(subFieldNames, subIndexSchemaStr, subAttrSchemaStr, "");

    schema->SetSubIndexPartitionSchema(subSchema);

    IndexlibExtendDocumentPtr mainDoc = makeExtendDoc(schema, "f1:1,f2:2,f3:abc efg");
    mainDoc->addSubDocument(makeExtendDoc(subSchema, "sub_f1:11,sub_f2:12,sub_f3:11 12"));
    mainDoc->addSubDocument(makeExtendDoc(subSchema, "sub_f1:21,sub_f2:22,sub_f3:21 22"));
    mainDoc->addSubDocument(makeExtendDoc(subSchema, "sub_f1:31,sub_f3:31 32"));

    DocumentFactoryWrapper wrapper(schema);
    wrapper.Init();
    DocumentParserPtr parser(wrapper.CreateDocumentParser());
    ASSERT_TRUE(dynamic_cast<NormalDocumentParser*>(parser.get()));

    NormalDocumentPtr document = DYNAMIC_POINTER_CAST(NormalDocument, parser->Parse(mainDoc));
    EXPECT_EQ((size_t)2, document->GetSubDocuments().size());

    const NormalDocument::DocumentVector& subDocs = document->GetSubDocuments();
    NormalDocumentPtr sub1 = DYNAMIC_POINTER_CAST(NormalDocument, subDocs[0]);
    ASSERT_EQ("12", sub1->GetPrimaryKey());
    NormalDocumentPtr sub2 = DYNAMIC_POINTER_CAST(NormalDocument, subDocs[1]);
    ASSERT_EQ("22", sub2->GetPrimaryKey());
}

TEST_F(NormalDocumentParserTest, testCompitableSerialzeForModifyOperation)
{
    string fieldNames = "f1:long;f2:long;f3:text";
    string indexSchemaStr = "index1:NUMBER:f1;pk:PRIMARYKEY64:f2";
    string attrSchemaStr = "f1";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(fieldNames, indexSchemaStr, attrSchemaStr, "");

    IndexPartitionSchemaPtr oldSchema(schema->Clone());
    ModifySchemaMaker::AddModifyOperations(schema, "", "", "", "f2");

    {
        // new schema with modify operation
        IndexlibExtendDocumentPtr mainDoc = makeExtendDoc(schema, "f1:1,f2:2,f3:abc efg");
        DocumentFactoryWrapper wrapper(schema);
        wrapper.Init();
        DocumentParserPtr parser(wrapper.CreateDocumentParser());
        ASSERT_TRUE(dynamic_cast<NormalDocumentParser*>(parser.get()));
        NormalDocumentPtr document = DYNAMIC_POINTER_CAST(NormalDocument, parser->Parse(mainDoc));
        ASSERT_EQ(LEGACY_DOCUMENT_BINARY_VERSION, document->GetSerializedVersion());
    }

    {
        // old schema with out modify operation
        IndexlibExtendDocumentPtr mainDoc = makeExtendDoc(oldSchema, "f1:1,f2:2,f3:abc efg");
        DocumentFactoryWrapper wrapper(oldSchema);
        wrapper.Init();
        DocumentParserPtr parser(wrapper.CreateDocumentParser());
        ASSERT_TRUE(dynamic_cast<NormalDocumentParser*>(parser.get()));

        NormalDocumentPtr document = DYNAMIC_POINTER_CAST(NormalDocument, parser->Parse(mainDoc));
        ASSERT_EQ(6, document->GetSerializedVersion());
    }

    {
        // doc support null
        string fieldNames = "f1:long::::-1:true;f2:long;f3:text";
        IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(fieldNames, indexSchemaStr, attrSchemaStr, "");
        IndexlibExtendDocumentPtr mainDoc = makeExtendDoc(schema, "f1:1,f2:2,f3:abc efg");
        DocumentFactoryWrapper wrapper(schema);
        wrapper.Init();
        DocumentParserPtr parser(wrapper.CreateDocumentParser());
        ASSERT_TRUE(dynamic_cast<NormalDocumentParser*>(parser.get()));
        NormalDocumentPtr document = DYNAMIC_POINTER_CAST(NormalDocument, parser->Parse(mainDoc));
        ASSERT_EQ(LEGACY_DOCUMENT_BINARY_VERSION, document->GetSerializedVersion());
    }
}

TEST_F(NormalDocumentParserTest, testCompitableSerialize)
{
    string fieldNames = "f1:long;f2:long;f3:text";
    string indexSchemaStr = "index1:NUMBER:f1;pk:PRIMARYKEY64:f2";
    string attrSchemaStr = "f1";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(fieldNames, indexSchemaStr, attrSchemaStr, "");
    {
        // doc no field doc trace
        IndexlibExtendDocumentPtr mainDoc = makeExtendDoc(schema, "f1:1,f2:2,f3:abc efg");
        DocumentFactoryWrapper wrapper(schema);
        wrapper.Init();
        DocumentParserPtr parser(wrapper.CreateDocumentParser());
        ASSERT_TRUE(dynamic_cast<NormalDocumentParser*>(parser.get()));
        NormalDocumentPtr document = DYNAMIC_POINTER_CAST(NormalDocument, parser->Parse(mainDoc));
        ASSERT_EQ(6, document->GetSerializedVersion());
    }
    {
        // doc has doc trace
        IndexlibExtendDocumentPtr mainDoc = makeExtendDoc(schema, "f1:1,f2:2,f3:abc efg", "1");
        DocumentFactoryWrapper wrapper(schema);
        wrapper.Init();
        DocumentParserPtr parser(wrapper.CreateDocumentParser());
        ASSERT_TRUE(dynamic_cast<NormalDocumentParser*>(parser.get()));
        NormalDocumentPtr document = DYNAMIC_POINTER_CAST(NormalDocument, parser->Parse(mainDoc));
        ASSERT_EQ(LEGACY_DOCUMENT_BINARY_VERSION, document->GetSerializedVersion());
    }
}

void NormalDocumentParserTest::CheckModifiedToken(
    const std::pair<uint64_t, ModifiedTokens::Operation>& actualModifiedToken, ModifiedTokens::Operation expectedOp,
    const std::string& expectedUnhashedToken)
{
    dictkey_t hashValue;
    TextHasher::GetHashKey(expectedUnhashedToken.c_str(), expectedUnhashedToken.size(), hashValue);
    EXPECT_EQ(hashValue, actualModifiedToken.first);
    EXPECT_EQ(expectedOp, actualModifiedToken.second);
}

TEST_F(NormalDocumentParserTest, testModifiedFields)
{
    string fieldValue = "title:A b# c d e,__last_value__title:a f # c e g,nid:1";
    prepare(fieldValue);

    string answer[] = {"a", "b", "c", "d", "e"};
    section_len_t sectionLens[] = {7};
    size_t secTokenCounts[] = {5};
    pos_t posVec[] = {0, 1, 2, 1, 1};

    DocumentFactoryWrapper wrapper(_schemaPtr);
    wrapper.Init();
    DocumentParserPtr parser(wrapper.CreateDocumentParser());

    ASSERT_TRUE(dynamic_cast<NormalDocumentParser*>(parser.get()));
    DocumentPtr doc = parser->Parse(_extendDoc);
    ASSERT_TRUE(doc);

    NormalDocumentPtr document = DYNAMIC_POINTER_CAST(NormalDocument, doc);
    fieldid_t fieldId = _schemaPtr->GetFieldId("title");
    Field* resultField = document->GetIndexDocument()->GetField(fieldId);
    checkResult(resultField, answer, secTokenCounts, sectionLens, posVec);
    checkSectionAttribute(document->GetIndexDocument());
    ASSERT_TRUE(document->GetSummaryDocument());
    ASSERT_FALSE(document->NeedTrace());

    const ModifiedTokens* modifiedTokens = document->GetIndexDocument()->GetFieldModifiedTokens(fieldId);
    ASSERT_NE(modifiedTokens, nullptr);
    ASSERT_EQ(modifiedTokens->NonNullTermSize(), 4);
    EXPECT_EQ(modifiedTokens->FieldId(), fieldId);
    CheckModifiedToken((*modifiedTokens)[0], ModifiedTokens::Operation::ADD, "b");
    CheckModifiedToken((*modifiedTokens)[1], ModifiedTokens::Operation::ADD, "d");
    CheckModifiedToken((*modifiedTokens)[2], ModifiedTokens::Operation::REMOVE, "f");
    CheckModifiedToken((*modifiedTokens)[3], ModifiedTokens::Operation::REMOVE, "g");
}

TEST_F(NormalDocumentParserTest, testModifiedFields2)
{
    string fieldValue = "title:A b# c d e,__last_value__title:a e # c d b,nid:1";
    prepare(fieldValue);

    string answer[] = {"a", "b", "c", "d", "e"};
    section_len_t sectionLens[] = {7};
    size_t secTokenCounts[] = {5};
    pos_t posVec[] = {0, 1, 2, 1, 1};

    DocumentFactoryWrapper wrapper(_schemaPtr);
    wrapper.Init();
    DocumentParserPtr parser(wrapper.CreateDocumentParser());

    ASSERT_TRUE(dynamic_cast<NormalDocumentParser*>(parser.get()));
    DocumentPtr doc = parser->Parse(_extendDoc);
    ASSERT_TRUE(doc);

    NormalDocumentPtr document = DYNAMIC_POINTER_CAST(NormalDocument, doc);
    fieldid_t fieldId = _schemaPtr->GetFieldId("title");
    Field* resultField = document->GetIndexDocument()->GetField(fieldId);
    checkResult(resultField, answer, secTokenCounts, sectionLens, posVec);
    checkSectionAttribute(document->GetIndexDocument());
    ASSERT_TRUE(document->GetSummaryDocument());
    ASSERT_FALSE(document->NeedTrace());

    const ModifiedTokens* modifiedTokens = document->GetIndexDocument()->GetFieldModifiedTokens(fieldId);
    EXPECT_NE(nullptr, modifiedTokens);
    ASSERT_TRUE(modifiedTokens->Empty());
}

TEST_F(NormalDocumentParserTest, testAddFieldLatencyTag)
{
    string fieldValue = "title:A b# c d e,nid:1";
    prepare(fieldValue);
    RawDocumentPtr rawDoc = _extendDoc->getRawDocument();
    rawDoc->setField("source_timestamp", "10000");

    DocumentFactoryWrapper wrapper(_schemaPtr);
    wrapper.Init();
    DocumentParserPtr parser(wrapper.CreateDocumentParser());

    ASSERT_TRUE(dynamic_cast<NormalDocumentParser*>(parser.get()));
    DocumentPtr doc = parser->Parse(_extendDoc);
    ASSERT_TRUE(doc);
    ASSERT_EQ("source_1|10000;", doc->GetTag("_source_timestamp_tag_"));
}

IndexlibExtendDocumentPtr NormalDocumentParserTest::createExtendDoc(const string& fieldMap, regionid_t regionId)
{
    IndexlibExtendDocumentPtr extendDoc(new IndexlibExtendDocument());
    RawDocumentPtr rawDoc(new DefaultRawDocument(_hashMapManager));
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

const StringView* NormalDocumentParserTest::getSummaryField(fieldid_t fid, const DocumentPtr& doc)
{
    NormalDocumentPtr normalDoc = DYNAMIC_POINTER_CAST(NormalDocument, doc);
    assert(normalDoc);
    _searchSummaryDoc.reset(new SearchSummaryDocument(NULL, 20));

    SummaryFormatter formatter(_schemaPtr->GetSummarySchema());
    formatter.TEST_DeserializeSummaryDoc(normalDoc->GetSummaryDocument(), _searchSummaryDoc.get());

    return _searchSummaryDoc->GetFieldValue(_schemaPtr->GetSummarySchema()->GetSummaryFieldId(fid));
}
}} // namespace indexlib::document
