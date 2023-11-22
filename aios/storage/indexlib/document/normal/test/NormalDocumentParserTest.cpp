#include "indexlib/document/normal/NormalDocumentParser.h"

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/UnresolvedSchema.h"
#include "indexlib/document/BuiltinParserInitParam.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/document/RawDocument.h"
#include "indexlib/document/RawDocumentDefine.h"
#include "indexlib/document/normal/ModifiedTokens.h"
#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/document/normal/SummaryFormatter.h"
#include "indexlib/document/normal/test/TokenizeHelper.h"
#include "indexlib/document/raw_document/DefaultRawDocument.h"
#include "indexlib/document/raw_document/test/RawDocumentMaker.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/Constant.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertorFactory.h"
#include "indexlib/index/common/field_format/attribute/LineAttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/LocationAttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/PolygonAttributeConvertor.h"
#include "indexlib/index/common/field_format/spatial/SpatialFieldEncoder.h"
#include "indexlib/index/common/field_format/spatial/geo_hash/GeoHashUtil.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/config/PackageIndexConfig.h"
#include "indexlib/index/summary/Common.h"
#include "indexlib/table/normal_table/test/NormalTabletSchemaMaker.h"
#include "indexlib/util/KeyHasherTyped.h"
#include "indexlib/util/counter/AccumulativeCounter.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/counter/StateCounter.h"
#include "unittest/unittest.h"

using namespace std;
using namespace indexlib::util;
using namespace indexlib::document;

namespace indexlibv2 { namespace document {

class NormalDocumentParserTest : public TESTBASE
{
public:
    void setUp() override;

private:
    enum HasherType {
        text_hasher,
        uint64_number_hasher,
    };

protected:
    void checkResult(indexlib::document::Field* field, std::string answer[], size_t secTokenCounts[],
                     section_len_t sectionLens[], pos_t posVec[], HasherType hasherType = text_hasher);

    void checkTokens(indexlib::document::Field* field, const vector<dictkey_t>& tokens);

    void checkSectionAttribute(const std::shared_ptr<indexlib::document::IndexDocument>& indexDocument);

    void prepare(const std::string& fieldValue);

    const autil::StringView* getSummaryField(fieldid_t fid, const std::shared_ptr<NormalDocument>& doc);

    std::shared_ptr<NormalExtendDocument> makeExtendDoc(const std::shared_ptr<config::ITabletSchema>& schema,
                                                        const string& docValues, string traceId = "");

private:
    std::shared_ptr<NormalExtendDocument> createExtendDoc(const std::string& fieldMap);
    void CheckModifiedToken(const std::pair<uint64_t, ModifiedTokens::Operation>& actualModifiedToken,
                            ModifiedTokens::Operation expectedOp, const std::string& expectedUnhashedToken);
    std::shared_ptr<NormalDocument> ParseDocument(const std::shared_ptr<config::ITabletSchema>& schema,
                                                  const std::shared_ptr<DocumentInitParam>& param,
                                                  const std::shared_ptr<ExtendDocument>& extendDoc);
    std::shared_ptr<NormalDocument> ParseDocument(const std::shared_ptr<NormalDocumentParser>& parser,
                                                  const std::shared_ptr<ExtendDocument>& extendDoc);
    void AssertParseFail(const std::shared_ptr<config::ITabletSchema>& schema,
                         const std::shared_ptr<DocumentInitParam>& param,
                         const std::shared_ptr<ExtendDocument>& extendDoc);
    void AssertParseFail(const std::shared_ptr<NormalDocumentParser>& parser,
                         const std::shared_ptr<ExtendDocument>& extendDoc);
    void AssertParseEmpty(const std::shared_ptr<config::ITabletSchema>& schema,
                          const std::shared_ptr<DocumentInitParam>& param,
                          const std::shared_ptr<ExtendDocument>& extendDoc);
    void AssertParseEmpty(const std::shared_ptr<NormalDocumentParser>& parser,
                          const std::shared_ptr<ExtendDocument>& extendDoc);

    std::shared_ptr<index::AttributeConfig> GetAttributeConfig(fieldid_t fieldId) const;

protected:
    std::shared_ptr<ClassifiedDocument> _classifiedDocument;
    std::shared_ptr<NormalExtendDocument> _extendDoc;
    std::shared_ptr<config::TabletSchema> _schemaPtr;
    std::shared_ptr<TokenizeHelper> _tokenizeHelper;
    std::shared_ptr<SearchSummaryDocument> _searchSummaryDoc;

private:
    std::shared_ptr<KeyMapManager> _hashMapManager;
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.document, NormalDocumentParserTest);

void NormalDocumentParserTest::setUp()
{
    _hashMapManager.reset(new KeyMapManager());
    _schemaPtr.reset(
        framework::TabletSchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "document_parser_simple_schema.json")
            .release());
    ASSERT_TRUE(_schemaPtr);
    ASSERT_TRUE(framework::TabletSchemaLoader::ResolveSchema(nullptr, "", _schemaPtr.get()).IsOK());
    _schemaPtr->SetSchemaId(1);
    _extendDoc.reset(new NormalExtendDocument());
    std::shared_ptr<RawDocument> rawDoc(new DefaultRawDocument(_hashMapManager));
    _extendDoc->SetRawDocument(rawDoc);
    _extendDoc->GetRawDocument()->setDocOperateType(ADD_DOC);
    _classifiedDocument = _extendDoc->getClassifiedDocument();

    _tokenizeHelper.reset(new TokenizeHelper);
    _tokenizeHelper->init(_schemaPtr);
}

std::shared_ptr<index::AttributeConfig> NormalDocumentParserTest::GetAttributeConfig(fieldid_t fieldId) const
{
    const auto& configs = _schemaPtr->GetIndexConfigs(indexlibv2::index::ATTRIBUTE_INDEX_TYPE_STR);
    for (const auto& config : configs) {
        const auto& attrConfig = std::dynamic_pointer_cast<index::AttributeConfig>(config);
        assert(attrConfig);
        if (attrConfig->GetFieldId() == fieldId) {
            return attrConfig;
        }
    }
    return nullptr;
}

std::shared_ptr<NormalDocument>
NormalDocumentParserTest::ParseDocument(const std::shared_ptr<config::ITabletSchema>& schema,
                                        const std::shared_ptr<DocumentInitParam>& param,
                                        const std::shared_ptr<ExtendDocument>& extendDoc)
{
    auto parser = std::make_unique<NormalDocumentParser>(nullptr, false);
    auto status = parser->Init(schema, param);
    assert(status.IsOK());
    auto [status1, docBatch] = parser->Parse(*extendDoc);
    assert(status1.IsOK());
    assert(docBatch);
    assert(docBatch->GetBatchSize() == 1);
    auto doc = std::dynamic_pointer_cast<NormalDocument>((*docBatch)[0]);
    assert(doc);
    return doc;
}
std::shared_ptr<NormalDocument>
NormalDocumentParserTest::ParseDocument(const std::shared_ptr<NormalDocumentParser>& parser,
                                        const std::shared_ptr<ExtendDocument>& extendDoc)
{
    auto [status1, docBatch] = parser->Parse(*extendDoc);
    assert(status1.IsOK());
    assert(docBatch);
    assert(docBatch->GetBatchSize() == 1);
    auto doc = std::dynamic_pointer_cast<NormalDocument>((*docBatch)[0]);
    assert(doc);
    return doc;
}

void NormalDocumentParserTest::AssertParseFail(const std::shared_ptr<config::ITabletSchema>& schema,
                                               const std::shared_ptr<DocumentInitParam>& param,
                                               const std::shared_ptr<ExtendDocument>& extendDoc)
{
    auto parser = std::make_unique<NormalDocumentParser>(nullptr, false);
    auto status = parser->Init(schema, param);
    assert(status.IsOK());
    auto [status1, docBatch] = parser->Parse(*extendDoc);
    ASSERT_FALSE(status1.IsOK());
    ASSERT_FALSE(docBatch);
}

void NormalDocumentParserTest::AssertParseFail(const std::shared_ptr<NormalDocumentParser>& parser,
                                               const std::shared_ptr<ExtendDocument>& extendDoc)
{
    auto [status, docBatch] = parser->Parse(*extendDoc);
    ASSERT_FALSE(status.IsOK());
    ASSERT_FALSE(docBatch);
}

void NormalDocumentParserTest::AssertParseEmpty(const std::shared_ptr<config::ITabletSchema>& schema,
                                                const std::shared_ptr<DocumentInitParam>& param,
                                                const std::shared_ptr<ExtendDocument>& extendDoc)
{
    auto parser = std::make_unique<NormalDocumentParser>(nullptr, false);
    auto status = parser->Init(schema, param);
    assert(status.IsOK());
    auto [status1, docBatch] = parser->Parse(*extendDoc);
    ASSERT_TRUE(status1.IsOK());
    ASSERT_FALSE(docBatch);
}

void NormalDocumentParserTest::AssertParseEmpty(const std::shared_ptr<NormalDocumentParser>& parser,
                                                const std::shared_ptr<ExtendDocument>& extendDoc)
{
    auto [status, docBatch] = parser->Parse(*extendDoc);
    ASSERT_TRUE(status.IsOK());
    ASSERT_FALSE(docBatch);
}

TEST_F(NormalDocumentParserTest, testSimpleProcess)
{
    string fieldValue = "title:A b# c d e,nid:1";
    prepare(fieldValue);

    string answer[] = {"a", "b", "c", "d", "e"};
    section_len_t sectionLens[] = {7};
    size_t secTokenCounts[] = {5};
    pos_t posVec[] = {0, 1, 2, 1, 1};

    auto document = ParseDocument(_schemaPtr, nullptr, _extendDoc);
    fieldid_t fieldId = _schemaPtr->GetFieldId("title");
    Field* resultField = document->GetIndexDocument()->GetField(fieldId);
    checkResult(resultField, answer, secTokenCounts, sectionLens, posVec);
    checkSectionAttribute(document->GetIndexDocument());
    ASSERT_TRUE(document->GetSummaryDocument());
    ASSERT_TRUE(document->GetTraceId().empty());
    ASSERT_EQ(1u, document->GetSchemaId());
}

TEST_F(NormalDocumentParserTest, testConvertError)
{
    string fieldValue = "title:A b# c d e,nid:a";
    prepare(fieldValue);

    std::shared_ptr<CounterMap> counterMap(new CounterMap);
    BuiltInParserInitResource resource;
    resource.counterMap = counterMap;
    std::shared_ptr<BuiltInParserInitParam> initParam(new BuiltInParserInitParam);
    initParam->SetResource(resource);

    _schemaPtr.reset(framework::TabletSchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH() + "/schema",
                                                               "document_parser_simple_schema.json")
                         .release());
    ASSERT_TRUE(_schemaPtr);
    ASSERT_TRUE(framework::TabletSchemaLoader::ResolveSchema(nullptr, "", _schemaPtr.get()).IsOK());

    auto parser = std::make_shared<NormalDocumentParser>(nullptr, false);
    ASSERT_TRUE(parser->Init(_schemaPtr, initParam).IsOK());
    {
        ASSERT_TRUE(ParseDocument(parser, _extendDoc));
        EXPECT_EQ(1, parser->_attributeConvertErrorCounter->Get());
    }

    // clear
    {
        _extendDoc.reset(new NormalExtendDocument());
        _extendDoc->SetRawDocument(std::shared_ptr<RawDocument>(new DefaultRawDocument(_hashMapManager)));
        string fieldValue = "title:A b# c d e,nid:1,user_id:1";
        prepare(fieldValue);
        ASSERT_TRUE(ParseDocument(parser, _extendDoc));
        EXPECT_EQ(1, parser->_attributeConvertErrorCounter->Get());
    }

    // clear
    {
        _extendDoc.reset(new NormalExtendDocument());
        _extendDoc->SetRawDocument(std::shared_ptr<RawDocument>(new DefaultRawDocument(_hashMapManager)));
        string fieldValue = "title:A b# c d e,nid:c,user_id:1";
        prepare(fieldValue);
        ASSERT_TRUE(ParseDocument(parser, _extendDoc));
        EXPECT_EQ(2, parser->_attributeConvertErrorCounter->Get());
    }

    // clear
    {
        // pk is invalid
        _extendDoc.reset(new NormalExtendDocument());
        _extendDoc->SetRawDocument(std::shared_ptr<RawDocument>(new DefaultRawDocument(_hashMapManager)));
        string fieldValue = "title:A b# c d e,user_id:c";
        prepare(fieldValue);
        AssertParseEmpty(parser, _extendDoc);
        EXPECT_EQ(3, parser->_attributeConvertErrorCounter->Get());
    }
}

TEST_F(NormalDocumentParserTest, testShapeIndex)
{
    std::shared_ptr<RawDocument> rawDoc = _extendDoc->GetRawDocument();
    string lineStr = "0.1 30.1,30.1 30.1,30.1 0.1";
    string polygonStr = "0.1 30.1,30.1 30.1,30.1 0.1,0.1 0.1,0.1 30.1";
    rawDoc->setField("line", lineStr);
    rawDoc->setField("polygon", polygonStr);
    rawDoc->setField("nid", "1");
    ASSERT_TRUE(_tokenizeHelper->process(_extendDoc));

    auto document = ParseDocument(_schemaPtr, nullptr, _extendDoc);

    fieldid_t lineFieldId = _schemaPtr->GetFieldConfig("line")->GetFieldId();
    fieldid_t polygonFieldId = _schemaPtr->GetFieldConfig("polygon")->GetFieldId();

    {
        // check index tokens
        indexlib::index::SpatialFieldEncoder encoder(_schemaPtr->GetIndexConfigs("inverted_index"));
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
        autil::StringView value = document->GetAttributeDocument()->GetField(lineFieldId);
        string fieldValue(value.data(), value.size());
        indexlibv2::index::LineAttributeConvertor lineConvertor;
        string expectResult = lineConvertor.Encode(lineStr);
        ASSERT_EQ(expectResult, fieldValue);

        value = document->GetAttributeDocument()->GetField(polygonFieldId);
        fieldValue = string(value.data(), value.size());
        indexlibv2::index::PolygonAttributeConvertor polygonConvertor;
        expectResult = lineConvertor.Encode(polygonStr);
        ASSERT_EQ(expectResult, fieldValue);
    }
}

// TODO: uncomment this test when spatial index migrated to indexlibv2
// TEST_F(NormalDocumentParserTest, testSpatialIndex)
// {
//     std::shared_ptr<RawDocument> rawDoc = _extendDoc->GetRawDocument();
//     rawDoc->setField("location", "180.0 30.0140.1 31.1");
//     rawDoc->setField("nid", "1");
//     ASSERT_TRUE(_tokenizeHelper->process(_extendDoc));
//     auto document = ParseDocument(_schemaPtr, nullptr, _extendDoc);

//     std::vector<uint64_t> terms;
//     int8_t detailLevel = indexlibv2::index::GeoHashUtil::DistanceToLevel(20);
//     int8_t topLevel = indexlibv2::index::GeoHashUtil::DistanceToLevel(10000);
//     indexlibv2::index::GeoHashUtil::Encode(180.0, 30.0, terms, topLevel, detailLevel);

//     string answer[10];
//     ASSERT_EQ((size_t)5, terms.size());
//     for (size_t i = 0; i < terms.size(); i++) {
//         answer[i] = autil::StringUtil::toString(terms[i]);
//     }

//     terms.clear();
//     indexlibv2::index::GeoHashUtil::Encode(140.1, 31.1, terms, topLevel, detailLevel);
//     ASSERT_EQ((size_t)5, terms.size());
//     for (size_t i = 0; i < terms.size(); i++) {
//         answer[5 + i] = autil::StringUtil::toString(terms[i]);
//     }

//     section_len_t sectionLens[] = {0};
//     size_t secTokenCounts[] = {10};
//     pos_t posVec[] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
//     fieldid_t fieldId = _schemaPtr->GetFieldId("location");
//     Field* resultField = document->GetIndexDocument()->GetField(fieldId);

//     checkResult(resultField, answer, secTokenCounts, sectionLens, posVec, /*hasher_type=*/uint64_number_hasher);
//     fieldid_t locationFieldId = _schemaPtr->GetFieldId("location");
//     autil::StringView value = document->GetAttributeDocument()->GetField(locationFieldId);
//     string fieldValue(value.data(), value.size());
//     indexlibv2::index::LocationAttributeConvertor convertor;
//     string expectResult = convertor.Encode("180.0 30.0140.1 31.1");
//     ASSERT_EQ(expectResult, fieldValue);
// }

TEST_F(NormalDocumentParserTest, testSectionAttributeForNonAddDocType)
{
    string fieldValue = "title:A b# c d e,nid:1";
    {
        tearDown();
        setUp();
        _extendDoc->GetRawDocument()->setDocOperateType(UPDATE_FIELD);
        prepare(fieldValue);

        auto document = ParseDocument(_schemaPtr, nullptr, _extendDoc);
        ASSERT_EQ((indexid_t)INVALID_INDEXID, document->GetIndexDocument()->GetMaxIndexIdInSectionAttribute());
    }

    {
        tearDown();
        setUp();
        _extendDoc->GetRawDocument()->setDocOperateType(DELETE_DOC);
        prepare(fieldValue);

        auto document = ParseDocument(_schemaPtr, nullptr, _extendDoc);
        ASSERT_EQ((indexid_t)INVALID_INDEXID, document->GetIndexDocument()->GetMaxIndexIdInSectionAttribute());
    }

    {
        tearDown();
        setUp();
        _extendDoc->GetRawDocument()->setDocOperateType(DELETE_SUB_DOC);
        prepare(fieldValue);

        auto document = ParseDocument(_schemaPtr, nullptr, _extendDoc);
        ASSERT_EQ((indexid_t)INVALID_INDEXID, document->GetIndexDocument()->GetMaxIndexIdInSectionAttribute());
    }

    {
        tearDown();
        setUp();
        string newFieldValue = "nid:1,user_id:12345";
        _extendDoc->GetRawDocument()->setDocOperateType(DELETE_DOC);
        prepare(newFieldValue);

        ASSERT_TRUE(_schemaPtr->_impl->SetRuntimeSetting<std::string>("order_preserving_field", "user_id", true));
        auto document = ParseDocument(_schemaPtr, nullptr, _extendDoc);
        ASSERT_TRUE(document);
        const std::shared_ptr<AttributeDocument>& attrDoc = document->GetAttributeDocument();
        cerr << "size = " << attrDoc->GetNotEmptyFieldCount() << endl;
        ASSERT_NE(0, attrDoc->GetNotEmptyFieldCount());
    }
}

TEST_F(NormalDocumentParserTest, testNonUpdateField)
{
    _extendDoc->GetRawDocument()->setDocOperateType(ADD_DOC);
    string fieldValue = "content:A b# c d e,user_id:1234,nid:123,multi_string:111222333,auction_type:sale";
    prepare(fieldValue);

    auto document = ParseDocument(_schemaPtr, nullptr, _extendDoc);
    ASSERT_EQ(string("123"), document->GetIndexDocument()->GetPrimaryKey());

    fieldid_t fieldId = _schemaPtr->GetFieldId("content");
    const autil::StringView* summaryFieldValue = getSummaryField(fieldId, document);
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
    _extendDoc->GetRawDocument()->setDocOperateType(UPDATE_FIELD);

    string fieldValue = "content:A b# c d e,user_id:1234,nid:123,multi_string:111222333";
    prepare(fieldValue);

    auto document = ParseDocument(_schemaPtr, nullptr, _extendDoc);

    fieldid_t fieldId = _schemaPtr->GetFieldId("content");
    const autil::StringView* summaryFieldValue = getSummaryField(fieldId, document);
    EXPECT_TRUE(!summaryFieldValue || summaryFieldValue->empty());
    EXPECT_TRUE(document->GetAttributeDocument()->GetField(fieldId).empty());

    fieldid_t pkFieldId = _schemaPtr->GetFieldId("nid");
    EXPECT_TRUE(document->GetIndexDocument()->GetField(pkFieldId));
    EXPECT_TRUE(document->GetAttributeDocument()->GetField(pkFieldId).empty());

    autil::StringView constStr;
    string fieldString;
    std::shared_ptr<config::FieldConfig> fieldConfig;
    fieldId = _schemaPtr->GetFieldId("user_id");
    fieldConfig = _schemaPtr->GetFieldConfig(fieldId);
    std::shared_ptr<indexlibv2::index::AttributeConvertor> convertor;
    convertor.reset(
        indexlibv2::index::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(GetAttributeConfig(fieldId)));
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

    auto document = ParseDocument(_schemaPtr, nullptr, _extendDoc);
    ASSERT_TRUE(document);

    fieldid_t fieldId = _schemaPtr->GetFieldId("content");
    const autil::StringView* summaryFieldValue = getSummaryField(fieldId, document);
    string fieldString = string(summaryFieldValue->data(), summaryFieldValue->size());
    ASSERT_EQ(string("A\t \tb\t#\t \tc\t \td\t \te"), fieldString);

    std::shared_ptr<config::FieldConfig> fieldConfig;
    fieldId = _schemaPtr->GetFieldId("user_id");
    fieldConfig = _schemaPtr->GetFieldConfig(fieldId);
    std::shared_ptr<indexlibv2::index::AttributeConvertor> convertor;
    convertor.reset(
        indexlibv2::index::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(GetAttributeConfig(fieldId)));
    autil::StringView constStr = document->GetAttributeDocument()->GetField(fieldId);
    fieldString = string(constStr.data(), constStr.size());
    string expectedValue;
    expectedValue = convertor->Encode(string("1234"));
    ASSERT_EQ(expectedValue, fieldString);

    fieldId = _schemaPtr->GetFieldId("multi_string");
    fieldConfig = _schemaPtr->GetFieldConfig(fieldId);
    summaryFieldValue = getSummaryField(fieldId, document);
    EXPECT_TRUE(!summaryFieldValue || summaryFieldValue->empty());

    convertor.reset(
        indexlibv2::index::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(GetAttributeConfig(fieldId)));
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
    std::shared_ptr<RawDocument> rawDoc = _extendDoc->GetRawDocument();
    vector<vector<string>> splitValue;
    autil::StringUtil::fromString(fieldValue, splitValue, ":", ",");
    for (size_t i = 0; i < splitValue.size(); ++i) {
        assert(splitValue[i].size() == 2);
        rawDoc->setField(splitValue[i][0], splitValue[i][1]);
    }
    ASSERT_TRUE(_tokenizeHelper->process(_extendDoc));
}

std::shared_ptr<NormalExtendDocument>
NormalDocumentParserTest::makeExtendDoc(const std::shared_ptr<config::ITabletSchema>& schema, const string& docValues,
                                        string traceId)
{
    std::shared_ptr<NormalExtendDocument> extDoc(new NormalExtendDocument);
    std::shared_ptr<RawDocument> rawDoc(new DefaultRawDocument(_hashMapManager));
    extDoc->SetRawDocument(rawDoc);

    vector<vector<string>> splitValue;
    autil::StringUtil::fromString(docValues, splitValue, ":", ",");
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

void NormalDocumentParserTest::checkSectionAttribute(
    const std::shared_ptr<indexlib::document::IndexDocument>& indexDocument)
{
    const auto& indexConfigs = _schemaPtr->GetIndexConfigs(indexlib::index::INVERTED_INDEX_TYPE_STR);
    for (const auto& config : indexConfigs) {
        const auto& invertedConfig = std::dynamic_pointer_cast<config::InvertedIndexConfig>(config);
        ASSERT_TRUE(invertedConfig);
        const autil::StringView& sectionAttr = indexDocument->GetSectionAttribute(invertedConfig->GetIndexId());
        auto indexType = invertedConfig->GetInvertedIndexType();
        if (indexType == it_expack || indexType == it_pack) {
            const auto& packConfig = std::dynamic_pointer_cast<config::PackageIndexConfig>(invertedConfig);
            ASSERT_TRUE(packConfig);
            if (packConfig->HasSectionAttribute()) {
                ASSERT_FALSE(sectionAttr.empty());
                continue;
            }
        }
        ASSERT_EQ(autil::StringView::empty_instance(), sectionAttr);
    }
}

TEST_F(NormalDocumentParserTest, testSummaryFieldWithOriginalValue)
{
    // BugID=61168
    string fieldValue = "content:I am coding with C language,nid:1";
    prepare(fieldValue);

    auto document = ParseDocument(_schemaPtr, nullptr, _extendDoc);
    ASSERT_TRUE(document);

    fieldid_t fieldId = _schemaPtr->GetFieldId("content");
    const autil::StringView* value = getSummaryField(fieldId, document);
    string fieldString = string(value->data(), value->size());
    ASSERT_EQ(string("I\t \tam\t \tcoding\t \twith\t \tC\t \tlanguage"), fieldString);
}

// TODO: uncomment this test when indexlibv2 support pack attribute
// TEST_F(NormalDocumentParserTest, testAppendPackAttributes)
// {
//     string fieldValue = "nid:1,in_pack_1:23,in_pack_2:I-am-packed";
//     prepare(fieldValue);

//     auto document = ParseDocument(_schemaPtr, nullptr, _extendDoc);
//     ASSERT_TRUE(document);

//     const std::shared_ptr<AttributeDocument>& attrDoc = document->GetAttributeDocument();
//     ASSERT_TRUE(attrDoc);
//     ASSERT_EQ(size_t(1), attrDoc->GetPackFieldCount());
//     const autil::StringView& packField = attrDoc->GetPackField(0);
//     ASSERT_FALSE(packField.empty());
// }

TEST_F(NormalDocumentParserTest, testValidateWithPkEmpty)
{
    string field = "f1:long;f2:long;f3:string";
    string index = "index1:NUMBER:f1;pk:PRIMARYKEY64:f2";
    string attr = "f1";
    _schemaPtr = table::NormalTabletSchemaMaker::Make(field, index, attr, "");
    ASSERT_TRUE(framework::TabletSchemaLoader::ResolveSchema(nullptr, "", _schemaPtr.get()).IsOK());

    std::shared_ptr<NormalExtendDocument> extendDoc(new NormalExtendDocument);
    auto parser = std::make_shared<NormalDocumentParser>(nullptr, false);
    ASSERT_TRUE(parser->Init(_schemaPtr, nullptr).IsOK());

    // no raw doc
    AssertParseEmpty(parser, extendDoc);

    extendDoc->SetRawDocument(std::shared_ptr<RawDocument>(new DefaultRawDocument(_hashMapManager)));
    extendDoc->GetRawDocument()->setDocOperateType(ADD_DOC);
    TokenizeHelper tokenizeHelper;
    tokenizeHelper.init(_schemaPtr);
    ASSERT_TRUE(tokenizeHelper.process(extendDoc));

    // pk empty
    AssertParseEmpty(parser, extendDoc);

    // pk not empty
    extendDoc->GetRawDocument()->setField("f2", "notempty");
    auto document = ParseDocument(parser, extendDoc);
    ASSERT_TRUE(document);
}

TEST_F(NormalDocumentParserTest, testValidateNoIndexField)
{
    string field = "f1:long;f2:long;f3:string";
    string index = "index1:NUMBER:f1;";
    string attr = "f1";
    _schemaPtr = table::NormalTabletSchemaMaker::Make(field, index, attr, "");
    ASSERT_TRUE(framework::TabletSchemaLoader::ResolveSchema(nullptr, "", _schemaPtr.get()).IsOK());
    auto parser = std::make_shared<NormalDocumentParser>(nullptr, false);
    ASSERT_TRUE(parser->Init(_schemaPtr, nullptr).IsOK());
    std::shared_ptr<NormalExtendDocument> document(new NormalExtendDocument);

    document->SetRawDocument(std::shared_ptr<RawDocument>(new DefaultRawDocument(_hashMapManager)));
    document->GetRawDocument()->setDocOperateType(ADD_DOC);
    TokenizeHelper tokenizeHelper;
    tokenizeHelper.init(_schemaPtr);
    ASSERT_TRUE(tokenizeHelper.process(document));

    // no pk, no index field
    AssertParseEmpty(parser, document);

    // no pk, with index field
    document.reset(new NormalExtendDocument);
    document->SetRawDocument(std::shared_ptr<RawDocument>(new DefaultRawDocument(_hashMapManager)));
    document->GetRawDocument()->setDocOperateType(ADD_DOC);
    document->GetRawDocument()->setField("f1", "123");
    ASSERT_TRUE(tokenizeHelper.process(document));
    ASSERT_TRUE(ParseDocument(parser, document));
}

TEST_F(NormalDocumentParserTest, testValidateNonAddDoc)
{
    string field = "f1:long;f2:long;f3:string";
    string index = "index1:NUMBER:f1;";
    string attr = "f1";
    {
        // test update doc without primary key
        _schemaPtr = table::NormalTabletSchemaMaker::Make(field, index, attr, "");
        ASSERT_TRUE(framework::TabletSchemaLoader::ResolveSchema(nullptr, "", _schemaPtr.get()).IsOK());

        std::shared_ptr<NormalExtendDocument> document(new NormalExtendDocument);
        document->SetRawDocument(std::shared_ptr<RawDocument>(new DefaultRawDocument(_hashMapManager)));
        auto parser = std::make_shared<NormalDocumentParser>(nullptr, false);
        ASSERT_TRUE(parser->Init(_schemaPtr, nullptr).IsOK());

        document->GetRawDocument()->setDocOperateType(DELETE_DOC);
        AssertParseEmpty(parser, document);

        document->GetRawDocument()->setDocOperateType(DELETE_SUB_DOC);
        AssertParseEmpty(parser, document);

        document->GetRawDocument()->setDocOperateType(UPDATE_FIELD);
        AssertParseEmpty(parser, document);
    }
    {
        // test update doc success
        index = "index1:NUMBER:f1;pk:PRIMARYKEY64:f2";
        _schemaPtr = table::NormalTabletSchemaMaker::Make(field, index, attr, "");
        ASSERT_TRUE(framework::TabletSchemaLoader::ResolveSchema(nullptr, "", _schemaPtr.get()).IsOK());
        std::shared_ptr<NormalExtendDocument> document(new NormalExtendDocument);
        document->SetRawDocument(std::shared_ptr<RawDocument>(new DefaultRawDocument(_hashMapManager)));

        document->GetRawDocument()->setField("f2", "1234");
        document->GetRawDocument()->setDocOperateType(UPDATE_FIELD);

        TokenizeHelper tokenizeHelper;
        tokenizeHelper.init(_schemaPtr);
        ASSERT_TRUE(tokenizeHelper.process(document));
        ASSERT_TRUE(ParseDocument(_schemaPtr, nullptr, document));

        // test error OP Type
        document.reset(new NormalExtendDocument);
        document->SetRawDocument(std::shared_ptr<RawDocument>(new DefaultRawDocument(_hashMapManager)));
        document->GetRawDocument()->setField("f2", "1234");
        document->GetRawDocument()->setDocOperateType(DocOperateType(123));
        ASSERT_TRUE(tokenizeHelper.process(document));
        AssertParseEmpty(_schemaPtr, nullptr, document);
    }
}

TEST_F(NormalDocumentParserTest, testParseNullField)
{
    string field = "f1:long::-1:true;f2:long;f3:text";
    string index = "index1:NUMBER:f1;pk:PRIMARYKEY64:f2";
    string attr = "f1";
    _schemaPtr = table::NormalTabletSchemaMaker::Make(field, index, attr, "");
    ASSERT_TRUE(framework::TabletSchemaLoader::ResolveSchema(nullptr, "", _schemaPtr.get()).IsOK());
    std::shared_ptr<NormalExtendDocument> doc = makeExtendDoc(_schemaPtr, "f2:2,f3:abc efg");
    auto document = ParseDocument(_schemaPtr, nullptr, doc);
    set<fieldid_t> nullFields = {0};
    ASSERT_EQ(nullFields, document->GetAttributeDocument()->_nullFields);

    std::shared_ptr<config::FieldConfig> fieldConfig = _schemaPtr->GetFieldConfig("f1");
    fieldConfig->SetNullFieldLiteralString("NULL");
    std::shared_ptr<NormalExtendDocument> doc2 = makeExtendDoc(_schemaPtr, "f1:NULL,f2:2,f3:abc efg");
    auto document2 = ParseDocument(_schemaPtr, nullptr, doc2);
    ASSERT_EQ(nullFields, document2->GetAttributeDocument()->_nullFields);
}

TEST_F(NormalDocumentParserTest, testCompitableSerialzeForModifyOperation)
{
    string field = "f1:long;f2:long;f3:text";
    string index = "index1:NUMBER:f1;pk:PRIMARYKEY64:f2";
    string attr = "f1";
    _schemaPtr = table::NormalTabletSchemaMaker::Make(field, index, attr, "");
    ASSERT_TRUE(framework::TabletSchemaLoader::ResolveSchema(nullptr, "", _schemaPtr.get()).IsOK());
    {
        // old schema with out modify operation
        std::shared_ptr<NormalExtendDocument> mainDoc = makeExtendDoc(_schemaPtr, "f1:1,f2:2,f3:abc efg");
        auto document = ParseDocument(_schemaPtr, nullptr, mainDoc);
        ASSERT_EQ(12, document->GetSerializedVersion());
    }

    {
        // doc support null
        string field = "f1:long::-1:true;f2:long;f3:text";
        _schemaPtr = table::NormalTabletSchemaMaker::Make(field, index, attr, "");
        ASSERT_TRUE(framework::TabletSchemaLoader::ResolveSchema(nullptr, "", _schemaPtr.get()).IsOK());
        std::shared_ptr<NormalExtendDocument> mainDoc = makeExtendDoc(_schemaPtr, "f1:1,f2:2,f3:abc efg");
        auto document = ParseDocument(_schemaPtr, nullptr, mainDoc);
        ASSERT_EQ(DOCUMENT_BINARY_VERSION, document->GetSerializedVersion());
    }
}

TEST_F(NormalDocumentParserTest, testCompitableSerialize)
{
    string field = "f1:long;f2:long;f3:text";
    string index = "index1:NUMBER:f1;pk:PRIMARYKEY64:f2";
    string attr = "f1";
    _schemaPtr = table::NormalTabletSchemaMaker::Make(field, index, attr, "");
    ASSERT_TRUE(framework::TabletSchemaLoader::ResolveSchema(nullptr, "", _schemaPtr.get()).IsOK());
    {
        // doc no field doc trace
        std::shared_ptr<NormalExtendDocument> mainDoc = makeExtendDoc(_schemaPtr, "f1:1,f2:2,f3:abc efg");
        auto document = ParseDocument(_schemaPtr, nullptr, mainDoc);
        ASSERT_EQ(12, document->GetSerializedVersion());
    }
    {
        // doc has doc trace
        std::shared_ptr<NormalExtendDocument> mainDoc = makeExtendDoc(_schemaPtr, "f1:1,f2:2,f3:abc efg", "1");
        auto document = ParseDocument(_schemaPtr, nullptr, mainDoc);
        ASSERT_EQ(DOCUMENT_BINARY_VERSION, document->GetSerializedVersion());
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

    auto document = ParseDocument(_schemaPtr, nullptr, _extendDoc);
    fieldid_t fieldId = _schemaPtr->GetFieldId("title");
    Field* resultField = document->GetIndexDocument()->GetField(fieldId);
    checkResult(resultField, answer, secTokenCounts, sectionLens, posVec);
    checkSectionAttribute(document->GetIndexDocument());
    ASSERT_TRUE(document->GetSummaryDocument());
    ASSERT_TRUE(document->GetTraceId().empty());

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

    auto document = ParseDocument(_schemaPtr, nullptr, _extendDoc);
    fieldid_t fieldId = _schemaPtr->GetFieldId("title");
    Field* resultField = document->GetIndexDocument()->GetField(fieldId);
    checkResult(resultField, answer, secTokenCounts, sectionLens, posVec);
    checkSectionAttribute(document->GetIndexDocument());
    ASSERT_TRUE(document->GetSummaryDocument());
    ASSERT_TRUE(document->GetTraceId().empty());

    const ModifiedTokens* modifiedTokens = document->GetIndexDocument()->GetFieldModifiedTokens(fieldId);
    EXPECT_NE(nullptr, modifiedTokens);
    ASSERT_TRUE(modifiedTokens->Empty());
}

std::shared_ptr<NormalExtendDocument> NormalDocumentParserTest::createExtendDoc(const string& fieldMap)
{
    std::shared_ptr<NormalExtendDocument> extendDoc(new NormalExtendDocument());
    std::shared_ptr<RawDocument> rawDoc(new DefaultRawDocument(_hashMapManager));
    vector<string> fields = autil::StringTokenizer::tokenize(autil::StringView(fieldMap), ";");
    for (size_t i = 0; i < fields.size(); ++i) {
        vector<string> kv = autil::StringTokenizer::tokenize(autil::StringView(fields[i]), ":");
        rawDoc->setField(kv[0], kv[1]);
    }
    rawDoc->setDocOperateType(ADD_DOC);
    extendDoc->SetRawDocument(rawDoc);
    return extendDoc;
}

const autil::StringView* NormalDocumentParserTest::getSummaryField(fieldid_t fid,
                                                                   const std::shared_ptr<NormalDocument>& normalDoc)
{
    assert(normalDoc);
    _searchSummaryDoc.reset(new SearchSummaryDocument(NULL, 20));

    const auto& config = _schemaPtr->GetIndexConfig(index::SUMMARY_INDEX_TYPE_STR, index::SUMMARY_INDEX_NAME);
    assert(config);
    const auto& summaryConfig = std::dynamic_pointer_cast<config::SummaryIndexConfig>(config);
    assert(summaryConfig);
    SummaryFormatter formatter(summaryConfig);
    formatter.TEST_DeserializeSummaryDoc(normalDoc->GetSummaryDocument(), _searchSummaryDoc.get());

    return _searchSummaryDoc->GetFieldValue(summaryConfig->GetSummaryFieldId(fid));
}

TEST_F(NormalDocumentParserTest, testValidSourceNotChange)
{
    auto schema =
        framework::TabletSchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "schema_summary_reuse_source.json");
    ASSERT_TRUE(schema);
    ASSERT_TRUE(framework::TabletSchemaLoader::ResolveSchema(nullptr, "", schema.get()).IsOK());
    auto parser = std::make_unique<NormalDocumentParser>(nullptr, false);
    ASSERT_TRUE(parser->Init(std::shared_ptr<config::ITabletSchema>(schema.release()), nullptr).IsOK());

    {
        // before process
        std::shared_ptr<RawDocument> rawDoc;
        rawDoc.reset(RawDocumentMaker::Make("string1=1,price=3").release());
        ASSERT_TRUE(rawDoc);
        NormalExtendDocument extendDoc;
        extendDoc.SetRawDocument(rawDoc);
        auto classfiedDoc = extendDoc.getClassifiedDocument();
        classfiedDoc->setOriginalSnapshot(std::shared_ptr<RawDocument::Snapshot>(rawDoc->GetSnapshot()));

        // change not reuse field is valid
        rawDoc->setField("string1", "111");

        auto [st, docBatch] = parser->Parse(extendDoc);
        ASSERT_TRUE(st.IsOK());
        ASSERT_TRUE(docBatch);
    }

    {
        // before process
        std::shared_ptr<RawDocument> rawDoc;
        rawDoc.reset(RawDocumentMaker::Make("string1=1,price=3").release());
        ASSERT_TRUE(rawDoc);
        NormalExtendDocument extendDoc;
        extendDoc.SetRawDocument(rawDoc);
        auto classfiedDoc = extendDoc.getClassifiedDocument();
        classfiedDoc->setOriginalSnapshot(std::shared_ptr<RawDocument::Snapshot>(rawDoc->GetSnapshot()));

        // change reuse field is invalid
        rawDoc->setField("price", "111");
        auto [st, docBatch] = parser->Parse(extendDoc);
        ASSERT_TRUE(st.IsOK());
        ASSERT_FALSE(docBatch);
    }
}
}} // namespace indexlibv2::document
