#include <autil/StringUtil.h>
#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/common/field_format/attribute/location_attribute_convertor.h"
#include "indexlib/common/field_format/attribute/line_attribute_convertor.h"
#include "indexlib/common/field_format/attribute/polygon_attribute_convertor.h"
#include "indexlib/common/field_format/spatial/geo_hash/geo_hash_util.h"
#include "indexlib/document/index_document/normal_document/summary_formatter.h"
#include "indexlib/config/index_partition_schema_maker.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/util/counter/accumulative_counter.h"
#include "indexlib/util/counter/state_counter.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/document/document_parser/normal_parser/normal_document_parser.h"
#include "indexlib/document/document_parser/normal_parser/test/tokenize_helper.h"
#include "indexlib/document/document_factory_wrapper.h"
#include "indexlib/document/raw_document.h"
#include "indexlib/document/raw_document/default_raw_document.h"
#include "indexlib/document/raw_document/raw_document_define.h"
#include "indexlib/document/builtin_parser_init_param.h"
#include "indexlib/config/test/schema_loader.h"
#include "indexlib/common/field_format/spatial/spatial_field_encoder.h"
#include "indexlib/document/document_parser/kv_parser/kv_key_extractor.h"
#include "indexlib/testlib/schema_maker.h"
#include "indexlib/testlib/modify_schema_maker.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace autil;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(testlib);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(document);

class NormalDocumentParserTest : public INDEXLIB_TESTBASE {
public:
    void CaseSetUp();
    void CaseTearDown();

protected:
    void checkResult(IE_NAMESPACE(document)::Field* field, std::string answer[],
                     size_t secTokenCounts[], section_len_t sectionLens[],
                     pos_t posVec[], const KeyHasher& hasher = TextHasher());
    
    void checkTokens(IE_NAMESPACE(document)::Field* field,
                     const vector<dictkey_t>& tokens);
    
    void checkSectionAttribute(
	const IE_NAMESPACE(document)::IndexDocumentPtr& indexDocument);
    
    void prepare(const std::string &fieldValue);

    const ConstString* getSummaryField(fieldid_t fid, const DocumentPtr& doc);

    IndexlibExtendDocumentPtr makeExtendDoc(const IndexPartitionSchemaPtr& schema,
                                            const string& docValues, string traceId = "");
        
private:
    IndexlibExtendDocumentPtr createExtendDoc(
            const std::string &fieldMap,
            regionid_t regionId = DEFAULT_REGIONID);
protected:
    ClassifiedDocumentPtr _classifiedDocument;
    IndexlibExtendDocumentPtr _extendDoc;
    config::IndexPartitionSchemaPtr _schemaPtr;
    TokenizeHelperPtr _tokenizeHelper;
    SearchSummaryDocumentPtr _searchSummaryDoc;
private:
    KeyMapManagerPtr _hashMapManager;
};


void NormalDocumentParserTest::CaseSetUp() {
    _hashMapManager.reset(new KeyMapManager());
    _schemaPtr = SchemaLoader::LoadSchema(TEST_DATA_PATH,
            "/document_parser_simple_schema.json");

    _extendDoc.reset(new IndexlibExtendDocument());
    RawDocumentPtr rawDoc(new DefaultRawDocument(_hashMapManager));
    _extendDoc->setRawDocument(rawDoc);
    _extendDoc->getRawDocument()->setDocOperateType(ADD_DOC);
    _classifiedDocument = _extendDoc->getClassifiedDocument();

    _tokenizeHelper.reset(new TokenizeHelper);
    _tokenizeHelper->init(_schemaPtr);
}


void NormalDocumentParserTest::CaseTearDown() {}

TEST_F(NormalDocumentParserTest, testSimpleProcess) {
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
    fieldid_t fieldId = _schemaPtr->GetFieldSchema()->GetFieldId("title");
    Field *resultField = document->GetIndexDocument()->GetField(fieldId);
    checkResult(resultField, answer,  secTokenCounts, sectionLens, posVec);
    checkSectionAttribute(document->GetIndexDocument());
    ASSERT_TRUE(document->GetSummaryDocument());
    ASSERT_FALSE(document->NeedTrace());
}

TEST_F(NormalDocumentParserTest, testConvertError) {
    string fieldValue = "title:A b# c d e,nid:a";
    prepare(fieldValue);

    CounterMapPtr counterMap(new CounterMap);
    BuiltInParserInitResource resource;
    resource.counterMap = counterMap;
    BuiltInParserInitParamPtr initParam(new BuiltInParserInitParam);
    initParam->SetResource(resource);

    IndexPartitionSchemaPtr schema = SchemaLoader::LoadSchema(
        TEST_DATA_PATH, "/schema/document_parser_simple_schema.json");

    DocumentFactoryWrapper wrapper(schema);
    wrapper.Init();
    DocumentParserPtr parser(wrapper.CreateDocumentParser(initParam));

    NormalDocumentParserPtr normalParser =
        DYNAMIC_POINTER_CAST(NormalDocumentParser, parser);
    ASSERT_TRUE(normalParser);
    {
        DocumentPtr doc = parser->Parse(_extendDoc);
        ASSERT_TRUE(doc);
        EXPECT_EQ(1, normalParser->mAttributeConvertErrorCounter->Get());
    }

    //clear
    {
        _extendDoc.reset(new IndexlibExtendDocument());
        _extendDoc->setRawDocument(RawDocumentPtr(new DefaultRawDocument(_hashMapManager)));
        string fieldValue = "title:A b# c d e,nid:1,user_id:1";
        prepare(fieldValue);
        DocumentPtr doc = parser->Parse(_extendDoc);
        ASSERT_TRUE(doc);
        EXPECT_EQ(1, normalParser->mAttributeConvertErrorCounter->Get());
    }

    //clear
    {
        _extendDoc.reset(new IndexlibExtendDocument());
        _extendDoc->setRawDocument(RawDocumentPtr(new DefaultRawDocument(_hashMapManager)));
        string fieldValue = "title:A b# c d e,nid:c,user_id:1";
        prepare(fieldValue);
        DocumentPtr doc = parser->Parse(_extendDoc);
        ASSERT_TRUE(doc);
        EXPECT_EQ(2, normalParser->mAttributeConvertErrorCounter->Get());        
    }
    
    //clear
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

TEST_F(NormalDocumentParserTest, testShapeIndex) {
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
    
    fieldid_t lineFieldId = _schemaPtr->GetFieldSchema()->GetFieldConfig(
            "line")->GetFieldId();
    fieldid_t polygonFieldId = _schemaPtr->GetFieldSchema()->GetFieldConfig(
            "polygon")->GetFieldId();
    
    {
        //check index tokens
        SpatialFieldEncoder encoder(_schemaPtr);
        vector<dictkey_t> lineIndexTokens;
        vector<dictkey_t> polygonIndexTokens;
        encoder.Encode(lineFieldId, lineStr, lineIndexTokens);
        encoder.Encode(polygonFieldId, polygonStr, polygonIndexTokens);
        auto *field = document->GetIndexDocument()->GetField(lineFieldId);
        checkTokens(field, lineIndexTokens);
        field = document->GetIndexDocument()->GetField(polygonFieldId);
        checkTokens(field, polygonIndexTokens);
    }
    {
        //check attribute
        ConstString value = document->GetAttributeDocument()->GetField(lineFieldId);
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

TEST_F(NormalDocumentParserTest, testSpatialIndex) {
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

    std::vector<uint64_t> terms;
    int8_t detailLevel = GeoHashUtil::DistanceToLevel(20);
    int8_t topLevel = GeoHashUtil::DistanceToLevel(10000);
    GeoHashUtil::Encode(180.0, 30.0, terms, topLevel, detailLevel);
    
    string answer[10];
    ASSERT_EQ((size_t)5, terms.size());
    for (size_t i = 0; i < terms.size(); i++)
    {
        answer[i] = StringUtil::toString(terms[i]);
    }

    terms.clear();
    GeoHashUtil::Encode(140.1, 31.1, terms, topLevel, detailLevel);
    ASSERT_EQ((size_t)5, terms.size());
    for (size_t i = 0; i < terms.size(); i++)
    {
        answer[5 + i] = StringUtil::toString(terms[i]);
    }

    section_len_t sectionLens[] = {0};
    size_t secTokenCounts[] = {10};
    pos_t posVec[] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    fieldid_t fieldId = _schemaPtr->GetFieldSchema()->GetFieldId("location");
    Field *resultField = document->GetIndexDocument()->GetField(fieldId);
    UInt64NumberHasher hasher;
    checkResult(resultField, answer,  secTokenCounts, sectionLens, posVec, hasher);
    fieldid_t locationFieldId = _schemaPtr->GetFieldSchema()->GetFieldId("location");
    ConstString value = document->GetAttributeDocument()->GetField(locationFieldId);
    string fieldValue(value.data(), value.size());
    LocationAttributeConvertor convertor;
    string expectResult = convertor.Encode("180.0 30.0140.1 31.1");
    ASSERT_EQ(expectResult, fieldValue);
}

TEST_F(NormalDocumentParserTest, testSectionAttributeForNonAddDocType) {
    string fieldValue = "title:A b# c d e,nid:1";
    {
        TearDown();
        SetUp();
        _extendDoc->getRawDocument()->setDocOperateType(UPDATE_FIELD);
        prepare(fieldValue);

        DocumentFactoryWrapper wrapper(_schemaPtr);
        wrapper.Init();
        DocumentParserPtr parser(wrapper.CreateDocumentParser());
        ASSERT_TRUE(dynamic_cast<NormalDocumentParser*>(parser.get()));
        DocumentPtr doc = parser->Parse(_extendDoc);
        ASSERT_TRUE(doc);
        NormalDocumentPtr document = DYNAMIC_POINTER_CAST(NormalDocument, doc);
        ASSERT_EQ((indexid_t)INVALID_INDEXID,
                  document->GetIndexDocument()->GetMaxIndexIdInSectionAttribute());
    }

    {
        TearDown();
        SetUp();
        _extendDoc->getRawDocument()->setDocOperateType(DELETE_DOC);
        prepare(fieldValue);

        DocumentFactoryWrapper wrapper(_schemaPtr);
        wrapper.Init();
        DocumentParserPtr parser(wrapper.CreateDocumentParser());
        ASSERT_TRUE(dynamic_cast<NormalDocumentParser*>(parser.get()));
        DocumentPtr doc = parser->Parse(_extendDoc);
        ASSERT_TRUE(doc);
        NormalDocumentPtr document = DYNAMIC_POINTER_CAST(NormalDocument, doc);
        ASSERT_EQ((indexid_t)INVALID_INDEXID,
                  document->GetIndexDocument()->GetMaxIndexIdInSectionAttribute());
    }

    {
        TearDown();
        SetUp();
        _extendDoc->getRawDocument()->setDocOperateType(DELETE_SUB_DOC);
        prepare(fieldValue);

        DocumentFactoryWrapper wrapper(_schemaPtr);
        wrapper.Init();
        DocumentParserPtr parser(wrapper.CreateDocumentParser());
        ASSERT_TRUE(dynamic_cast<NormalDocumentParser*>(parser.get()));
        DocumentPtr doc = parser->Parse(_extendDoc);
        ASSERT_TRUE(doc);
        NormalDocumentPtr document = DYNAMIC_POINTER_CAST(NormalDocument, doc);
        ASSERT_EQ((indexid_t)INVALID_INDEXID,
                  document->GetIndexDocument()->GetMaxIndexIdInSectionAttribute());
    }
}

TEST_F(NormalDocumentParserTest, testNonUpdateField) {
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

    fieldid_t fieldId = _schemaPtr->GetFieldSchema()->GetFieldId("content");
    const ConstString* summaryFieldValue = getSummaryField(fieldId, document);
    EXPECT_TRUE(summaryFieldValue && !summaryFieldValue->empty());

    fieldid_t pkFieldId = _schemaPtr->GetFieldSchema()->GetFieldId("nid");
    EXPECT_TRUE(document->GetIndexDocument()->GetField(pkFieldId));
    EXPECT_FALSE(document->GetAttributeDocument()->GetField(pkFieldId).empty());

    fieldId = _schemaPtr->GetFieldSchema()->GetFieldId("user_id");
    EXPECT_FALSE(document->GetAttributeDocument()->GetField(fieldId).empty());

    fieldId = _schemaPtr->GetFieldSchema()->GetFieldId("multi_string");
    summaryFieldValue = getSummaryField(fieldId, document);
    EXPECT_TRUE(!summaryFieldValue || summaryFieldValue->empty());
    EXPECT_FALSE(document->GetAttributeDocument()->GetField(fieldId).empty());

    fieldId = _schemaPtr->GetFieldSchema()->GetFieldId("auction_type");
    summaryFieldValue = getSummaryField(fieldId, document);
    EXPECT_TRUE(summaryFieldValue && !summaryFieldValue->empty());
    EXPECT_TRUE(document->GetAttributeDocument()->GetField(fieldId).empty());
}


TEST_F(NormalDocumentParserTest, testUpdateField) {
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
    
    fieldid_t fieldId = _schemaPtr->GetFieldSchema()->GetFieldId("content");
    const ConstString* summaryFieldValue = getSummaryField(fieldId, document);
    EXPECT_TRUE(!summaryFieldValue || summaryFieldValue->empty());
    EXPECT_TRUE(document->GetAttributeDocument()->GetField(fieldId).empty());

    fieldid_t pkFieldId = _schemaPtr->GetFieldSchema()->GetFieldId("nid");
    EXPECT_TRUE(document->GetIndexDocument()->GetField(pkFieldId));
    EXPECT_TRUE(document->GetAttributeDocument()->GetField(pkFieldId).empty());

    ConstString constStr;
    string fieldString;
    FieldConfigPtr fieldConfig;
    fieldId = _schemaPtr->GetFieldSchema()->GetFieldId("user_id");
    fieldConfig = _schemaPtr->GetFieldSchema()->GetFieldConfig(fieldId);
    AttributeConvertorPtr convertor;
    convertor.reset(
            AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(fieldConfig));
    constStr = document->GetAttributeDocument()->GetField(fieldId);
    fieldString = string(constStr.data(), constStr.size());
    string expectedValue;
    expectedValue = convertor->Encode(string("1234"));
    EXPECT_EQ(expectedValue, fieldString);

    fieldId = _schemaPtr->GetFieldSchema()->GetFieldId("multi_string");
    fieldConfig = _schemaPtr->GetFieldSchema()->GetFieldConfig(fieldId);
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
    
    fieldid_t fieldId = _schemaPtr->GetFieldSchema()->GetFieldId("content");
    const ConstString* summaryFieldValue = getSummaryField(fieldId, document);
    string fieldString = string(summaryFieldValue->data(), summaryFieldValue->size());
    ASSERT_EQ(string("A\t \tb\t#\t \tc\t \td\t \te"), fieldString);


    FieldConfigPtr fieldConfig;
    fieldId = _schemaPtr->GetFieldSchema()->GetFieldId("user_id");
    fieldConfig = _schemaPtr->GetFieldSchema()->GetFieldConfig(fieldId);
    AttributeConvertorPtr convertor;
    convertor.reset(
            AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(fieldConfig));
    ConstString constStr = document->GetAttributeDocument()->GetField(fieldId);
    fieldString = string(constStr.data(), constStr.size());
    string expectedValue;
    expectedValue = convertor->Encode(string("1234"));
    ASSERT_EQ(expectedValue, fieldString);

    fieldId = _schemaPtr->GetFieldSchema()->GetFieldId("multi_string");
    fieldConfig = _schemaPtr->GetFieldSchema()->GetFieldConfig(fieldId);
    summaryFieldValue = getSummaryField(fieldId, document);
    EXPECT_TRUE(!summaryFieldValue || summaryFieldValue->empty());

    convertor.reset(
            AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(fieldConfig));
    constStr = document->GetAttributeDocument()->GetField(fieldId);
    fieldString = string(constStr.data(), constStr.size());
    expectedValue = convertor->Encode(string("111222333"));
    ASSERT_EQ(expectedValue, fieldString);

    string answer[] = {"111", "222", "333"};
    section_len_t sectionLens[] = {4};
    size_t secTokenCounts[] = {3};
    pos_t posVec[] = {0, 1, 1};
    Field *resultField = document->GetIndexDocument()->GetField(fieldId);
    checkResult(resultField, answer,  secTokenCounts, sectionLens, posVec);
    checkSectionAttribute(document->GetIndexDocument());
}

void NormalDocumentParserTest::prepare(const std::string &fieldValue) {
    RawDocumentPtr rawDoc = _extendDoc->getRawDocument();
    vector<vector<string> > splitValue;
    StringUtil::fromString(fieldValue, splitValue, ":", ",");
    for (size_t i = 0; i < splitValue.size(); ++i) {
        assert(splitValue[i].size() == 2);
        rawDoc->setField(splitValue[i][0], splitValue[i][1]);
    }
    ASSERT_TRUE(_tokenizeHelper->process(_extendDoc));
}

IndexlibExtendDocumentPtr NormalDocumentParserTest::makeExtendDoc(
        const IndexPartitionSchemaPtr& schema,  const string& docValues,
        string traceId)
{
    IndexlibExtendDocumentPtr extDoc(new IndexlibExtendDocument);
    RawDocumentPtr rawDoc(new DefaultRawDocument(_hashMapManager));
    extDoc->setRawDocument(rawDoc);
    
    vector<vector<string> > splitValue;
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
    assert(ret); (void)ret;
    return extDoc;
}

void NormalDocumentParserTest::checkTokens(
        Field* field, const vector<dictkey_t>& tokens)
{
    IndexTokenizeField *indexTokenizeField = dynamic_cast<IndexTokenizeField*>(field);
    assert(indexTokenizeField);
    int tokenIdx = 0;
    for (auto secIt = indexTokenizeField->Begin();
         secIt != indexTokenizeField->End(); ++secIt)
    {
        Section* section = *secIt;
        for (int32_t sectionTokenIdx = 0;
             sectionTokenIdx < (int32_t)section->GetTokenCount();
             ++sectionTokenIdx, ++tokenIdx)
        {
            const IE_NAMESPACE(document)::Token* token = section->GetToken(sectionTokenIdx);
            ASSERT_EQ(tokens[tokenIdx], token->GetHashKey()) << tokenIdx;
        }
    }
    ASSERT_EQ(tokenIdx, tokens.size());
}

void NormalDocumentParserTest::checkResult(Field* field, string answer[],
        size_t secTokenCounts[],
        section_len_t sectionLens[],
        pos_t posVec[], const KeyHasher& hasher)
{
    IndexTokenizeField *indexTokenizeField = dynamic_cast<IndexTokenizeField*>(field);
    assert(indexTokenizeField);
    int secIdx = 0;
    int tokenIdx = 0;
    for (auto secIt = indexTokenizeField->Begin();
         secIt != indexTokenizeField->End(); ++secIt, ++secIdx)
    {
        Section* section = *secIt;
        ASSERT_EQ(sectionLens[secIdx], section->GetLength());
        ASSERT_EQ(secTokenCounts[secIdx], section->GetTokenCount());

        for (int32_t sectionTokenIdx = 0;
             sectionTokenIdx < (int32_t)section->GetTokenCount();
             ++sectionTokenIdx, ++tokenIdx)
        {
            const IE_NAMESPACE(document)::Token* token = section->GetToken(sectionTokenIdx);
            dictkey_t hashValue;
            //DefaultHasher hasher;
            hasher.GetHashKey(answer[tokenIdx].c_str(), hashValue);
            ASSERT_EQ(hashValue, token->GetHashKey());
            ASSERT_EQ(posVec[tokenIdx], token->GetPosIncrement());
        }
    }
}

void NormalDocumentParserTest::checkSectionAttribute(
    const IndexDocumentPtr& indexDocument)
{
    IndexSchemaPtr indexSchema = _schemaPtr->GetIndexSchema();
    IndexSchema::Iterator it;
    for (it = indexSchema->Begin(); it != indexSchema->End(); ++it)
    {
	const ConstString& sectionAttr = indexDocument->GetSectionAttribute(
	    (*it)->GetIndexId());
	if ((*it)->GetIndexType() == it_expack || (*it)->GetIndexType() == it_pack)
	{
	    PackageIndexConfigPtr packIndexConfig =
		std::tr1::dynamic_pointer_cast<PackageIndexConfig>(*it);

	    if (packIndexConfig->HasSectionAttribute())
	    {
		ASSERT_FALSE(sectionAttr.empty());
		continue;
	    }
	}
	ASSERT_EQ(ConstString::EMPTY_STRING, sectionAttr);
    }
}
    

TEST_F(NormalDocumentParserTest, testSummaryFieldWithOriginalValue)
{
    //BugID=61168
    string fieldValue = "content:I am coding with C language,nid:1";
    prepare(fieldValue);

    DocumentFactoryWrapper wrapper(_schemaPtr);
    wrapper.Init();
    DocumentParserPtr parser(wrapper.CreateDocumentParser());
    ASSERT_TRUE(dynamic_cast<NormalDocumentParser*>(parser.get()));
    DocumentPtr document = parser->Parse(_extendDoc);
    ASSERT_TRUE(document);

    fieldid_t fieldId = _schemaPtr->GetFieldSchema()->GetFieldId("content");
    const ConstString* value = getSummaryField(fieldId, document);
    string fieldString = string(value->data(), value->size());
    ASSERT_EQ(string("I\t \tam\t \tcoding\t \twith\t \tC\t \tlanguage"), fieldString);
}

TEST_F(NormalDocumentParserTest, testSetPrimaryKeyField) {
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(schema,
            "pk:int32;", //Field schema
            "pk:primarykey64:pk",//index schema
            "",//attribute schema
            "");//Summary schema

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

TEST_F(NormalDocumentParserTest, testAppendPackAttributes) {
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
    const ConstString& packField = attrDoc->GetPackField(0);
    ASSERT_FALSE(packField.empty());
}

TEST_F(NormalDocumentParserTest, testValidateWithPkEmpty) {
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema("name"));
    string fieldNames = "f1:long;f2:long;f3:string";
    string indexSchemaStr = "index1:NUMBER:f1;pk:PRIMARYKEY64:f2";

    string attrSchemaStr = "f1";
    IE_NAMESPACE(config)::IndexPartitionSchemaMaker::MakeSchema(schema, 
            fieldNames, indexSchemaStr, attrSchemaStr, "");

    IndexlibExtendDocumentPtr extendDoc(new IndexlibExtendDocument);
    DocumentFactoryWrapper wrapper(schema);
    wrapper.Init();
    DocumentParserPtr parser(wrapper.CreateDocumentParser());
    ASSERT_TRUE(dynamic_cast<NormalDocumentParser*>(parser.get()));
    // no raw doc
    ASSERT_FALSE(parser->Parse(extendDoc));

    extendDoc->setRawDocument(RawDocumentPtr(new DefaultRawDocument(_hashMapManager)));
    extendDoc->getRawDocument()->mOpType = ADD_DOC;
    TokenizeHelper tokenizeHelper;
    tokenizeHelper.init(schema);
    ASSERT_TRUE(tokenizeHelper.process(extendDoc));
    
    // pk empty
    ASSERT_FALSE(parser->Parse(extendDoc));

    // pk not empty
    extendDoc->getRawDocument()->setField("f2", "notempty");
    ASSERT_TRUE(parser->Parse(extendDoc));
}

TEST_F(NormalDocumentParserTest, testValidateNoIndexField) {
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema("name"));
    string fieldNames = "f1:long;f2:long;f3:string";
    string indexSchemaStr = "index1:NUMBER:f1;";
    string attrSchemaStr = "f1";
    IE_NAMESPACE(config)::IndexPartitionSchemaMaker::MakeSchema(schema, 
            fieldNames, indexSchemaStr, attrSchemaStr, "");

    DocumentFactoryWrapper wrapper(schema);
    wrapper.Init();
    DocumentParserPtr parser(wrapper.CreateDocumentParser());
    ASSERT_TRUE(dynamic_cast<NormalDocumentParser*>(parser.get()));

    IndexlibExtendDocumentPtr document(new IndexlibExtendDocument);

    document->setRawDocument(RawDocumentPtr(new DefaultRawDocument(_hashMapManager)));
    document->getRawDocument()->mOpType = ADD_DOC;
    TokenizeHelper tokenizeHelper;
    tokenizeHelper.init(schema);
    ASSERT_TRUE(tokenizeHelper.process(document));
    
    // no pk, no index field
    EXPECT_FALSE(parser->Parse(document));

    // no pk, with index field
    document.reset(new IndexlibExtendDocument);
    document->setRawDocument(RawDocumentPtr(new DefaultRawDocument(_hashMapManager)));
    document->getRawDocument()->mOpType = ADD_DOC;
    document->getRawDocument()->setField("f1", "123");
    ASSERT_TRUE(tokenizeHelper.process(document));
    EXPECT_TRUE(parser->Parse(document));
}

TEST_F(NormalDocumentParserTest, testValidateNonAddDoc) {
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema("name"));
    string fieldNames = "f1:long;f2:long;f3:string";
    string indexSchemaStr = "index1:NUMBER:f1;";
    string attrSchemaStr = "f1";
    {
        // test update doc without primary key
        IE_NAMESPACE(config)::IndexPartitionSchemaMaker::MakeSchema(
            schema, fieldNames, indexSchemaStr, attrSchemaStr, "");

        DocumentFactoryWrapper wrapper(schema);
        wrapper.Init();
        DocumentParserPtr parser(wrapper.CreateDocumentParser());
        ASSERT_TRUE(dynamic_cast<NormalDocumentParser*>(parser.get()));

        IndexlibExtendDocumentPtr document(new IndexlibExtendDocument);
        document->setRawDocument(RawDocumentPtr(new DefaultRawDocument(_hashMapManager)));

        document->getRawDocument()->mOpType = DELETE_DOC;
        EXPECT_FALSE(parser->Parse(document));

        document->getRawDocument()->mOpType = DELETE_SUB_DOC;
        EXPECT_FALSE(parser->Parse(document));

        document->getRawDocument()->mOpType = UPDATE_FIELD;
        EXPECT_FALSE(parser->Parse(document));
    }
    {
        // test update doc success
        schema.reset(new IndexPartitionSchema("name"));
        indexSchemaStr = "index1:NUMBER:f1;pk:PRIMARYKEY64:f2";
        IE_NAMESPACE(config)::IndexPartitionSchemaMaker::MakeSchema(
            schema, fieldNames, indexSchemaStr, attrSchemaStr, "");
        IndexlibExtendDocumentPtr document(new IndexlibExtendDocument);
        document->setRawDocument(RawDocumentPtr(new DefaultRawDocument(_hashMapManager)));
        
        document->getRawDocument()->setField("f2", "1234");
        document->getRawDocument()->mOpType = UPDATE_FIELD;

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
        document->getRawDocument()->mOpType = DocOperateType(123);
        ASSERT_TRUE(tokenizeHelper.process(document));
        EXPECT_FALSE(parser->Parse(document));
    }
}

TEST_F(NormalDocumentParserTest, testMainSubDoc) {
    string fieldNames = "f1:long;f2:long;f3:text";
    string indexSchemaStr = "index1:NUMBER:f1;pk:PRIMARYKEY64:f2";
    string attrSchemaStr = "f1";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
        fieldNames, indexSchemaStr, attrSchemaStr, "");

    string subFieldNames = "sub_f1:long;sub_f2:long;sub_f3:text";
    string subIndexSchemaStr = "sub_index1:NUMBER:sub_f1;sub_pk:PRIMARYKEY64:sub_f2:number_hash";
    string subAttrSchemaStr = "sub_f1";
    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(
        subFieldNames, subIndexSchemaStr, subAttrSchemaStr, "");

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

TEST_F(NormalDocumentParserTest, testCompitableSerialzeForModifyOperation) {
    string fieldNames = "f1:long;f2:long;f3:text";
    string indexSchemaStr = "index1:NUMBER:f1;pk:PRIMARYKEY64:f2";
    string attrSchemaStr = "f1";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
        fieldNames, indexSchemaStr, attrSchemaStr, "");

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
        ASSERT_EQ(7, document->GetSerializedVersion());
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
}

TEST_F(NormalDocumentParserTest, testCompitableSerialize) {
    string fieldNames = "f1:long;f2:long;f3:text";
    string indexSchemaStr = "index1:NUMBER:f1;pk:PRIMARYKEY64:f2";
    string attrSchemaStr = "f1";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
            fieldNames, indexSchemaStr, attrSchemaStr, "");
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
        ASSERT_EQ(7, document->GetSerializedVersion());
    }

}

IndexlibExtendDocumentPtr NormalDocumentParserTest::createExtendDoc(
        const string &fieldMap, regionid_t regionId)
{
    IndexlibExtendDocumentPtr extendDoc(new IndexlibExtendDocument());
    RawDocumentPtr rawDoc(new DefaultRawDocument(_hashMapManager));
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

const ConstString* NormalDocumentParserTest::getSummaryField(fieldid_t fid, const DocumentPtr& doc)
{
    NormalDocumentPtr normalDoc = DYNAMIC_POINTER_CAST(NormalDocument, doc);
    assert(normalDoc);
    _searchSummaryDoc.reset(new SearchSummaryDocument(NULL, 20));

    SummaryFormatter formatter(_schemaPtr->GetSummarySchema());
    formatter.TEST_DeserializeSummaryDoc(normalDoc->GetSummaryDocument(), _searchSummaryDoc.get());
    
    return _searchSummaryDoc->GetFieldValue(
            _schemaPtr->GetSummarySchema()->GetSummaryFieldId(fid));
}

IE_NAMESPACE_END(document);
