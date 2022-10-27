#include "build_service/test/unittest.h"
#include "build_service/plugin/PlugInManager.h"
#include "build_service/processor/DocumentProcessorFactory.h"
#include "build_service/processor/TokenizeDocumentProcessor.h"
#include "build_service/analyzer/Token.h"
#include "build_service/analyzer/AnalyzerFactory.h"
#include "build_service/analyzer/TokenizerManager.h"
#include "build_service/analyzer/AnalyzerInfos.h"
#include "build_service/analyzer/SimpleTokenizer.h"
#include "build_service/analyzer/Analyzer.h"
#include <dlfcn.h>
#include <indexlib/document/index_document/normal_document/field.h>
#include <indexlib/document/raw_document/default_raw_document.h>
#include <autil/StringTokenizer.h>
#include <autil/StringUtil.h>
#include <autil/legacy/json.h>


using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);

using namespace build_service::analyzer;
using namespace build_service::config;
using namespace build_service::util;
using namespace build_service::document;

namespace build_service {
namespace processor {

class TokenizeDocumentProcessorTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
protected:
    void initDocument(const document::ExtendDocumentPtr &document);
    analyzer::AnalyzerInfosPtr makeAnalyzerParameter(const std::string& analyzerName);
    IE_NAMESPACE(config)::IndexPartitionSchemaPtr readIndexSchema(const std::string& schemaFile);
    document::RawDocumentPtr readRawDocument(const std::string& docFile);

    fieldid_t getFieldId(const std::string& fieldName) const;
    void checkResult(document::TokenizeSection *section,
                     std::vector<std::string> &sectionResult);

    void testSectionTokenize(const std::string &fieldValue, const std::string &fieldName,
                             const std::vector<std::vector<std::string> > &sectionResult,
                             const std::vector<int32_t> &sectionWeight,
                             const std::string& fieldAnalyzerName = "");

    void checkSectionTokenize(const std::string &fieldValue, const std::string &fieldName,
                             const std::vector<std::vector<std::string> > &sectionResult,
                              const std::vector<int32_t> &sectionWeight,
                              TokenizeDocumentProcessor &documentProcessor,
                              const std::string& fieldAnalyzerName = "");

    void checkSectionTokenizeFailed(const std::string &fieldValue, const std::string &fieldName);

protected:
    analyzer::AnalyzerFactoryPtr _analyzerFactoryPtr;
    IE_NAMESPACE(config)::IndexPartitionSchemaPtr _schemaPtr;
protected:
    void initStackSpace();
};

void TokenizeDocumentProcessorTest::setUp() {
    string configRoot = TEST_DATA_PATH"tokenize_processor_test/config/";
    ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));
    _analyzerFactoryPtr = AnalyzerFactory::create(resourceReaderPtr);
    ASSERT_TRUE(_analyzerFactoryPtr);
    string schemaRelativePath = "schemas/simple_schema.json";

    _schemaPtr.reset(new IndexPartitionSchema);
    ASSERT_TRUE(resourceReaderPtr->getConfigWithJsonPath(
                    schemaRelativePath, "", *_schemaPtr));
}

void TokenizeDocumentProcessorTest::tearDown() {
}

TEST_F(TokenizeDocumentProcessorTest, testSimpleProcess) {
    BS_LOG(DEBUG, "Begin Test!");
    const string fieldValue = "A b, c d e";

    TokenizeDocumentProcessor documentProcessor;
    DocProcessorInitParam param;
    param.schemaPtr = _schemaPtr;
    param.analyzerFactoryPtr = _analyzerFactoryPtr;
    documentProcessor.init(param);

    ExtendDocumentPtr extendDocument(new ExtendDocument);
    RawDocumentPtr rawDoc(new IE_NAMESPACE(document)::DefaultRawDocument());

    string fieldName = "title";
    rawDoc->setField(fieldName, fieldValue);
    extendDocument->setRawDocument(rawDoc);

    ASSERT_TRUE(documentProcessor.process(extendDocument));

    fieldid_t fieldId = getFieldId(fieldName);
    TokenizeFieldPtr tokenizeField = extendDocument->getTokenizeDocument()->getField(fieldId);
    ASSERT_EQ((uint32_t)1, tokenizeField->getSectionCount());

    string strs[] = {"A", " ", "b", ",", " ", "c", " ", "d", " ", "e"};
    vector<string> sectionResult(strs, strs + sizeof(strs)/sizeof(string));
    TokenizeField::Iterator it = tokenizeField->createIterator();
    checkResult(*(it), sectionResult);

    string emptyFieldName = "category";
    fieldId = getFieldId(emptyFieldName);
    tokenizeField = extendDocument->getTokenizeDocument()->getField(fieldId);
    ASSERT_TRUE(tokenizeField.get() != NULL);
}

TEST_F(TokenizeDocumentProcessorTest, testWithSomeFieldExist) {
    TokenizeDocumentProcessor processor;
    DocProcessorInitParam param;
    param.schemaPtr = _schemaPtr;
    param.analyzerFactoryPtr = _analyzerFactoryPtr;
    processor.init(param);

    ExtendDocumentPtr extendDoc(new ExtendDocument);
    RawDocumentPtr rawDoc(new IE_NAMESPACE(document)::DefaultRawDocument());
    rawDoc->setField("title", "测试字段字符串");
    extendDoc->setRawDocument(rawDoc);
    fieldid_t fieldId = getFieldId("title");
    TokenizeDocumentPtr tokenizeDoc = extendDoc->getTokenizeDocument();
    TokenizeFieldPtr tokenizeField = tokenizeDoc->createField(fieldId);
    TokenizeSection *section = tokenizeField->getNewSection();
    TokenizeSection::Iterator iterator = section->createIterator();
    analyzer::Token token;
    token.setText(string("测试字段"));
    section->insertBasicToken(token, iterator);
    token.setText(string("字符串"));
    section->insertBasicToken(token, iterator);

    ASSERT_TRUE(processor.process(extendDoc));
    tokenizeField = tokenizeDoc->getField(fieldId);
    ASSERT_EQ((uint32_t)1, tokenizeField->getSectionCount());

    vector<string> expectedResults;
    expectedResults.push_back("测试字段");
    expectedResults.push_back("字符串");
    TokenizeField::Iterator fieldIt = tokenizeField->createIterator();
    checkResult(*fieldIt, expectedResults);
}

TEST_F(TokenizeDocumentProcessorTest, testMulitSectionTokenize)
{
    BS_LOG(DEBUG, "Begin Test!");
    const string fieldValue = "A b, c d e123 a b";

    TokenizeDocumentProcessor documentProcessor;
    DocProcessorInitParam param;
    param.schemaPtr = _schemaPtr;
    param.analyzerFactoryPtr = _analyzerFactoryPtr;
    documentProcessor.init(param);

    ExtendDocumentPtr extendDocument(new ExtendDocument);
    RawDocumentPtr rawDoc(new IE_NAMESPACE(document)::DefaultRawDocument());

    string fieldName = "title";
    rawDoc->setField(fieldName, fieldValue);
    extendDocument->setRawDocument(rawDoc);

    ASSERT_TRUE(documentProcessor.process(extendDocument));

    fieldid_t fieldId = getFieldId(fieldName);
    TokenizeFieldPtr tokenizeField = extendDocument->getTokenizeDocument()->getField(fieldId);

    ASSERT_EQ((uint32_t)2, tokenizeField->getSectionCount());

    TokenizeField::Iterator it  = tokenizeField->createIterator();
    ASSERT_TRUE(*it);
    string strs[] = {"A", " ", "b", ",", " ", "c", " ", "d", " ", "e"};
    vector<string> sectionResult1 (strs, strs + sizeof(strs) / sizeof(string));
    checkResult(*it, sectionResult1);

    ASSERT_TRUE(it.next());
    string strs2[] = {"123", " ", "a", " ", "b"};
    vector<string> sectionResult2 (strs2, strs2 + sizeof(strs2) / sizeof(string));
    checkResult(*it, sectionResult2);
}

TEST_F(TokenizeDocumentProcessorTest, testTokenizeSingleValueField)
{
    BS_LOG(DEBUG, "Begin Test!");
    const string fieldValue = "aaa bbb 0a aa0";
    const string fieldName = "user_name";
    string strs[] = {"aaa bbb 0a aa0"};
    vector<string> vecStr(strs, strs + sizeof(strs)/sizeof(string));
    vector<vector<string> > result;
    result.push_back(vecStr);

    vector<int32_t> sectionWeightVec;
    sectionWeightVec.push_back(0);

    testSectionTokenize(fieldValue, fieldName, result, sectionWeightVec);
}

TEST_F(TokenizeDocumentProcessorTest, testMulitSectionTokenizeWithWrongWeight)
{
    BS_LOG(DEBUG, "Begin Test!");
    const string fieldName = "subject";

    const string fieldValue = "我爱菊花茶\034 10 \0350 abc\035";
    checkSectionTokenizeFailed(fieldValue, fieldName);

    const string fieldValue2 = "\034我爱菊花茶\034 10 \0350 abc\035";
    checkSectionTokenizeFailed(fieldValue2, fieldName);

    const string fieldValue3 = "我爱菊花茶10tt";

    checkSectionTokenizeFailed(fieldValue3, fieldName);
}


void TokenizeDocumentProcessorTest::
checkSectionTokenizeFailed(const string &fieldValue, const string &fieldName) {
    TokenizeDocumentProcessor documentProcessor;
    DocProcessorInitParam param;
    param.schemaPtr = _schemaPtr;
    param.analyzerFactoryPtr = _analyzerFactoryPtr;
    documentProcessor.init(param);

    ExtendDocumentPtr extendDocument(new ExtendDocument);
    RawDocumentPtr rawDoc(new IE_NAMESPACE(document)::DefaultRawDocument());

    rawDoc->setField(fieldName, fieldValue);
    extendDocument->setRawDocument(rawDoc);

    ASSERT_TRUE(!documentProcessor.process(extendDocument));
}

TEST_F(TokenizeDocumentProcessorTest, testClone) {
    BS_LOG(DEBUG, "Begin Test!");
    const string fieldValue = "我爱菊花茶\03410\0350 abc\034\035ccc";
    const string fieldName = "subject";
    string strs[] = {"我", "爱", "菊", "花", "茶"};
    vector<string> vecStr(strs, strs + sizeof(strs)/sizeof(string));
    string strs2[] = {"0"," ", "abc"};
    vector<string> vecStr2(strs2, strs2 + sizeof(strs2)/sizeof(string));
    string strs3[] = {"ccc"};
    vector<string> vecStr3(strs3, strs3 + sizeof(strs3)/sizeof(string));
    vector<vector<string> > result;
    result.push_back(vecStr);
    result.push_back(vecStr2);
    result.push_back(vecStr3);

    vector<int32_t> sectionWeightVec;
    sectionWeightVec.push_back(10);
    sectionWeightVec.push_back(0);
    sectionWeightVec.push_back(0);

    TokenizeDocumentProcessor documentProcessor;
    DocProcessorInitParam param;
    param.schemaPtr = _schemaPtr;
    param.analyzerFactoryPtr = _analyzerFactoryPtr;
    documentProcessor.init(param);
    checkSectionTokenize(fieldValue, fieldName, result, sectionWeightVec, documentProcessor);

    TokenizeDocumentProcessor *documentProcessor2 = dynamic_cast<TokenizeDocumentProcessor*>(documentProcessor.clone());
    checkSectionTokenize(fieldValue, fieldName, result, sectionWeightVec, *documentProcessor2);
    documentProcessor2->destroy();
}

TEST_F(TokenizeDocumentProcessorTest, testSectionTokenizeWithMultiValueField)
{
    BS_LOG(DEBUG, "Begin Test!");
    const string fieldValue = "aaa\035bbb\035ccc";
    const string fieldName = "multi_string";
    string strs[] = {"aaa", "\035", "bbb", "\035", "ccc"};
    vector<string> vecStr(strs, strs + sizeof(strs)/sizeof(string));
    vector<vector<string> > result;
    result.push_back(vecStr);

    vector<int32_t> sectionWeightVec;
    sectionWeightVec.push_back(0);

    testSectionTokenize(fieldValue, fieldName, result, sectionWeightVec);
}

TEST_F(TokenizeDocumentProcessorTest, testSectionTokenizeWithEmptySection)
{
    BS_LOG(DEBUG, "Begin Test!");
    const string fieldValue = "\035\035\035";
    const string fieldName = "title";
    vector<vector<string> > result;
    vector<int32_t> sectionWeightVec;

    testSectionTokenize(fieldValue, fieldName, result, sectionWeightVec);

    const string fieldValue2 = "\035\035\03412\035";
    testSectionTokenize(fieldValue2, fieldName, result, sectionWeightVec);

    const string fieldValue3 = "\035\035\03412";
    testSectionTokenize(fieldValue3, fieldName, result, sectionWeightVec);

    const string fieldValue4 = "\03412\034\035\035";
    testSectionTokenize(fieldValue4, fieldName, result, sectionWeightVec);

    const string fieldValue5 = "\034\035\035";
    testSectionTokenize(fieldValue5, fieldName, result, sectionWeightVec);

    const string fieldValue6 = "";
    testSectionTokenize(fieldValue6, fieldName, result, sectionWeightVec);
}

TEST_F(TokenizeDocumentProcessorTest, testSectionTokenizeWithEmptyFieldValue)
{
    BS_LOG(DEBUG, "Begin Test!");
    const string fieldValue = "";
    const string fieldName = "multi_string";
    vector<vector<string> > result;
    vector<int32_t> sectionWeightVec;
    testSectionTokenize(fieldValue, fieldName, result, sectionWeightVec, "");

    const string fieldName2 = "subject";
    testSectionTokenize(fieldValue, fieldName2, result, sectionWeightVec, "");

    const string fieldName3 = "user_id";

    testSectionTokenize(fieldValue, fieldName3, result, sectionWeightVec, "");
}


void TokenizeDocumentProcessorTest::
testSectionTokenize(const string &fieldValue, const string &fieldName,
                    const vector<vector<string> > &sectionResult,
                    const vector<int32_t> &sectionWeight,
                    const string &fieldAnalyzerName)
{
    BS_LOG(DEBUG, "testSectionTokenize: fieldName[%s], fieldValue[%s]",
        fieldName.c_str(), fieldValue.c_str());
    TokenizeDocumentProcessor documentProcessor;
    DocProcessorInitParam param;
    param.schemaPtr = _schemaPtr;
    param.analyzerFactoryPtr = _analyzerFactoryPtr;
    documentProcessor.init(param);
    checkSectionTokenize(fieldValue, fieldName, sectionResult,
                         sectionWeight, documentProcessor, fieldAnalyzerName);
}

void TokenizeDocumentProcessorTest::
checkSectionTokenize(const string &fieldValue, const string &fieldName,
                     const vector<vector<string> > &sectionResult,
                     const vector<int32_t> &sectionWeight,
                     TokenizeDocumentProcessor &documentProcessor,
                     const string &fieldAnalyzerName)
{
    ExtendDocumentPtr extendDocument(new ExtendDocument);
    fieldid_t fieldId = getFieldId(fieldName);
    extendDocument->setFieldAnalyzerName(fieldId, fieldAnalyzerName);
    RawDocumentPtr rawDoc(new IE_NAMESPACE(document)::DefaultRawDocument());

    rawDoc->setField(fieldName, fieldValue);
    extendDocument->setRawDocument(rawDoc);

    ASSERT_TRUE(documentProcessor.process(extendDocument));

    TokenizeFieldPtr tokenizeField = extendDocument->getTokenizeDocument()->getField(fieldId);
    ASSERT_TRUE(tokenizeField.get());

    uint32_t sectionNum = sectionResult.size();
    ASSERT_EQ((uint32_t)sectionNum, tokenizeField->getSectionCount());

    TokenizeField::Iterator it = tokenizeField->createIterator();
    for (size_t i = 0; i < sectionResult.size(); ++i) {
        vector<string> vecStr = sectionResult[i];
        checkResult(*it, vecStr);
        it.next();
    }
}

IndexPartitionSchemaPtr TokenizeDocumentProcessorTest::readIndexSchema(const string& schemaPath) {
    ifstream fin(schemaPath.c_str());
    assert(fin);

    string jsonStr;
    string line;
    while (getline(fin, line))
    {
        jsonStr.append(line);
    }

    Any any = ParseJson(jsonStr);
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    FromJson(*schema, any);

    return schema;
}

fieldid_t TokenizeDocumentProcessorTest::getFieldId(const string& fieldName) const
{
    FieldSchemaPtr fieldSchema = _schemaPtr->GetFieldSchema();
    return fieldSchema->GetFieldId(fieldName);
}

void TokenizeDocumentProcessorTest::checkResult(TokenizeSection *section,
        vector<string> &sectionResult)
{
    ASSERT_TRUE(section);
    size_t basicLength = section->getBasicLength();
    TokenizeSection::Iterator tokenIt = section->createIterator();
    ASSERT_EQ(sectionResult.size(), basicLength);

    for (size_t i = 0; i < sectionResult.size(); ++i) {
        analyzer::Token *token = tokenIt.getToken();
        assert(token);
        StringTokenizer tokenizer(sectionResult[i], ":",
                StringTokenizer::TOKEN_IGNORE_EMPTY);
        ASSERT_TRUE(tokenizer.getNumTokens() > 0);
        ASSERT_EQ(token->getText(), tokenizer[0]);
        size_t j = 1;
        for (j = 1; j < tokenizer.getNumTokens(); j++) {
             if (tokenIt.nextExtend() == false) {
                break;
            }
            analyzer::Token *token = tokenIt.getToken();
            ASSERT_TRUE(token);
            ASSERT_EQ(token->getText(), tokenizer[j]);
        }
        ASSERT_EQ(j, tokenizer.getNumTokens());
        ASSERT_TRUE(!tokenIt.nextExtend());
        tokenIt.nextBasic();
    }
}

TEST_F(TokenizeDocumentProcessorTest, testAttributeField)
{
    BS_LOG(DEBUG, "Begin Test!");
    const string fieldValue = "aaaaaaaaaaa";
    const string fieldName = "user_id";
    TokenizeDocumentProcessor documentProcessor;
    DocProcessorInitParam param;
    param.schemaPtr = _schemaPtr;
    param.analyzerFactoryPtr = _analyzerFactoryPtr;
    documentProcessor.init(param);

    ExtendDocumentPtr extendDocument(new ExtendDocument);
    RawDocumentPtr rawDoc(new IE_NAMESPACE(document)::DefaultRawDocument());

    rawDoc->setField(fieldName, fieldValue);
    extendDocument->setRawDocument(rawDoc);

    //initStackSpace();
    ASSERT_TRUE(documentProcessor.process(extendDocument));

}

TEST_F(TokenizeDocumentProcessorTest, testSimpleAnalyzerWithSection) {
    TokenNodeAllocatorPtr allocator(new TokenNodeAllocator);
    TokenizeFieldPtr field(new TokenizeField(allocator));
    field->setFieldId(1);
    Tokenizer *tokenizer = new SimpleTokenizer("\t");
    typedef std::tr1::shared_ptr<Analyzer> AnalyzerPtr;
    AnalyzerPtr analyzer(new Analyzer(tokenizer));
    AnalyzerInfo info;
    analyzer->init(&info, NULL);

    AnalyzerFactoryPtr factory(new AnalyzerFactory);
    TokenizeDocumentProcessor processor;
    DocProcessorInitParam param;
    param.schemaPtr = _schemaPtr;
    param.analyzerFactoryPtr = factory;
    processor.init(param);
    string str = "rose\tflower\t\t3\t";
    ASSERT_TRUE(processor.doTokenizeTextField(field, ConstString(str), analyzer.get()));
    ASSERT_EQ((uint32_t)1, field->getSectionCount());
    TokenizeField::Iterator it = field->createIterator();
    ASSERT_TRUE(!it.isEnd());
    TokenizeSection *section = *it;
    ASSERT_EQ((section_weight_t)3, section->getSectionWeight());
}

TEST_F(TokenizeDocumentProcessorTest, testAssignInvalidFieldAnalyzer)
{
    BS_LOG(DEBUG, "Begin Test!");
    const string fieldValue = "我爱菊花茶";
    const string fieldName = "subject";

    TokenizeDocumentProcessor documentProcessor;
    DocProcessorInitParam param;
    param.schemaPtr = _schemaPtr;
    param.analyzerFactoryPtr = _analyzerFactoryPtr;
    documentProcessor.init(param);

    ExtendDocumentPtr extendDocument(new ExtendDocument);
    string invalidAnalyzerName = "invalidAnalyzerName";
    extendDocument->setFieldAnalyzerName(getFieldId(fieldName), invalidAnalyzerName);
    RawDocumentPtr rawDoc(new IE_NAMESPACE(document)::DefaultRawDocument());
    rawDoc->setField(fieldName, fieldValue);
    extendDocument->setRawDocument(rawDoc);
    ASSERT_TRUE(!documentProcessor.process(extendDocument));
}

void TokenizeDocumentProcessorTest::initStackSpace()
{
    char b[2048*4];
    memset(b,0,sizeof(b));
}

}
}
