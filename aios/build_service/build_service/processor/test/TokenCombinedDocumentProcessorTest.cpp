#include "build_service/processor/TokenCombinedDocumentProcessor.h"
#include "build_service/document/ExtendDocument.h"
#include "build_service/test/unittest.h"
#include "build_service/processor/TokenCombinedDocumentProcessor.h"
#include "build_service/analyzer/Token.h"
#include <autil/StringUtil.h>
#include <set>
#include <string>
#include <autil/StringTokenizer.h>
#include <indexlib/document/raw_document/default_raw_document.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
using namespace build_service::document;
using namespace build_service::analyzer;

namespace build_service {
namespace processor {

class TokenCombinedDocumentProcessorTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();

protected:
    IE_NAMESPACE(config)::IndexPartitionSchemaPtr makeFakeSchema();
    document::ExtendDocumentPtr makeInitialDocument();

    void insertFakeTokenizeDocument(const document::ExtendDocumentPtr &extendDocumentPtr,
                                    const std::string &tokenString);
    void insertFakeTokenizeDocument(const document::ExtendDocumentPtr &extendDocumentPtr,
                                    const std::vector<std::string> &tokenStrings);

    void checkTokenList(const std::string &tokenStr, document::TokenizeSection *tokenSection);
    void innerTestOneSection(const std::string& originalDoc,
                             const std::string& expectedDoc);
    void innerTestSections(const std::vector<std::string>& originalDocs,
                           const std::vector<std::string>& expectedDocs);
protected:
    IE_NAMESPACE(config)::IndexPartitionSchemaPtr _schemaPtr;
    TokenCombinedDocumentProcessor *_tokenProcessor;
    std::set<std::string> _stopWordList;
};


void TokenCombinedDocumentProcessorTest::setUp() {
    _tokenProcessor = new TokenCombinedDocumentProcessor();
    _schemaPtr = makeFakeSchema();
    KeyValueMap kvMap;
    kvMap[TokenCombinedDocumentProcessor::PREFIX_IDENTIFIER] = "pp pp2";
    kvMap[TokenCombinedDocumentProcessor::SUFFIX_IDENTIFIER] = "ss ss2";
    kvMap[TokenCombinedDocumentProcessor::INFIX_IDENTIFIER] = "ii ii2 ii3";
    _tokenProcessor->init(kvMap, _schemaPtr, NULL);
    _stopWordList.clear();
}

void TokenCombinedDocumentProcessorTest::tearDown() {
    DELETE_AND_SET_NULL(_tokenProcessor);
}

TEST_F(TokenCombinedDocumentProcessorTest, testSimpleProcessForPrefix) {
    BS_LOG(DEBUG, "Begin Test!");
    string originalDoc = "aa, pp, CC";
    string expectedProcessedDoc = "aa,aa,0,false; pp,pp,1,true; CC,cc,1,false; ppCC,ppcc,0,false";
    innerTestOneSection(originalDoc, expectedProcessedDoc);
}

TEST_F(TokenCombinedDocumentProcessorTest, testSimpleProcessForSuffix) {
    BS_LOG(DEBUG, "Begin Test!");
    string originalDoc = "AA, ss, hh";
    string expectedProcessedDoc = "AA,aa,0,false; AAss,aass,0,false; ss,ss,1,true; hh,hh,1,false";
    innerTestOneSection(originalDoc, expectedProcessedDoc);
}

TEST_F(TokenCombinedDocumentProcessorTest, testSimpleProcessForInfix) {
    BS_LOG(DEBUG, "Begin Test!");
    string originalDoc = "AA, ii, hh";
    string expectedProcessedDoc = "AA,aa,0,false; AAii,aaii,0,false; ii,ii,1,true; hh,hh,1,false; iihh,iihh,0,false";
    innerTestOneSection(originalDoc, expectedProcessedDoc);
}

TEST_F(TokenCombinedDocumentProcessorTest, testProcessForPrefixWithNoCombination) {
    BS_LOG(DEBUG, "Begin Test!");
    string originalDoc = "aa, pp";
    string expectedProcessedDoc = "aa,aa,0,false; pp,pp,1,false";
    innerTestOneSection(originalDoc, expectedProcessedDoc);
}

TEST_F(TokenCombinedDocumentProcessorTest, testProcessForSuffixWithNoCombination) {
    BS_LOG(DEBUG, "Begin Test!");
    string originalDoc = "ss, hh";
    string expectedProcessedDoc = "ss,ss,0,false; hh,hh,1,false";
    innerTestOneSection(originalDoc, expectedProcessedDoc);
}

TEST_F(TokenCombinedDocumentProcessorTest, testProcessForInfixWithNoCombination) {
    BS_LOG(DEBUG, "Begin Test!");
    string originalDoc1 = "ii";
    string expectedProcessedDoc1 = "ii,ii,0,false";
    innerTestOneSection(originalDoc1, expectedProcessedDoc1);

    string originalDoc2 = "ii, aa";
    string expectedProcessedDoc2 = "ii,ii,0,true;aa,aa,1,false;iiaa,iiaa,0,false";
    innerTestOneSection(originalDoc2, expectedProcessedDoc2);

    string originalDoc3 = "UU, ii";
    string expectedProcessedDoc3 = "UU,uu,0,false;UUii,uuii,0,false;ii,ii,1,true";
    innerTestOneSection(originalDoc3, expectedProcessedDoc3);
}

TEST_F(TokenCombinedDocumentProcessorTest, testProcessForTwoPrefixCombined) {
    BS_LOG(DEBUG, "Begin Test!");
    string originalDoc = "aa, pp, pp2, cc";
    string expectedProcessedDoc = "aa,aa,0,false; pp,pp,1,true; pp2,pp2,1,false; pppp2,pppp2,0,false; cc,cc,1,false";
    innerTestOneSection(originalDoc, expectedProcessedDoc);
}

TEST_F(TokenCombinedDocumentProcessorTest, testProcessForTwoSuffixCombined) {
    BS_LOG(DEBUG, "Begin Test!");
    string originalDoc = "AA, ss, ss2, hh";
    string expectedProcessedDoc
        = "AA,aa,0,false; AAss,aass,0,false; ss,ss,1,true; ssss2,ssss2,0,false; ss2,ss2,1,true; hh,hh,1,false";
    innerTestOneSection(originalDoc, expectedProcessedDoc);
}

TEST_F(TokenCombinedDocumentProcessorTest, testProcessForTwoInfixCombined) {
    BS_LOG(DEBUG, "Begin Test!");
    string originalDoc = "AA, ii, ii2, hh";
    string expectedProcessedDoc = "AA,aa,0,false; AAii,aaii,0,false; ii,ii,1,true; "
                                  "ii2,ii2,1,false; iiii2,iiii2,0,false; hh,hh,1,false";
    innerTestOneSection(originalDoc, expectedProcessedDoc);
}

TEST_F(TokenCombinedDocumentProcessorTest, testSimpleProcessMultiSection) {
    BS_LOG(DEBUG, "Begin Test!");
    vector<string> originalDocs;
    vector<string> expectedDocs;
    originalDocs.push_back("aa, pp, CC, AA, ss, hh");
    expectedDocs.push_back("aa,aa,0,false; pp,pp,1,true; CC,cc,1,false; ppCC,ppcc,0,false;"
                           "AA,aa,1,false; AAss,aass,0,false; ss,ss,1,true; hh,hh,1,false");

    originalDocs.push_back("AA, ii, hh");
    expectedDocs.push_back("AA,aa,0,false; AAii,aaii,0,false; ii,ii,1,true; hh,hh,1,false; iihh,iihh,0,false");

    innerTestSections(originalDocs, expectedDocs);
}

TEST_F(TokenCombinedDocumentProcessorTest, testRemovedStopWord) {
    BS_LOG(DEBUG, "Begin Test!");

    _stopWordList.insert("stop");

    string originalDoc = "stop, word";
    string expectedProcessedDoc = "stop,stop,0,true; word,word,1,false";
    innerTestOneSection(originalDoc, expectedProcessedDoc);

    string originalDoc2 = "aa, stop, ss";
    string expectedProcessedDoc2 = "aa, aa, 0, false; stop,stop,1,true; ss,ss,1,false";
    innerTestOneSection(originalDoc2, expectedProcessedDoc2);

    string originalDoc3 = "pp, stop, bb";
    string expectedProcessedDoc3 = "pp, pp, 0, true; stop,stop,1,true; ppstop, ppstop, 0, false; bb,bb,1,false";
    innerTestOneSection(originalDoc3, expectedProcessedDoc3);

}

TEST_F(TokenCombinedDocumentProcessorTest, testEmptyInputTokenStream) {
    BS_LOG(DEBUG, "Begin Test!");
    string originalDoc = "";
    string expectedProcessedDoc = "";
    innerTestOneSection(originalDoc, expectedProcessedDoc);
}

void TokenCombinedDocumentProcessorTest::innerTestOneSection(const std::string& originalDoc,
        const std::string& expectedDoc)
{
    vector<string> originalDocs;
    originalDocs.push_back(originalDoc);
    vector<string> expectedDocs;
    expectedDocs.push_back(expectedDoc);
    innerTestSections(originalDocs, expectedDocs);
}

void TokenCombinedDocumentProcessorTest::innerTestSections(const vector<string>& originalDocs,
        const vector<string>& expectedDocs)
{
    document::ExtendDocumentPtr extendDocumentPtr = makeInitialDocument();
    insertFakeTokenizeDocument(extendDocumentPtr, originalDocs);

    bool ret = _tokenProcessor->process(extendDocumentPtr);
    ASSERT_TRUE(ret);

    TokenizeDocumentPtr tokenizeDocumentPtr = extendDocumentPtr->getTokenizeDocument();
    ASSERT_TRUE(tokenizeDocumentPtr.get());
    TokenizeFieldPtr tokenFieldPtr = tokenizeDocumentPtr->getField(0);
    ASSERT_TRUE(tokenFieldPtr.get());
    ASSERT_EQ((uint32_t)expectedDocs.size(), tokenFieldPtr->getSectionCount());
    TokenizeField::Iterator sectionIt = tokenFieldPtr->createIterator();

    for (size_t i = 0; i < expectedDocs.size(); ++i) {
        TokenizeSection *tokenSection = *sectionIt;
        ASSERT_TRUE(tokenSection);
        sectionIt.next();

        checkTokenList(expectedDocs[i], tokenSection);
    }
    ASSERT_TRUE(sectionIt.isEnd());
}

ExtendDocumentPtr TokenCombinedDocumentProcessorTest::makeInitialDocument() {
    ExtendDocumentPtr extendDocumentPtr(new ExtendDocument);
    extendDocumentPtr->setRawDocument(RawDocumentPtr(new
                    IE_NAMESPACE(document)::DefaultRawDocument()));
    return extendDocumentPtr;
}

void TokenCombinedDocumentProcessorTest::insertFakeTokenizeDocument(const ExtendDocumentPtr &extendDocumentPtr,
        const string &tokenString)
{
    vector<string> tokenStrings;
    tokenStrings.push_back(tokenString);
    insertFakeTokenizeDocument(extendDocumentPtr, tokenStrings);
}

void TokenCombinedDocumentProcessorTest::insertFakeTokenizeDocument(const ExtendDocumentPtr &extendDocumentPtr,
        const vector<string> &tokenStrings)
{
    TokenizeDocumentPtr tokenizeDocumentPtr = extendDocumentPtr->getTokenizeDocument();

    TokenizeFieldPtr tokenFieldPtr = tokenizeDocumentPtr->createField(0);

    for (size_t i = 0; i < tokenStrings.size(); ++i) {
        TokenizeSection *tokenSection = tokenFieldPtr->getNewSection();
        TokenizeSection::Iterator tokenIterator = tokenSection->createIterator();

        StringTokenizer st;
        st.tokenize(tokenStrings[i], ",", StringTokenizer::TOKEN_TRIM);
        size_t tokenNum = st.getNumTokens();

        for (size_t j = 0; j < tokenNum; ++j) {
            string tokenText = st[j];
            string normalizedTokenText = tokenText;
            StringUtil::toLowerCase(normalizedTokenText);

            bool isStopWord = _stopWordList.find(tokenText) != _stopWordList.end() ? true : false;
            Token token(tokenText, j == 0 ? 0 : 1, normalizedTokenText, isStopWord);
            tokenSection->insertBasicToken(token, tokenIterator);
        }
    }
}

IndexPartitionSchemaPtr TokenCombinedDocumentProcessorTest::makeFakeSchema()
{
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    schema->AddFieldConfig("body", ft_text);
    return schema;
}

//format: "token_text, token_normalizd_text, token_pos, is_stopword; token_text, token_normalizd_text, token_pos, is_stopword"
void TokenCombinedDocumentProcessorTest::checkTokenList(const string &tokenStr,
        TokenizeSection *tokenSection)
{
    //parse expect token list
    vector<Token> checkTokenList;
    StringTokenizer st;
    st.tokenize(tokenStr, ";", StringTokenizer::TOKEN_TRIM);
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        StringTokenizer st2;
        string oneTokenStr = st[i];
        st2.tokenize(oneTokenStr, ",", StringTokenizer::TOKEN_TRIM);
        if (st2.getNumTokens() != 4) {
            BS_LOG(ERROR, "tokenStr format error, substr:%s", oneTokenStr.c_str());
            continue;
        }
        string tokenText = st2[0];
        string normalizedTokenText = st2[1];
        int32_t pos;
        StringUtil::strToInt32(st2[2].c_str(), pos);
        bool isStopWord = st2[3] == "true" ? true : false;
        Token token(tokenText, (uint32_t)pos, normalizedTokenText, isStopWord);
        checkTokenList.push_back(token);
    }

    ASSERT_TRUE(tokenSection);

    //check token list in TokenSection
    TokenizeSection::Iterator tokenIt = tokenSection->createIterator();
    for (vector<Token>::const_iterator it = checkTokenList.begin();
         it != checkTokenList.end(); ++it)
    {
        const Token &expectToken = *it;
        const Token *tmpToken = tokenIt.getToken();
        ASSERT_TRUE(tmpToken);
        ASSERT_EQ(expectToken.getText(), tmpToken->getText());
        ASSERT_EQ(expectToken.getNormalizedText(), tmpToken->getNormalizedText());
        ASSERT_EQ(expectToken.getPosition(), tmpToken->getPosition());
        ASSERT_EQ(expectToken.isStopWord(), tmpToken->isStopWord());
        tokenIt.next();
    }
    ASSERT_TRUE(!tokenIt.next());
}


}
}
