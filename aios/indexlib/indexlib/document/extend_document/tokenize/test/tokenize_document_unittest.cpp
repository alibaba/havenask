#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/document/extend_document/tokenize/tokenize_field.h"
#include "indexlib/document/extend_document/tokenize_document.h"

IE_NAMESPACE_BEGIN(document);

class TokenizeDocumentTest : public INDEXLIB_TESTBASE {
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
};


void TokenizeDocumentTest::CaseSetUp() {
}

void TokenizeDocumentTest::CaseTearDown() {
}

TEST_F(TokenizeDocumentTest, testSimpleProcess) {
    TokenizeDocument tokenizeDocument;
    tokenizeDocument.reserve(10);
    TokenizeFieldPtr tokenizeField = tokenizeDocument.getField(1);
    ASSERT_TRUE(NULL == tokenizeField.get());
}

TEST_F(TokenizeDocumentTest, testCreateField){
    TokenizeDocument tokenizeDocument;
    tokenizeDocument.reserve(10);
    TokenizeFieldPtr tokenizeField = tokenizeDocument.createField(2);
    ASSERT_TRUE(NULL != tokenizeField.get());
    ASSERT_TRUE(NULL == tokenizeDocument._fields[0].get());
    ASSERT_TRUE(NULL == tokenizeDocument._fields[1].get());
    TokenizeFieldPtr tokenizeField1 = tokenizeDocument.createField(5);
    ASSERT_TRUE(NULL != tokenizeField1.get());
    ASSERT_TRUE(NULL == tokenizeDocument._fields[3].get());
    ASSERT_TRUE(NULL == tokenizeDocument._fields[4].get());
    ASSERT_EQ(tokenizeField, tokenizeDocument._fields[2]);
    ASSERT_EQ(tokenizeField1, tokenizeDocument._fields[5]);
}

TEST_F(TokenizeDocumentTest, testGetField){
    TokenizeDocument tokenizeDocument;
    tokenizeDocument.reserve(10);
    ASSERT_TRUE(NULL == tokenizeDocument.getField(2).get());

    TokenizeFieldPtr tokenizeField = tokenizeDocument.createField(2);
    ASSERT_TRUE(NULL == tokenizeDocument.getField(1).get());
    ASSERT_EQ(tokenizeField, tokenizeDocument.getField(2));
}

IE_NAMESPACE_END(document);
