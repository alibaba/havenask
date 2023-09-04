#include "indexlib/document/normal/tokenize/TokenizeDocument.h"

#include "indexlib/document/normal/tokenize/TokenizeField.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib { namespace document {

class TokenizeDocumentTest : public INDEXLIB_TESTBASE
{
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
};

void TokenizeDocumentTest::CaseSetUp() {}

void TokenizeDocumentTest::CaseTearDown() {}

TEST_F(TokenizeDocumentTest, testSimpleProcess)
{
    TokenizeDocument tokenizeDocument;
    tokenizeDocument.reserve(10);
    auto tokenizeField = tokenizeDocument.getField(1);
    ASSERT_TRUE(NULL == tokenizeField.get());
}

TEST_F(TokenizeDocumentTest, testCreateField)
{
    TokenizeDocument tokenizeDocument;
    tokenizeDocument.reserve(10);
    auto tokenizeField = tokenizeDocument.createField(2);
    ASSERT_TRUE(NULL != tokenizeField.get());
    ASSERT_TRUE(NULL == tokenizeDocument._fields[0].get());
    ASSERT_TRUE(NULL == tokenizeDocument._fields[1].get());
    auto tokenizeField1 = tokenizeDocument.createField(5);
    ASSERT_TRUE(NULL != tokenizeField1.get());
    ASSERT_TRUE(NULL == tokenizeDocument._fields[3].get());
    ASSERT_TRUE(NULL == tokenizeDocument._fields[4].get());
    ASSERT_EQ(tokenizeField, tokenizeDocument._fields[2]);
    ASSERT_EQ(tokenizeField1, tokenizeDocument._fields[5]);
}

TEST_F(TokenizeDocumentTest, testGetField)
{
    TokenizeDocument tokenizeDocument;
    tokenizeDocument.reserve(10);
    ASSERT_TRUE(NULL == tokenizeDocument.getField(2).get());

    auto tokenizeField = tokenizeDocument.createField(2);
    ASSERT_TRUE(NULL == tokenizeDocument.getField(1).get());
    ASSERT_EQ(tokenizeField, tokenizeDocument.getField(2));
}
}} // namespace indexlib::document
