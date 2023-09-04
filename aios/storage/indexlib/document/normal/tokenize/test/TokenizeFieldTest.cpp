#include "indexlib/document/normal/tokenize/TokenizeField.h"

#include "indexlib/util/testutil/unittest.h"

namespace indexlib { namespace document {

class TokenizeFieldTest : public INDEXLIB_TESTBASE
{
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

protected:
    std::shared_ptr<TokenNodeAllocator> _tokenNodeAllocatorPtr;
};

void TokenizeFieldTest::CaseSetUp() { _tokenNodeAllocatorPtr.reset(new TokenNodeAllocator); }

void TokenizeFieldTest::CaseTearDown() {}

TEST_F(TokenizeFieldTest, testSimpleProcess)
{
    TokenizeField tokenField(_tokenNodeAllocatorPtr);
    tokenField.setFieldId((fieldid_t)123);
    ASSERT_EQ((fieldid_t)123, tokenField.getFieldId());
    ASSERT_EQ((uint32_t)0, tokenField.getSectionCount());

    TokenizeSection* tokenizeSection = tokenField.getNewSection();
    ASSERT_TRUE(tokenizeSection);
    ASSERT_TRUE(tokenField.isEmpty());
    ASSERT_EQ((uint32_t)1, tokenField.getSectionCount());
}

TEST_F(TokenizeFieldTest, testIsEmpty)
{
    TokenizeField tokenField(_tokenNodeAllocatorPtr);
    ASSERT_EQ((uint32_t)0, tokenField.getSectionCount());

    TokenizeSection* tokenizeSection = tokenField.getNewSection();
    ASSERT_TRUE(tokenizeSection);

    tokenizeSection = tokenField.getNewSection();
    ASSERT_TRUE(tokenizeSection);

    ASSERT_EQ((uint32_t)2, tokenField.getSectionCount());
    ASSERT_TRUE(tokenField.isEmpty());

    AnalyzerToken token;
    TokenizeSection::Iterator sectionIt = tokenizeSection->createIterator();
    tokenizeSection->insertBasicToken(token, sectionIt);
    ASSERT_TRUE(!tokenField.isEmpty());
}

TEST_F(TokenizeFieldTest, testErase)
{
    TokenizeField tokenField(_tokenNodeAllocatorPtr);
    tokenField.getNewSection();
    tokenField.getNewSection();
    ASSERT_EQ((uint32_t)2, tokenField.getSectionCount());
    TokenizeField::Iterator sectionIt = tokenField.createIterator();
    tokenField.erase(sectionIt);
    ASSERT_EQ((uint32_t)1, tokenField.getSectionCount());
    tokenField.erase(sectionIt);
    ASSERT_EQ((uint32_t)0, tokenField.getSectionCount());
    tokenField.erase(sectionIt);
    ASSERT_EQ((uint32_t)0, tokenField.getSectionCount());

    ASSERT_TRUE(tokenField.isEmpty());
}

TEST_F(TokenizeFieldTest, testGetNewSection)
{
    TokenizeField tokenField(_tokenNodeAllocatorPtr);
    TokenizeSection* tokenSection = tokenField.getNewSection();
    ASSERT_EQ((uint32_t)1, tokenField.getSectionCount());
    ASSERT_TRUE(tokenSection);
}

TEST_F(TokenizeFieldTest, testCreateIterator)
{
    TokenizeField tokenField(_tokenNodeAllocatorPtr);

    ASSERT_EQ((uint32_t)0, tokenField.getSectionCount());
    TokenizeSection* tokenSection = tokenField.getNewSection();
    TokenizeField::Iterator it = tokenField.createIterator();
    ASSERT_EQ((*it), tokenSection);
    ASSERT_EQ((uint32_t)1, tokenField.getSectionCount());

    TokenizeSection* tokenSection2 = tokenField.getNewSection();
    ASSERT_EQ((uint32_t)2, tokenField.getSectionCount());

    it = tokenField.createIterator();
    ASSERT_EQ((*it), tokenSection);

    ASSERT_TRUE(it.next());
    ASSERT_EQ((*it), tokenSection2);

    ASSERT_TRUE(it.next());
    ASSERT_TRUE(it.isEnd());

    ASSERT_TRUE(!it.next());
}

TEST_F(TokenizeFieldTest, testIteratorNextAndIsEnd)
{
    TokenizeField tokenField(_tokenNodeAllocatorPtr);

    TokenizeField::Iterator it = tokenField.createIterator();
    ASSERT_TRUE(it.isEnd());
    ASSERT_TRUE(!it.next());
    ASSERT_EQ((uint32_t)0, tokenField.getSectionCount());

    TokenizeSection* tokenSection = tokenField.getNewSection();
    TokenizeSection* tokenSection2 = tokenField.getNewSection();
    ASSERT_EQ((uint32_t)2, tokenField.getSectionCount());

    it = tokenField.createIterator();
    ASSERT_TRUE(!it.isEnd());
    ASSERT_EQ((*it), tokenSection);

    ASSERT_TRUE(it.next());
    ASSERT_TRUE(!it.isEnd());
    ASSERT_EQ((*it), tokenSection2);

    ASSERT_TRUE(it.next());
    ASSERT_TRUE(it.isEnd());
    ASSERT_TRUE(!it.next());
}
}} // namespace indexlib::document
