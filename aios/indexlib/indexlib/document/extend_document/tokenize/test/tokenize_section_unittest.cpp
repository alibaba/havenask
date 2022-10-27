#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

#include "indexlib/document/extend_document/tokenize/tokenize_section.h"
#include <autil/StringUtil.h>
#include <autil/Thread.h>

using namespace std;
using namespace std::tr1;
using namespace autil;

IE_NAMESPACE_BEGIN(document);

class TokenizeSectionTest : public INDEXLIB_TESTBASE {
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
protected:
    void prepareSection(TokenizeSection &section);
    static void allocate();
protected:
    TokenNodeAllocatorPtr _tokenNodeAllocatorPtr;
};


void TokenizeSectionTest::CaseSetUp() {
    _tokenNodeAllocatorPtr.reset(new TokenNodeAllocator);
}

void TokenizeSectionTest::CaseTearDown() {
}

TEST_F(TokenizeSectionTest, testCreateIterator) {
    TokenizeSection section(_tokenNodeAllocatorPtr);
    prepareSection(section);
    TokenizeSection::Iterator it = section.createIterator();
    ASSERT_TRUE(!it._preNode);
    ASSERT_TRUE(it._curNode == section._header);
    ASSERT_TRUE(it._basicNode == section._header);
    ASSERT_TRUE(it._section == &section);
    it.nextBasic();
    ASSERT_TRUE(it._curNode == section._header->getNextBasic());
    ASSERT_TRUE(it._preNode == section._header);
    ASSERT_TRUE(it._basicNode == section._header->getNextBasic());
    it.nextExtend();
    ASSERT_TRUE(it._curNode == section._header->getNextBasic()->getNextExtend());
    ASSERT_TRUE(it._preNode == section._header->getNextBasic());
    ASSERT_TRUE(it._basicNode == section._header->getNextBasic());
}

TEST_F(TokenizeSectionTest, testInsertBasicToken)
{
    //insert 1, 2, 3, 4
    TokenizeSection section(_tokenNodeAllocatorPtr);
    TokenizeSection::Iterator it = section.createIterator();
    int n = 4;
    for (int i = 0; i < n; i++) {
        AnalyzerToken token;
        token.setText(StringUtil::toString(i));
        section.insertBasicToken(token, it);
    }

    it = section.createIterator();
    int i = 0;
    while (*it != NULL) {
        ASSERT_EQ(StringUtil::toString(i++), (*it)->getText());
        it.next();
    }
}

TEST_F(TokenizeSectionTest, testInsertExtendToken) {
    TokenizeSection section(_tokenNodeAllocatorPtr);
    TokenizeSection::Iterator it = section.createIterator();
    int n = 4;
    for (int i = 0; i < n; i++) {
        AnalyzerToken token;
        token.setText(StringUtil::toString(i));
        section.insertBasicToken(token, it);

        for (int j = 0; j < i+2; ++j)
        {
            token.setText(StringUtil::toString(j));
            section.insertExtendToken(token,it);
        }
    }

    it = section.createIterator();

    for (int i = 0; i < n; ++i) {
        ASSERT_TRUE((*it));
        ASSERT_EQ(StringUtil::toString(i),
                  (*it)->getText());
        for (int j = 0; j < i + 2; ++j) {
            ASSERT_TRUE(it.nextExtend());
            ASSERT_TRUE(*it);
            ASSERT_EQ(StringUtil::toString(j),
                      (*it)->getText());
        }
        ASSERT_TRUE(!it.nextExtend());
        it.nextBasic();
    }
    ASSERT_TRUE(!(*it));
}

TEST_F(TokenizeSectionTest, testEraseExtend)
{
    TokenizeSection section(_tokenNodeAllocatorPtr);
    prepareSection(section);
    ASSERT_EQ((uint32_t)4, section._basicLength);
    ASSERT_EQ((uint32_t)18, section._count);

    TokenizeSection::Iterator it = section.createIterator();
    for (int i = 0; i < 4; ++i) {
        section.eraseExtend(it._basicNode);
        it.nextBasic();
    }
    ASSERT_EQ((uint32_t)4, section._basicLength);
    ASSERT_EQ((uint32_t)4, section._count);

    it = section.createIterator();
    for (int i = 0; i < 4; ++i) {
        ASSERT_TRUE((*it));
        ASSERT_EQ(StringUtil::toString(i),
                  (*it)->getText());
        ASSERT_TRUE(!it.nextExtend());
        it.nextBasic();
    }
    ASSERT_TRUE(!(*it));
}

TEST_F(TokenizeSectionTest, testEraseSectionHead)
{
    TokenizeSection section(_tokenNodeAllocatorPtr);
    prepareSection(section);
    ASSERT_EQ((uint32_t)4, section._basicLength);
    ASSERT_EQ((uint32_t)18, section._count);

    TokenizeSection::Iterator it = section.createIterator();
    section.erase(it);

    ASSERT_TRUE(it._curNode == section._header);
    ASSERT_TRUE(it._preNode == NULL);
    ASSERT_EQ((uint32_t)3, section._basicLength);
    ASSERT_EQ((uint32_t)15, section._count);
    for (int i = 1; i < 4; ++i) {
        ASSERT_TRUE((*it));
        ASSERT_EQ(StringUtil::toString(i), (*it)->getText());
        for (int j = 0; j < i + 2; ++j) {
            ASSERT_TRUE(it.nextExtend());
            ASSERT_TRUE(*it);
            ASSERT_EQ(StringUtil::toString(j), (*it)->getText());
        }
        ASSERT_TRUE(!it.nextExtend());
        it.nextBasic();
    }
    ASSERT_TRUE(!(*it));
}

TEST_F(TokenizeSectionTest, testEraseBasicNode)
{
    TokenizeSection section(_tokenNodeAllocatorPtr);
    prepareSection(section);
    ASSERT_EQ((uint32_t)4, section._basicLength);
    ASSERT_EQ((uint32_t)18, section._count);

    TokenizeSection::Iterator it = section.createIterator();
    it.nextBasic();
    section.erase(it);

    ASSERT_EQ((uint32_t)3, section._basicLength);
    ASSERT_EQ((uint32_t)14, section._count);
    it = section.createIterator();
    for (int i = 0; i < 4; ++i) {
        if (i == 1) {
            continue;
        }
        ASSERT_TRUE((*it));
        ASSERT_EQ(StringUtil::toString(i), (*it)->getText());
        for (int j = 0; j < i + 2; ++j) {
            ASSERT_TRUE(it.nextExtend());
            ASSERT_TRUE(*it);
            ASSERT_EQ(StringUtil::toString(j), (*it)->getText());
        }
        ASSERT_TRUE(!it.nextExtend());
        it.nextBasic();
    }
    ASSERT_TRUE(!(*it));
}

TEST_F(TokenizeSectionTest, testEraseExtendNode)
{
    TokenizeSection section(_tokenNodeAllocatorPtr);
    prepareSection(section);
    ASSERT_EQ((uint32_t)4, section._basicLength);
    ASSERT_EQ((uint32_t)18, section._count);

    TokenizeSection::Iterator it = section.createIterator();
    it.nextBasic();
    it.nextExtend();
    it.nextExtend();
    section.erase(it);

    ASSERT_EQ((uint32_t)4, section._basicLength);
    ASSERT_EQ((uint32_t)17, section._count);

    it = section.createIterator();
    for (int i = 0; i < 4; ++i) {
        ASSERT_TRUE((*it));
        ASSERT_EQ(StringUtil::toString(i), (*it)->getText());
        for (int j = 0; j < i + 2; ++j) {
            if (i == 1 && j == 1) {
                continue;
            }
            ASSERT_TRUE(it.nextExtend());
            ASSERT_TRUE(*it);
            ASSERT_EQ(StringUtil::toString(j), (*it)->getText());
        }
        ASSERT_TRUE(!it.nextExtend());
        it.nextBasic();
    }
    ASSERT_TRUE(!(*it));
}


TEST_F(TokenizeSectionTest, testEraseNullIterator)
{
    TokenizeSection section(_tokenNodeAllocatorPtr);
    prepareSection(section);
    ASSERT_EQ((uint32_t)4, section._basicLength);
    ASSERT_EQ((uint32_t)18, section._count);

    TokenizeSection::Iterator it;
    section.erase(it);

    ASSERT_EQ((uint32_t)4, section._basicLength);
    ASSERT_EQ((uint32_t)18, section._count);
}

TEST_F(TokenizeSectionTest, testIteratorNext)
{
    TokenizeSection section(_tokenNodeAllocatorPtr);
    prepareSection(section);
    TokenizeSection::Iterator it = section.createIterator();

    ASSERT_EQ((uint32_t)4, section._basicLength);
    ASSERT_EQ((uint32_t)18, section._count);

     for (int i = 0; i < 4; ++i) {
        ASSERT_TRUE((*it));
        ASSERT_EQ(StringUtil::toString(i), (*it)->getText());
        for (int j = 0; j < i + 2; ++j) {
            it.next();
            ASSERT_TRUE(*it);
            ASSERT_EQ(StringUtil::toString(j), (*it)->getText());
        }
        it.next();
    }
     ASSERT_TRUE(!(*it));

}

TEST_F(TokenizeSectionTest, testIteratorNextBasic)
{
    TokenizeSection section(_tokenNodeAllocatorPtr);
    prepareSection(section);
    TokenizeSection::Iterator it = section.createIterator();

    for (int i = 0; i < 4; ++i) {
        ASSERT_TRUE((*it));
        ASSERT_EQ(StringUtil::toString(i), (*it)->getText());
        it.nextBasic();
    }
     ASSERT_TRUE(!(*it));
     ASSERT_TRUE(!it.nextBasic());
     ASSERT_TRUE(it._preNode);
}

TEST_F(TokenizeSectionTest, testIteratorNextExtend)
{
    TokenizeSection section(_tokenNodeAllocatorPtr);
    prepareSection(section);
    TokenizeSection::Iterator it = section.createIterator();

    for (int i = 0; i < 2; ++i) {
         it.nextExtend();
         ASSERT_TRUE((*it));
         ASSERT_EQ(StringUtil::toString(i), (*it)->getText());
     }
     ASSERT_TRUE(!it.nextExtend());
     ASSERT_TRUE(it._preNode);
}

void TokenizeSectionTest::prepareSection(TokenizeSection &section)
{
    TokenizeSection::Iterator it = section.createIterator();
    int n = 4;
    for (int i = 0; i < n; i++) {
        AnalyzerToken token;
        token.setText(StringUtil::toString(i));
        section.insertBasicToken(token, it);

        for (int j = 0; j < i+2; ++j)
        {
            token.setText(StringUtil::toString(j));
            section.insertExtendToken(token,it);
        }
    }
}

TEST_F(TokenizeSectionTest, testAllocator)
{
    TokenNodeAllocatorPool::reset();
    const uint32_t THREAD_COUNT = 100;
    ThreadPtr threads[THREAD_COUNT];

    for (uint32_t i = 0; i < THREAD_COUNT; i++) {
        threads[i] = Thread::createThread(tr1::bind(&(TokenizeSectionTest::allocate)));
    }
    for (uint32_t i = 0; i < THREAD_COUNT; i++) {
        threads[i]->join();
    }
    ASSERT_EQ((size_t)THREAD_COUNT, TokenNodeAllocatorPool::size());
}

void TokenizeSectionTest::allocate() {
    TokenNodeAllocatorPtr allocator = TokenNodeAllocatorPool::getAllocator();
    ASSERT_TRUE(allocator.get() != NULL);
}

IE_NAMESPACE_END(document);
