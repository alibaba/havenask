#include "indexlib/index/normal/inverted_index/test/term_meta_unittest.h"
#include "indexlib/index_define.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, TermMetaTest);

TermMetaTest::TermMetaTest()
{
}

TermMetaTest::~TermMetaTest()
{
}

void TermMetaTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
}

void TermMetaTest::CaseTearDown()
{
}

void TermMetaTest::TestEqual()
{
    TermMeta oldTermMeta(1, 2, 3);
    
    TermMeta newTermMeta;
    newTermMeta = oldTermMeta;
    
    ASSERT_EQ(oldTermMeta.mDocFreq, newTermMeta.mDocFreq);
    ASSERT_EQ(oldTermMeta.mTotalTermFreq, newTermMeta.mTotalTermFreq);
    ASSERT_EQ(oldTermMeta.mPayload, newTermMeta.mPayload);
    ASSERT_TRUE(oldTermMeta == newTermMeta);
}

IE_NAMESPACE_END(index);
