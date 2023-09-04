#include "indexlib/index/inverted_index/format/TermMeta.h"

#include "unittest/unittest.h"

namespace indexlib::index {

class TermMetaTest : public TESTBASE
{
};

TEST_F(TermMetaTest, testEqual)
{
    TermMeta oldTermMeta(1, 2, 3);

    TermMeta newTermMeta;
    newTermMeta = oldTermMeta;

    ASSERT_EQ(oldTermMeta._docFreq, newTermMeta._docFreq);
    ASSERT_EQ(oldTermMeta._totalTermFreq, newTermMeta._totalTermFreq);
    ASSERT_EQ(oldTermMeta._payload, newTermMeta._payload);
    ASSERT_TRUE(oldTermMeta == newTermMeta);
}

} // namespace indexlib::index
