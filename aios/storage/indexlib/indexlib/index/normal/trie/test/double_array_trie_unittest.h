#ifndef __INDEXLIB_DOUBLEARRAYTRIETEST_H
#define __INDEXLIB_DOUBLEARRAYTRIETEST_H

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/trie/double_array_trie.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class DoubleArrayTrieTest : public INDEXLIB_TESTBASE
{
private:
    typedef DoubleArrayTrie::KVPairVector KVPairVector;

public:
    DoubleArrayTrieTest();
    ~DoubleArrayTrieTest();

    DECLARE_CLASS_NAME(DoubleArrayTrieTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    autil::mem_pool::Pool mPool;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DoubleArrayTrieTest, TestSimpleProcess);
}} // namespace indexlib::index

#endif //__INDEXLIB_DOUBLEARRAYTRIETEST_H
