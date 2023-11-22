#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index/normal/trie/trie_index_merger.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class TrieIndexMergerTest : public INDEXLIB_TESTBASE
{
public:
    TrieIndexMergerTest();
    ~TrieIndexMergerTest();

    DECLARE_CLASS_NAME(TrieIndexMergerTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestEstimateMemoryUse();
    void TestSimpleProcess();

private:
    void InnerTestMerge(const std::string& docStr, const std::string& mergeStr, const std::string& searchPks,
                        const std::string& expectDocIds);

private:
    std::string mRootDir;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(TrieIndexMergerTest, TestEstimateMemoryUse);
INDEXLIB_UNIT_TEST_CASE(TrieIndexMergerTest, TestSimpleProcess);
}} // namespace indexlib::index
