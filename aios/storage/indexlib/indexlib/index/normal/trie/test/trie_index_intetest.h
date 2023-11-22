#pragma once

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index/normal/trie/trie_index_reader.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class TrieIndexInteTest : public INDEXLIB_TESTBASE
{
private:
    typedef TrieIndexReader::KVPair KVPair;
    typedef TrieIndexReader::KVPairVector KVPairVector;

public:
    TrieIndexInteTest();
    ~TrieIndexInteTest();

    DECLARE_CLASS_NAME(TrieIndexInteTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestLookup();
    void TestPrefixSearch();
    void TestPrefixSearchOptimize();
    void TestPrefixSearchMultiSegment();
    void TestInfixSearch();
    void TestInfixSearchOptimize();
    void TestInfixSearchMultiSegment();
    void TestDuplicateAdd();
    void TestDelete();
    void TestDeleteMultiSegment();
    void TestMerge();
    void TestRealtimeBuild();
    void TestMergeWithTestSplit();
    void TestMergeWithTestSplitWithEmpty();

private:
    bool CheckResult(TrieIndexReader::Iterator* iter, const std::string& expectStr);
    bool CheckPrefixSearch(const TrieIndexReader& reader, const std::string& key, const std::string& expectStr,
                           int32_t maxMatches = -1);
    bool CheckInfixSearch(const TrieIndexReader& reader, const std::string& key, const std::string& expectStr,
                          int32_t maxMatches = -1);

private:
    config::IndexPartitionOptions mOptions;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(TrieIndexInteTest, TestLookup);
INDEXLIB_UNIT_TEST_CASE(TrieIndexInteTest, TestPrefixSearch);
INDEXLIB_UNIT_TEST_CASE(TrieIndexInteTest, TestPrefixSearchOptimize);
INDEXLIB_UNIT_TEST_CASE(TrieIndexInteTest, TestPrefixSearchMultiSegment);
INDEXLIB_UNIT_TEST_CASE(TrieIndexInteTest, TestInfixSearch);
INDEXLIB_UNIT_TEST_CASE(TrieIndexInteTest, TestInfixSearchOptimize);
INDEXLIB_UNIT_TEST_CASE(TrieIndexInteTest, TestInfixSearchMultiSegment);
INDEXLIB_UNIT_TEST_CASE(TrieIndexInteTest, TestDuplicateAdd);
INDEXLIB_UNIT_TEST_CASE(TrieIndexInteTest, TestDelete);
INDEXLIB_UNIT_TEST_CASE(TrieIndexInteTest, TestDeleteMultiSegment);
INDEXLIB_UNIT_TEST_CASE(TrieIndexInteTest, TestMerge);
INDEXLIB_UNIT_TEST_CASE(TrieIndexInteTest, TestRealtimeBuild);
INDEXLIB_UNIT_TEST_CASE(TrieIndexInteTest, TestMergeWithTestSplit);
INDEXLIB_UNIT_TEST_CASE(TrieIndexInteTest, TestMergeWithTestSplitWithEmpty);
}} // namespace indexlib::index
