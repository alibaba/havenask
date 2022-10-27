#include "indexlib/index/normal/trie/test/trie_index_intetest.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "autil/StringUtil.h"
#include "indexlib/util/simple_pool.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, TrieIndexInteTest);

TrieIndexInteTest::TrieIndexInteTest()
{
}

TrieIndexInteTest::~TrieIndexInteTest()
{
}

void TrieIndexInteTest::CaseSetUp()
{
    mOptions.GetOfflineConfig().buildConfig.enablePackageFile = false;
    std::string mergeConfigStr = "{\"class_name\":\"default\",\"parameters\":{\"split_num\":\"2\"}}";
    autil::legacy::FromJsonString(mOptions.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);
}

void TrieIndexInteTest::CaseTearDown()
{
}

void TrieIndexInteTest::TestLookup()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEST_DATA_PATH(), "string", SFP_TRIE);
    provider.Build("123,12,efg,45,4,(456", SFP_OFFLINE);
    TrieIndexReader reader;
    reader.Open(provider.GetIndexConfig(), provider.GetPartitionData());
    ASSERT_EQ((docid_t)0, reader.Lookup("123"));
    ASSERT_EQ((docid_t)1, reader.Lookup("12"));
    ASSERT_EQ((docid_t)2, reader.Lookup("efg"));
    ASSERT_EQ((docid_t)3, reader.Lookup("45"));
    ASSERT_EQ((docid_t)4, reader.Lookup("4"));
    ASSERT_EQ((docid_t)5, reader.Lookup("(456"));
    ASSERT_EQ((docid_t)INVALID_DOCID, reader.Lookup("abc"));
}

bool TrieIndexInteTest::CheckResult(TrieIndexReader::Iterator* iter,
                                    const string& expectStr)
{
    vector<vector<string> > expectResult;
    StringUtil::fromString(expectStr, expectResult, ":", ",");
    if (expectResult.size() != iter->Size())
    {
        EXPECT_EQ(expectResult.size(), iter->Size());
        return false;
    }
    for (size_t i = 0; i < expectResult.size(); ++i)
    {
        if (!iter->Valid())
        {
            return false;
        }
        const vector<string>& expect = expectResult[i];
        assert(expect.size() == 2);
        const string& expectKey = expect[0];
        string actualKey(iter->GetKey().data(), iter->GetKey().size());
        if (expectKey != actualKey)
        {
            EXPECT_EQ(expectKey, actualKey) << "Key";
            return false;
        }
        docid_t expectDocId = StringUtil::fromString<docid_t>(expect[1]);
        if (expectDocId != iter->GetDocid())
        {
            EXPECT_EQ(expectDocId, iter->GetDocid()) << "DocId";
            return false;
        }
        iter->Next();
    }
    return true;
}

bool TrieIndexInteTest::CheckPrefixSearch(const TrieIndexReader& reader,
        const string& key, const string& expectStr, int32_t maxMatches)
{
    Pool pool;
    TrieIndexReader::Iterator* iter = NULL;
    if (maxMatches >= 0)
    {
        iter = reader.PrefixSearch(ConstString(key), &pool, maxMatches);
        if (iter->Size() > (size_t)maxMatches)
        {
            EXPECT_TRUE(iter->Size() <= (size_t)maxMatches);
            return false;
        }
    }
    else
    {
        iter = reader.PrefixSearch(ConstString(key), &pool);
    }
    return CheckResult(iter, expectStr);
}

bool TrieIndexInteTest::CheckInfixSearch(const TrieIndexReader& reader,
        const string& key, const string& expectStr, int32_t maxMatches)
{
    Pool pool;
    TrieIndexReader::Iterator* iter = NULL;
    if (maxMatches >= 0)
    {
        iter = reader.InfixSearch(ConstString(key), &pool, maxMatches);
        if (iter->Size() > (size_t)maxMatches)
        {
            EXPECT_TRUE(iter->Size() <= (size_t)maxMatches);
            return false;
        }
    }
    else
    {
        iter = reader.InfixSearch(ConstString(key), &pool);
    }
    return CheckResult(iter, expectStr);
}

void TrieIndexInteTest::TestPrefixSearch()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEST_DATA_PATH(), "string", SFP_TRIE);
    provider.Build("4567,dup,efg,45,4,456,dup,5", SFP_OFFLINE);
    TrieIndexReader reader;
    reader.Open(provider.GetIndexConfig(), provider.GetPartitionData());
    ASSERT_FALSE(reader.mOptimizeSearch);
    ASSERT_TRUE(CheckPrefixSearch(reader, "4", "4:4"));
    ASSERT_TRUE(CheckPrefixSearch(reader, "5", "5:7"));
    ASSERT_TRUE(CheckPrefixSearch(reader, "45", "4:4,45:3"));
    ASSERT_TRUE(CheckPrefixSearch(reader, "456", "4:4,45:3,456:5"));
    ASSERT_TRUE(CheckPrefixSearch(reader, "456789", "4:4,45:3,456:5,4567:0"));
    ASSERT_TRUE(CheckPrefixSearch(reader, "dup", "dup:6"));
    ASSERT_TRUE(CheckPrefixSearch(reader, "non", ""));
    ASSERT_TRUE(CheckPrefixSearch(reader, "456", "", 0));
    ASSERT_TRUE(CheckPrefixSearch(reader, "456789", "4:4", 1));
}

void TrieIndexInteTest::TestPrefixSearchOptimize()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEST_DATA_PATH(), "string", SFP_TRIE);
    provider.Build("4567,abcd,efg,45,4,456,56,5,中国人民,中国人民银行,中国,人民", SFP_OFFLINE);
    TrieIndexReader reader;
    reader.Open(provider.GetIndexConfig(), provider.GetPartitionData());
    ASSERT_TRUE(reader.mOptimizeSearch);
    ASSERT_TRUE(CheckPrefixSearch(reader, "4", "4:4"));
    ASSERT_TRUE(CheckPrefixSearch(reader, "5", "5:7"));
    ASSERT_TRUE(CheckPrefixSearch(reader, "45", "4:4,45:3"));
    ASSERT_TRUE(CheckPrefixSearch(reader, "456", "4:4,45:3,456:5"));
    ASSERT_TRUE(CheckPrefixSearch(reader, "456789", "4:4,45:3,456:5,4567:0"));
    ASSERT_TRUE(CheckPrefixSearch(reader, "non", ""));
    ASSERT_TRUE(CheckPrefixSearch(reader, "456", "", 0));
    ASSERT_TRUE(CheckPrefixSearch(reader, "456789", "4:4", 1));
    ASSERT_TRUE(CheckPrefixSearch(reader, "中国人民银行", "中国:10,中国人民:8,中国人民银行:9"));
    ASSERT_TRUE(CheckPrefixSearch(reader, "中", ""));
}

void TrieIndexInteTest::TestPrefixSearchMultiSegment()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEST_DATA_PATH(), "string", SFP_TRIE);
    provider.Build("123,12,efg,45#4,456#123,12,5", SFP_OFFLINE);
    TrieIndexReader reader;
    reader.Open(provider.GetIndexConfig(), provider.GetPartitionData());
    ASSERT_FALSE(reader.mOptimizeSearch);
    ASSERT_TRUE(CheckPrefixSearch(reader, "123", "12:7,123:6"));
    ASSERT_TRUE(CheckPrefixSearch(reader, "4", "4:4"));
    ASSERT_TRUE(CheckPrefixSearch(reader, "abc", ""));
    ASSERT_TRUE(CheckPrefixSearch(reader, "456", "45:3,4:4,456:5"));
    ASSERT_TRUE(CheckPrefixSearch(reader, "5", "5:8"));
    ASSERT_TRUE(CheckPrefixSearch(reader, "123", "", 0));
    ASSERT_TRUE(CheckPrefixSearch(reader, "456", "45:3,4:4", 2));
}

void TrieIndexInteTest::TestDelete()
{
    SingleFieldPartitionDataProvider provider(mOptions);
    provider.Init(GET_TEST_DATA_PATH(), "string", SFP_TRIE);
    provider.Build("a,ab,abc,abcd,delete:abc", SFP_OFFLINE);
    TrieIndexReader reader;
    reader.Open(provider.GetIndexConfig(), provider.GetPartitionData());
    ASSERT_EQ(INVALID_DOCID, reader.Lookup("abc"));
    ASSERT_TRUE(CheckPrefixSearch(reader, "abcd", "a:0,ab:1,abcd:3"));
    ASSERT_TRUE(CheckPrefixSearch(reader, "abcd", "a:0,ab:1,abcd:3", 3));
}

void TrieIndexInteTest::TestDeleteMultiSegment()
{
    SingleFieldPartitionDataProvider provider(mOptions);
    provider.Init(GET_TEST_DATA_PATH(), "string", SFP_TRIE);
    provider.Build("a,ab,abc,abcd#delete:abc", SFP_OFFLINE);
    TrieIndexReader reader;
    reader.Open(provider.GetIndexConfig(), provider.GetPartitionData());
    ASSERT_EQ(INVALID_DOCID, reader.Lookup("abc"));
    ASSERT_TRUE(CheckPrefixSearch(reader, "abcd", "a:0,ab:1,abcd:3"));
}

void TrieIndexInteTest::TestDuplicateAdd()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEST_DATA_PATH(), "string", SFP_TRIE);
    provider.Build("123,123,efg,45#efg,5", SFP_OFFLINE);
    TrieIndexReader reader;
    reader.Open(provider.GetIndexConfig(), provider.GetPartitionData());
    ASSERT_EQ((docid_t)1, reader.Lookup("123"));
    ASSERT_EQ((docid_t)4, reader.Lookup("efg"));
    ASSERT_EQ((docid_t)3, reader.Lookup("45"));
    ASSERT_EQ((docid_t)5, reader.Lookup("5"));
    DeletionMapReaderPtr deleteReader = 
        provider.GetPartitionData()->GetDeletionMapReader();
    ASSERT_TRUE(deleteReader->IsDeleted(0));
    ASSERT_TRUE(deleteReader->IsDeleted(2));
}

void TrieIndexInteTest::TestMerge()
{
    SingleFieldPartitionDataProvider provider(mOptions);
    provider.Init(GET_TEST_DATA_PATH(), "string", SFP_TRIE);
    provider.Build("123,12#hello,45#4,456#123,12,5", SFP_OFFLINE);
    provider.Merge("0,1,2,3");
    TrieIndexReader reader;
    reader.Open(provider.GetIndexConfig(), provider.GetPartitionData());
    ASSERT_TRUE(CheckPrefixSearch(reader, "123", "12:5,123:4"));
}

void TrieIndexInteTest::TestMergeWithTestSplit()
{
    std::string mergeConfigStr = "{\"class_name\":\"test\",\"parameters\":{\"segment_count\":\"2\"}}";
    autil::legacy::FromJsonString(mOptions.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);
    SingleFieldPartitionDataProvider provider(mOptions);
    provider.Init(GET_TEST_DATA_PATH(), "string", SFP_TRIE);
    provider.Build("123,12#hello,45#4,456#123,12,5", SFP_OFFLINE);
    provider.Merge("0,1,2,3");
    TrieIndexReader reader;
    reader.Open(provider.GetIndexConfig(), provider.GetPartitionData());
    ASSERT_TRUE(CheckPrefixSearch(reader, "123", "123:2,12:6"));
}

void TrieIndexInteTest::TestMergeWithTestSplitWithEmpty()
{
    std::string mergeConfigStr = "{\"class_name\":\"test\",\"parameters\":{\"segment_count\":\"4\"}}";
    autil::legacy::FromJsonString(mOptions.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);
    SingleFieldPartitionDataProvider provider(mOptions);
    provider.Init(GET_TEST_DATA_PATH(), "string", SFP_TRIE);
    provider.Build("123,12#hello,45#4,456#123,12,5", SFP_OFFLINE);
    provider.Merge("0,1,2,3");
    TrieIndexReader reader;
    reader.Open(provider.GetIndexConfig(), provider.GetPartitionData());
    ASSERT_TRUE(CheckPrefixSearch(reader, "123", "123:2,12:5"));
}


void TrieIndexInteTest::TestRealtimeBuild()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEST_DATA_PATH(), "string", SFP_TRIE);
    provider.Build("12", SFP_OFFLINE);
    //realtime build not core
    provider.Build("14", SFP_REALTIME);
    TrieIndexReader reader;
    reader.Open(provider.GetIndexConfig(), provider.GetPartitionData());
    //read offline data
    ASSERT_EQ((docid_t)0, reader.Lookup("12"));
    //read online data
    ASSERT_EQ(INVALID_DOCID, reader.Lookup("14"));
}

void TrieIndexInteTest::TestInfixSearch()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEST_DATA_PATH(), "string", SFP_TRIE);
    provider.Build("4567,dup,efg,45,4,456,dup,5", SFP_OFFLINE);
    TrieIndexReader reader;
    reader.Open(provider.GetIndexConfig(), provider.GetPartitionData());
    ASSERT_FALSE(reader.mOptimizeSearch);
    ASSERT_TRUE(CheckInfixSearch(reader, "4", "4:4"));
    ASSERT_TRUE(CheckInfixSearch(reader, "5", "5:7"));
    ASSERT_TRUE(CheckInfixSearch(reader, "45", "4:4,45:3,5:7"));
    ASSERT_TRUE(CheckInfixSearch(reader, "456", "4:4,45:3,456:5,5:7"));
    ASSERT_TRUE(CheckInfixSearch(reader, "4545", "4:4,45:3,5:7,4:4,45:3,5:7"));
    ASSERT_TRUE(CheckInfixSearch(reader, "dup", "dup:6"));
    ASSERT_TRUE(CheckInfixSearch(reader, "non", ""));
    ASSERT_TRUE(CheckInfixSearch(reader, "45", "", 0));
    ASSERT_TRUE(CheckInfixSearch(reader, "456", "4:4,45:3", 2));
}

void TrieIndexInteTest::TestInfixSearchOptimize()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEST_DATA_PATH(), "string", SFP_TRIE);
    provider.Build("4567,abcd,efg,45,4,456,65,5,中国人,中国,国人", SFP_OFFLINE);
    TrieIndexReader reader;
    reader.Open(provider.GetIndexConfig(), provider.GetPartitionData());
    ASSERT_TRUE(reader.mOptimizeSearch);
    ASSERT_TRUE(CheckInfixSearch(reader, "4", "4:4"));
    ASSERT_TRUE(CheckInfixSearch(reader, "5", "5:7"));
    ASSERT_TRUE(CheckInfixSearch(reader, "45", "4:4,45:3,5:7"));
    ASSERT_TRUE(CheckInfixSearch(reader, "456", "4:4,45:3,456:5,5:7"));
    ASSERT_TRUE(CheckInfixSearch(reader, "4545", "4:4,45:3,5:7,4:4,45:3,5:7"));
    ASSERT_TRUE(CheckInfixSearch(reader, "non", ""));
    ASSERT_TRUE(CheckInfixSearch(reader, "45", "", 0));
    ASSERT_TRUE(CheckInfixSearch(reader, "456", "4:4,45:3", 2));
    ASSERT_TRUE(CheckInfixSearch(reader, "中国人", "中国:9,中国人:8,国人:10"));
    ASSERT_TRUE(CheckInfixSearch(reader, "中", ""));
}

void TrieIndexInteTest::TestInfixSearchMultiSegment()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEST_DATA_PATH(), "string", SFP_TRIE);
    provider.Build("123,12,efg,45#4,456#123,12,5", SFP_OFFLINE);
    TrieIndexReader reader;
    reader.Open(provider.GetIndexConfig(), provider.GetPartitionData());
    ASSERT_FALSE(reader.mOptimizeSearch);
    ASSERT_TRUE(CheckInfixSearch(reader, "123", "12:7,123:6"));
    ASSERT_TRUE(CheckInfixSearch(reader, "4", "4:4"));
    ASSERT_TRUE(CheckInfixSearch(reader, "5", "5:8"));
    ASSERT_TRUE(CheckInfixSearch(reader, "456", "45:3,4:4,456:5,5:8"));
    ASSERT_TRUE(CheckInfixSearch(reader, "non", ""));
    ASSERT_TRUE(CheckInfixSearch(reader, "123", "", 0));
    ASSERT_TRUE(CheckInfixSearch(reader, "456", "45:3,4:4", 2));
}

IE_NAMESPACE_END(index);

