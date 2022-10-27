#include <iostream>
#include "indexlib/file_system/file_writer.h"
#include "indexlib/index/normal/trie/test/double_array_trie_unittest.h"
#include "indexlib/util/path_util.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, DoubleArrayTrieTest);

DoubleArrayTrieTest::DoubleArrayTrieTest()
{
}

DoubleArrayTrieTest::~DoubleArrayTrieTest()
{
}

void DoubleArrayTrieTest::CaseSetUp()
{
}

void DoubleArrayTrieTest::CaseTearDown()
{
}

void DoubleArrayTrieTest::TestSimpleProcess()
{
    KVPairVector::allocator_type allocator(&mPool);
    KVPairVector sortedVector(allocator);
    string inputFile = PathUtil::JoinPath(TEST_DATA_PATH, "trie_key");
    ifstream in(inputFile.c_str());
    int32_t id = 0;
    string key;
    while (getline(in, key))
    {
        sortedVector.push_back(make_pair(ConstString(key, &mPool), id));
        ++id;
    }
    in.close();
    
    DoubleArrayTrie trie(&mPool);
    ASSERT_TRUE(trie.Build(sortedVector));
    const ConstString& data = trie.Data();
    FileWriterPtr fileWriter = GET_PARTITION_DIRECTORY()->CreateFileWriter("file");
    fileWriter->Write(data.data(), data.size());
    fileWriter->Close();
}

IE_NAMESPACE_END(index);

