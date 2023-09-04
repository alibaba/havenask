#include "indexlib/index/normal/trie/test/double_array_trie_unittest.h"

#include <fstream>
#include <iostream>

#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::util;
using namespace indexlib::file_system;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, DoubleArrayTrieTest);

DoubleArrayTrieTest::DoubleArrayTrieTest() {}

DoubleArrayTrieTest::~DoubleArrayTrieTest() {}

void DoubleArrayTrieTest::CaseSetUp() {}

void DoubleArrayTrieTest::CaseTearDown() {}

void DoubleArrayTrieTest::TestSimpleProcess()
{
    KVPairVector::allocator_type allocator(&mPool);
    KVPairVector sortedVector(allocator);
    string inputFile = PathUtil::JoinPath(GET_PRIVATE_TEST_DATA_PATH(), "trie_key");
    ifstream in(inputFile.c_str());
    int32_t id = 0;
    string key;
    while (getline(in, key)) {
        sortedVector.push_back(make_pair(autil::MakeCString(key, &mPool), id));
        ++id;
    }
    in.close();

    DoubleArrayTrie trie(&mPool);
    ASSERT_TRUE(trie.Build(sortedVector));
    const StringView& data = trie.Data();
    FileWriterPtr fileWriter = GET_PARTITION_DIRECTORY()->CreateFileWriter("file");
    fileWriter->Write(data.data(), data.size()).GetOrThrow();
    ASSERT_EQ(FSEC_OK, fileWriter->Close());
}
}} // namespace indexlib::index
