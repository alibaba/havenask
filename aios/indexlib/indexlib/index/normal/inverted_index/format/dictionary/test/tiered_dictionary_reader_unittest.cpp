#include "indexlib/index/normal/inverted_index/format/dictionary/test/tiered_dictionary_reader_unittest.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, TieredDictionaryReaderTest);

TieredDictionaryReaderTest::TieredDictionaryReaderTest()
{
}

TieredDictionaryReaderTest::~TieredDictionaryReaderTest()
{
}

void TieredDictionaryReaderTest::CaseSetUp()
{
}

void TieredDictionaryReaderTest::CaseTearDown()
{
}

void TieredDictionaryReaderTest::TestIntegrateRead()
{
    string fileName = "integrate";
    InnerTestRead<uint8_t>(128, fileName, FSFT_MMAP);
    InnerTestRead<uint16_t>(1000, fileName, FSFT_MMAP);
    InnerTestRead<uint32_t>(2000, fileName, FSFT_MMAP);
    InnerTestRead<uint64_t>(4000, fileName, FSFT_MMAP);
}

void TieredDictionaryReaderTest::TestBlockRead()
{
    string fileName = "block";
    InnerTestRead<uint8_t>(128, fileName, FSFT_BLOCK);
    InnerTestRead<uint16_t>(1000, fileName, FSFT_BLOCK);
    InnerTestRead<uint32_t>(2000, fileName, FSFT_BLOCK);
    InnerTestRead<uint64_t>(4000, fileName, FSFT_BLOCK);
}

IE_NAMESPACE_END(index);
