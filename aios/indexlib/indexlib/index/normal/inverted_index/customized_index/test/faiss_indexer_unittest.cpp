#include "indexlib/index/normal/inverted_index/customized_index/test/faiss_indexer_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, FaissIndexerTest);

FaissIndexerTest::FaissIndexerTest()
{
}

FaissIndexerTest::~FaissIndexerTest()
{
}

void FaissIndexerTest::CaseSetUp()
{
}

void FaissIndexerTest::CaseTearDown()
{
}

void FaissIndexerTest::TestSimpleProcess()
{
    const auto& dir = GET_PARTITION_DIRECTORY();
    IndexerPtr indexer(new FaissIndexer());
    indexer->Open(dir);
    indexer->Build("hello", 0);
    indexer->Build("world", 1);
    indexer->Dump(dir);
}

IE_NAMESPACE_END(index);

