#include "indexlib/testlib/fake_bitmap_index_reader.h"
#include "indexlib/testlib/fake_posting_maker.h"
#include "indexlib/testlib/fake_bitmap_posting_iterator.h"

using namespace std;
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(testlib);
IE_LOG_SETUP(testlib, FakeBitmapIndexReader);

FakeBitmapIndexReader::FakeBitmapIndexReader(const string &datas) {
    FakePostingMaker::makeFakePostingsDetail(datas, _map);
}

FakeBitmapIndexReader::~FakeBitmapIndexReader() { 
}

PostingIterator *FakeBitmapIndexReader::Lookup(const common::Term& term,
        uint32_t statePoolSize, PostingType type, autil::mem_pool::Pool *sessionPool)
{
    FakePostingIterator::Map::iterator it = _map.find(term.GetWord());
    if (it == _map.end()) {
        return NULL;
    }
    return POOL_COMPATIBLE_NEW_CLASS(sessionPool, FakeBitmapPostingIterator, it->second, statePoolSize, sessionPool);
}

IE_NAMESPACE_END(testlib);

