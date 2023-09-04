#include "indexlib/testlib/fake_bitmap_index_reader.h"

#include "indexlib/testlib/fake_bitmap_posting_iterator.h"
#include "indexlib/testlib/fake_posting_maker.h"

using namespace std;
using namespace indexlib::index;

namespace indexlib { namespace testlib {
IE_LOG_SETUP(testlib, FakeBitmapIndexReader);

FakeBitmapIndexReader::FakeBitmapIndexReader(const string& datas)
{
    FakePostingMaker::makeFakePostingsDetail(datas, _map);
}

FakeBitmapIndexReader::~FakeBitmapIndexReader() {}

index::Result<PostingIterator*> FakeBitmapIndexReader::Lookup(const index::Term& term, uint32_t statePoolSize,
                                                              PostingType type, autil::mem_pool::Pool* sessionPool)
{
    FakePostingIterator::Map::iterator it = _map.find(term.GetWord());
    if (it == _map.end()) {
        return NULL;
    }
    return POOL_COMPATIBLE_NEW_CLASS(sessionPool, FakeBitmapPostingIterator, it->second, statePoolSize, sessionPool);
}
}} // namespace indexlib::testlib
