#include "ha3_sdk/testlib/index/FakeBitmapIndexReader.h"

#include <cstddef>
#include <map>
#include <utility>

#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "ha3_sdk/testlib/index/FakeBitmapPostingIterator.h"
#include "ha3_sdk/testlib/index/FakePostingMaker.h"
#include "ha3_sdk/testlib/index/FakeTextIndexReader.h"

namespace indexlib {
namespace index {
class PostingIterator;
} // namespace index
} // namespace indexlib

namespace indexlib {
namespace index {
AUTIL_LOG_SETUP(ha3, FakeBitmapIndexReader);

using namespace std;

FakeBitmapIndexReader::FakeBitmapIndexReader(const string &datas) {
    FakePostingMaker::makeFakePostingsDetail(datas, _map);
}

FakeBitmapIndexReader::~FakeBitmapIndexReader() {}

index::Result<PostingIterator *> FakeBitmapIndexReader::Lookup(const Term &term,
                                                               uint32_t statePoolSize,
                                                               PostingType type,
                                                               autil::mem_pool::Pool *sessionPool) {
    FakeTextIndexReader::Map::iterator it = _map.find(term.GetWord());
    if (it == _map.end()) {
        return NULL;
    }
    return POOL_COMPATIBLE_NEW_CLASS(
        sessionPool, FakeBitmapPostingIterator, it->second, statePoolSize, sessionPool);
}

} // namespace index
} // namespace indexlib
