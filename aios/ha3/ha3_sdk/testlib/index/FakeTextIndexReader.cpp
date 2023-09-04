#include "ha3_sdk/testlib/index/FakeTextIndexReader.h"

#include <cstddef>

#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "ha3_sdk/testlib/index/FakePostingIterator.h"
#include "ha3_sdk/testlib/index/FakePostingMaker.h"
#include "ha3_sdk/testlib/index/FakeSectionAttributeReader.h"
#include "indexlib/index/common/ErrorCode.h"

namespace indexlib {
namespace index {
class PostingIterator;
class SectionAttributeReader;
} // namespace index
} // namespace indexlib

using namespace std;

namespace indexlib {
namespace index {

FakeTextIndexReader::FakeTextIndexReader(const string &mpStr, indexlib::index::ErrorCode seekRet) {
    FakePostingMaker::makeFakePostingsDetail(mpStr, _map);
    _seekRet = seekRet;
}

FakeTextIndexReader::FakeTextIndexReader(const string &mpStr, const PositionMap &posMap) {
    FakePostingMaker::makeFakePostingsDetail(mpStr, _map);
    _posMap = posMap;
    _seekRet = indexlib::index::ErrorCode::OK;
}

index::Result<PostingIterator *> FakeTextIndexReader::Lookup(const Term &term,
                                                             uint32_t statePoolSize,
                                                             PostingType type,
                                                             autil::mem_pool::Pool *sessionPool) {
    Map::iterator it = _map.find(term.GetWord());
    if (it == _map.end()) {
        return NULL;
    }
    FakePostingIterator *fakeIterator = POOL_COMPATIBLE_NEW_CLASS(
        sessionPool, FakePostingIterator, it->second, statePoolSize, sessionPool);
    fakeIterator->setSeekRet(_seekRet);
    IteratorTypeMap::iterator typeIt = _typeMap.find(term.GetWord());
    if (typeIt != _typeMap.end()) {
        fakeIterator->setType(typeIt->second);
    }
    PositionMap::iterator posIt = _posMap.find(term.GetWord());
    if (posIt != _posMap.end()) {
        fakeIterator->setPosition(posIt->second);
    }
    return fakeIterator;
}

const SectionAttributeReader *
FakeTextIndexReader::GetSectionReader(const std::string &indexName) const {
    _ptr.reset(new FakeSectionAttributeReader(_docSectionMap));
    return _ptr.get();
}

} // namespace index
} // namespace indexlib
