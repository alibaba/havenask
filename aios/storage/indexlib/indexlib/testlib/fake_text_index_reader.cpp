#include "indexlib/testlib/fake_text_index_reader.h"

#include "indexlib/testlib/fake_posting_maker.h"

using namespace std;
using namespace indexlib::index;

namespace indexlib { namespace testlib {
IE_LOG_SETUP(testlib, FakeTextIndexReader);

FakeTextIndexReader::FakeTextIndexReader(const string& mpStr) { FakePostingMaker::makeFakePostingsDetail(mpStr, _map); }

FakeTextIndexReader::FakeTextIndexReader(const string& mpStr, const FakePostingIterator::PositionMap& posMap)
{
    FakePostingMaker::makeFakePostingsDetail(mpStr, _map);
    _posMap = posMap;
}

index::Result<PostingIterator*> FakeTextIndexReader::Lookup(const index::Term& term, uint32_t statePoolSize,
                                                            PostingType type, autil::mem_pool::Pool* sessionPool)
{
    FakePostingIterator::Map::iterator it = _map.find(term.GetWord());
    if (it == _map.end()) {
        return NULL;
    }
    FakePostingIterator* fakeIterator =
        POOL_COMPATIBLE_NEW_CLASS(sessionPool, FakePostingIterator, it->second, statePoolSize, sessionPool);
    FakePostingIterator::IteratorTypeMap::iterator typeIt = _typeMap.find(term.GetWord());
    if (typeIt != _typeMap.end()) {
        fakeIterator->setType(typeIt->second);
    }
    FakePostingIterator::PositionMap::iterator posIt = _posMap.find(term.GetWord());
    if (posIt != _posMap.end()) {
        fakeIterator->setPosition(posIt->second);
    }
    return fakeIterator;
}

const SectionAttributeReader* FakeTextIndexReader::GetSectionReader(const std::string& indexName) const
{
    _ptr.reset(new FakeSectionAttributeReader(_docSectionMap));
    return _ptr.get();
}
}} // namespace indexlib::testlib
