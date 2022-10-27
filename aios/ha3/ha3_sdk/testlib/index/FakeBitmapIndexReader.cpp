#include <ha3_sdk/testlib/index/FakeBitmapIndexReader.h>
#include <ha3_sdk/testlib/index/FakeTextIndexReader.h>
#include <ha3_sdk/testlib/index/FakePostingIterator.h>
#include <ha3_sdk/testlib/index/FakePostingMaker.h>
#include <ha3_sdk/testlib/index/FakeBitmapPostingIterator.h>

IE_NAMESPACE_BEGIN(index);
HA3_LOG_SETUP(index, FakeBitmapIndexReader);

using namespace std;
IE_NAMESPACE_USE(common);

FakeBitmapIndexReader::FakeBitmapIndexReader(const string &datas) {
    FakePostingMaker::makeFakePostingsDetail(datas, _map);
}

FakeBitmapIndexReader::~FakeBitmapIndexReader() { 
}

PostingIterator *FakeBitmapIndexReader::Lookup(const common::Term& term,
        uint32_t statePoolSize, PostingType type, autil::mem_pool::Pool *sessionPool)
{
    FakeTextIndexReader::Map::iterator it = _map.find(term.GetWord());
    if (it == _map.end()) {
        return NULL;
    }
    return POOL_COMPATIBLE_NEW_CLASS(sessionPool, FakeBitmapPostingIterator, it->second, statePoolSize, sessionPool);
}


IE_NAMESPACE_END(index);

