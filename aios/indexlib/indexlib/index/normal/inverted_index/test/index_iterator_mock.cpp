#include "indexlib/index/normal/inverted_index/test/index_iterator_mock.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, IndexIteratorMock);

IndexIteratorMock::IndexIteratorMock(const vector<dictkey_t>& keys) 
{
    mDictKeys.assign(keys.begin(), keys.end());
}

IndexIteratorMock::~IndexIteratorMock() 
{
}

PostingDecoder* IndexIteratorMock::Next(dictkey_t& key)
{
    key = mDictKeys.front();
    mDictKeys.erase(mDictKeys.begin());
    return NULL;
} 

IE_NAMESPACE_END(index);

