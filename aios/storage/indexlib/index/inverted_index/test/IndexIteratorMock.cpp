#include "indexlib/index/inverted_index/test/IndexIteratorMock.h"

namespace indexlib::index {

IndexIteratorMock::IndexIteratorMock(const std::vector<index::DictKeyInfo>& keys)
{
    _dictKeys.assign(keys.begin(), keys.end());
}

IndexIteratorMock::~IndexIteratorMock() {}

PostingDecoder* IndexIteratorMock::Next(index::DictKeyInfo& key)
{
    key = _dictKeys.front();
    _dictKeys.erase(_dictKeys.begin());
    return NULL;
}
} // namespace indexlib::index
