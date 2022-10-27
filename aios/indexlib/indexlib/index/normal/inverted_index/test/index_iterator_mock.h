#ifndef __INDEXLIB_INDEX_ITERATOR_MOCK_H
#define __INDEXLIB_INDEX_ITERATOR_MOCK_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/index_iterator.h"

IE_NAMESPACE_BEGIN(index);

class IndexIteratorMock : public IndexIterator
{
public:
    IndexIteratorMock(const std::vector<dictkey_t>& keys);
    ~IndexIteratorMock();
public:
    bool HasNext() const override { return !mDictKeys.empty(); }
    PostingDecoder* Next(dictkey_t& key) override;
private:
    std::vector<dictkey_t> mDictKeys;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexIteratorMock);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INDEX_ITERATOR_MOCK_H
