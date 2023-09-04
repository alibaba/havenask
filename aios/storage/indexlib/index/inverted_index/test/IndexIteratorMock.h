#pragma once
#include <memory>

#include "indexlib/index/inverted_index/IndexIterator.h"

namespace indexlib::index {

class IndexIteratorMock : public IndexIterator
{
public:
    IndexIteratorMock(const std::vector<index::DictKeyInfo>& keys);
    ~IndexIteratorMock();

public:
    bool HasNext() const override { return !_dictKeys.empty(); }
    PostingDecoder* Next(index::DictKeyInfo& key) override;

private:
    std::vector<index::DictKeyInfo> _dictKeys;
};

} // namespace indexlib::index
