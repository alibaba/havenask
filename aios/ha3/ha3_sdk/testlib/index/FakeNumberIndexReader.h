#pragma once

#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3_sdk/testlib/index/FakeTopIndexReader.h"
#include "indexlib/index/common/Term.h"
#include "indexlib/indexlib.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace indexlib {
namespace index {
class PostingIterator;
class SectionAttributeReader;
} // namespace index
} // namespace indexlib

namespace indexlib {
namespace index {

class FakeNumberIndexReader : public FakeTopIndexReader {
public:
    typedef std::vector<int32_t> NumberValues;
    typedef std::shared_ptr<std::vector<int32_t>> NumberValuesPtr;

    FakeNumberIndexReader()
        : _numberValuePtr(new NumberValues) {}
    virtual ~FakeNumberIndexReader() {}

    // std::shared_ptr<PostingIterator> Lookup(const indexlib::index::Term &term, bool hasPosition);
    index::Result<PostingIterator *> Lookup(const Term &term,
                                            uint32_t statePoolSize = 1000,
                                            PostingType type = pt_default,
                                            autil::mem_pool::Pool *pool = NULL);

    void setNumberValuesFromString(const std::string &numberValues);
    NumberValuesPtr getNumberValues();
    size_t size();
    void clear();
    const SectionAttributeReader *GetSectionReader(const std::string &indexName) const;

private:
    void parseOneLine(const std::string &aLine);

private:
    NumberValuesPtr _numberValuePtr;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace index
} // namespace indexlib
