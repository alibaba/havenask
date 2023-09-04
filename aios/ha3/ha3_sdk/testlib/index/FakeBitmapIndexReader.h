#pragma once

#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3_sdk/testlib/index/FakeTextIndexReader.h"
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
} // namespace index
} // namespace indexlib

namespace indexlib {
namespace index {

class FakeBitmapIndexReader : public FakeTopIndexReader {
public:
    FakeBitmapIndexReader(const std::string &datas);
    ~FakeBitmapIndexReader();

private:
    FakeBitmapIndexReader(const FakeBitmapIndexReader &);
    FakeBitmapIndexReader &operator=(const FakeBitmapIndexReader &);

public:
    virtual index::Result<PostingIterator *> Lookup(const Term &term,
                                                    uint32_t statePoolSize = 1000,
                                                    PostingType type = pt_default,
                                                    autil::mem_pool::Pool *sessionPool = NULL);

public:
    FakeTextIndexReader::Map _map;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<FakeBitmapIndexReader> FakeBitmapIndexReaderPtr;

} // namespace index
} // namespace indexlib
