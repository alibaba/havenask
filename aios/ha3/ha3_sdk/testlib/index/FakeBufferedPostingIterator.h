#pragma once

#include <memory>
#include <stdint.h>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "ha3_sdk/testlib/index/FakeBufferedIndexDecoder.h"
#include "indexlib/index/inverted_index/BufferedPostingIterator.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"
#include "indexlib/misc/common.h"
#include "indexlib/util/ObjectPool.h"

namespace indexlibv2 {
namespace index {
class PostingFormatOption;
} // namespace index
} // namespace indexlibv2

namespace indexlib {
namespace index {

class FakeBufferedPostingIterator : public indexlib::index::BufferedPostingIterator {
public:
    FakeBufferedPostingIterator(const indexlib::index::PostingFormatOption &postingFormatOption,
                                autil::mem_pool::Pool *sessionPool);
    ~FakeBufferedPostingIterator() {
        if (_termMeta) {
            delete _termMeta;
        }
    }

private:
    FakeBufferedPostingIterator(const FakeBufferedPostingIterator &);
    FakeBufferedPostingIterator &operator=(const FakeBufferedPostingIterator &);

public:
    void init(const std::string &docIdStr, const std::string &fieldMapStr, uint32_t statePoolSize) {
        FakeBufferedIndexDecoder *decoder = POOL_COMPATIBLE_NEW_CLASS(
            _sessionPool, FakeBufferedIndexDecoder, _postingFormatOption, _sessionPool);
        decoder->Init(docIdStr, fieldMapStr);
        _decoder = decoder;
        _statePool.Init(statePoolSize);
        _termMeta = new indexlib::index::TermMeta();
    }
    /* override */ indexlib::index::TermMeta *GetTermMeta() const {
        return _termMeta;
    }

private:
    indexlib::index::TermMeta *_termMeta;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<FakeBufferedPostingIterator> FakeBufferedPostingIteratorPtr;

} // namespace index
} // namespace indexlib
