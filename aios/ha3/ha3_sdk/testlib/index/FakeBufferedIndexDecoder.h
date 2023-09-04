#pragma once

#include <memory>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "indexlib/index/inverted_index/BufferedIndexDecoder.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"

namespace indexlibv2 {
namespace index {
class PostingFormatOption;
} // namespace index
} // namespace indexlibv2

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

namespace indexlib {
namespace index {

class FakeBufferedIndexDecoder : public indexlib::index::BufferedIndexDecoder {
public:
    FakeBufferedIndexDecoder(const indexlib::index::PostingFormatOption &formatOption,
                             autil::mem_pool::Pool *pool);

    ~FakeBufferedIndexDecoder();

private:
    FakeBufferedIndexDecoder(const FakeBufferedIndexDecoder &);
    FakeBufferedIndexDecoder &operator=(const FakeBufferedIndexDecoder &);

public:
    void Init(const std::string &docIdStr, const std::string &fieldStr);

public:
    virtual bool DecodeDocBuffer(docid_t startDocId,
                                 docid_t *docBuffer,
                                 docid_t &firstDocId,
                                 docid_t &lastDocId,
                                 ttf_t &currentTTF) override;

    virtual bool DecodeCurrentTFBuffer(tf_t *tfBuffer) override;
    virtual void DecodeCurrentDocPayloadBuffer(docpayload_t *docPayloadBuffer) override;
    virtual void DecodeCurrentFieldMapBuffer(fieldmap_t *fieldBitmapBuffer) override;
    uint32_t GetSeekedDocCount() const override;

private:
    std::vector<docid_t> _docIds;
    std::vector<fieldmap_t> _fieldMaps;
    uint32_t _blockCursor;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<FakeBufferedIndexDecoder> FakeBufferedIndexDecoderPtr;

} // namespace index
} // namespace indexlib
