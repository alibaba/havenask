#pragma once

#include <stdint.h>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/inverted_index/InDocPositionIterator.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"

namespace indexlib {
namespace index {

class FakeInDocPositionIterator : public InDocPositionIterator {
public:
    // FakeInDocPositionIterator(const std::vector<pos_t> &occArray,
    //                          const std::vector<int32_t> &fieldBitArray);
    FakeInDocPositionIterator(const std::vector<pos_t> &occArray,
                              const std::vector<int32_t> &fieldBitArray,
                              const std::vector<sectionid_t> &fieldBitArray2);
    virtual ~FakeInDocPositionIterator();

public:
    pos_t SeekPosition(pos_t pos);
    indexlib::index::ErrorCode SeekPositionWithErrorCode(pos_t pos, pos_t &nextpos) {
        nextpos = SeekPosition(pos);
        return indexlib::index::ErrorCode::OK;
    }

    pospayload_t GetPosPayload();

    virtual sectionid_t GetSectionId();
    virtual section_len_t GetSectionLength();
    virtual section_weight_t GetSectionWeight();
    virtual fieldid_t GetFieldId();
    virtual int32_t GetFieldPosition();
    virtual bool HasPosPayload() const;

private:
    std::vector<pos_t> _occArray;
    std::vector<int32_t> _fieldBitArray;
    std::vector<sectionid_t> _sectionIdArray;
    int32_t _offset;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace index
} // namespace indexlib
