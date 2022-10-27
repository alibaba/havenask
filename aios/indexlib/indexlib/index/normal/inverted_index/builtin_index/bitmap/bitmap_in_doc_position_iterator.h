#ifndef __INDEXLIB_BITMAP_TEXT_IN_DOC_POSITION_ITERATOR_H
#define __INDEXLIB_BITMAP_TEXT_IN_DOC_POSITION_ITERATOR_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>
#include "indexlib/index/normal/inverted_index/accessor/in_doc_position_iterator.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_in_doc_position_state.h"

IE_NAMESPACE_BEGIN(index);

class BitmapInDocPositionIterator : public InDocPositionIterator
{
public:
    BitmapInDocPositionIterator() {}
    ~BitmapInDocPositionIterator() {}

public:
    common::ErrorCode SeekPositionWithErrorCode(pos_t pos, pos_t& nextpos)override
    {
        nextpos = INVALID_POSITION;
        return common::ErrorCode::OK;
    }
    pospayload_t GetPosPayload() override { return 0;}
    sectionid_t GetSectionId() override { return 0;}
    section_len_t GetSectionLength() override { return 0;}
    section_weight_t GetSectionWeight() override { return 0; }
    fieldid_t GetFieldId() override  { return INVALID_FIELDID; }
    int32_t GetFieldPosition() override  { return 0; }
    bool HasPosPayload() const override { return false; }

private:
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<BitmapInDocPositionIterator> BitmapInDocPositionIteratorPtr;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_BITMAP_TEXT_IN_DOC_POSITION_ITERATOR_H
