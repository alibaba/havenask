#ifndef __INDEXLIB_POSITION_LIST_FORMAT_H
#define __INDEXLIB_POSITION_LIST_FORMAT_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/multi_value.h"
#include "indexlib/common/atomic_value_typed.h"
#include "indexlib/index/normal/inverted_index/format/position_list_format_option.h"
#include "indexlib/index/normal/inverted_index/format/position_skip_list_format.h"

IE_NAMESPACE_BEGIN(index);

typedef common::AtomicValueTyped<pos_t> PosValue;
typedef common::AtomicValueTyped<pospayload_t> PosPayloadValue;

class PositionListFormat : public common::MultiValue
{
public:
    PositionListFormat(const PositionListFormatOption& option)
        : mPositionSkipListFormat(NULL)
        , mPosValue(NULL)
        , mPosPayloadValue(NULL)
    {
        Init(option);
    }

    ~PositionListFormat()
    {
        DELETE_AND_SET_NULL(mPositionSkipListFormat);
    }

public:
    PosValue* GetPosValue() const { return mPosValue; }
    PosPayloadValue* GetPosPayloadValue() const { return mPosPayloadValue; }

    PositionSkipListFormat* GetPositionSkipListFormat() const
    { return mPositionSkipListFormat; }

private:
    void Init(const PositionListFormatOption& option);
    void InitPositionSkipListFormat(const PositionListFormatOption& option);

private:
    PositionSkipListFormat* mPositionSkipListFormat;
    PosValue* mPosValue;
    PosPayloadValue* mPosPayloadValue;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PositionListFormat);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_POSITION_LIST_FORMAT_H
