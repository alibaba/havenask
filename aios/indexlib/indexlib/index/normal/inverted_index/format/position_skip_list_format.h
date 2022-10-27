#ifndef __INDEXLIB_POSITION_SKIP_LIST_FORMAT_H
#define __INDEXLIB_POSITION_SKIP_LIST_FORMAT_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/common/atomic_value_typed.h"
#include "indexlib/common/multi_value.h"
#include "indexlib/index/normal/inverted_index/format/position_list_format_option.h"

IE_NAMESPACE_BEGIN(index);

class PositionSkipListFormat : public common::MultiValue
{
public:
    typedef common::AtomicValueTyped<uint32_t> TotalPosValue;
    typedef common::AtomicValueTyped<uint32_t> OffsetValue;

public:
    PositionSkipListFormat(const PositionListFormatOption& option)
        : mTotalPosValue(NULL)
        , mOffsetValue(NULL)
    {
        Init(option);
    }

    ~PositionSkipListFormat() {}

private:
    void Init(const PositionListFormatOption& option);

private:
    TotalPosValue* mTotalPosValue;
    OffsetValue* mOffsetValue;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PositionSkipListFormat);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_POSITION_SKIP_LIST_FORMAT_H
