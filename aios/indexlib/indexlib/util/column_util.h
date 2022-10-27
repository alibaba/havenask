#ifndef __INDEXLIB_COLUMN_UTIL_H
#define __INDEXLIB_COLUMN_UTIL_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(util);

class ColumnUtil
{
public:
    ColumnUtil() {}
    ~ColumnUtil() {}

public:
    static size_t TransformColumnId(
        size_t globalColumnId, size_t currentColumnCount, size_t globalColumnCount);

    static size_t GetColumnId(uint64_t key, size_t columnCount);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ColumnUtil);

///////////////////////////////////
inline size_t ColumnUtil::TransformColumnId(
        size_t globalColumnId, size_t currentColumnCount, size_t globalColumnCount)
{
    assert(globalColumnCount % currentColumnCount == 0);
    assert((globalColumnCount & (globalColumnCount - 1)) == 0);
    assert((currentColumnCount & (currentColumnCount - 1)) == 0);
    return globalColumnId & (currentColumnCount - 1);
}

inline size_t ColumnUtil::GetColumnId(uint64_t key, size_t columnCount)
{
    assert((columnCount & (columnCount - 1)) == 0);
    assert(columnCount);
    return key & (columnCount - 1);
}

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_COLUMN_UTIL_H
