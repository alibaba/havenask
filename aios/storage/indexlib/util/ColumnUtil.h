/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <memory>

namespace indexlib { namespace util {

class ColumnUtil
{
public:
    ColumnUtil() {}
    ~ColumnUtil() {}

public:
    static size_t TransformColumnId(size_t globalColumnId, size_t currentColumnCount, size_t globalColumnCount);

    static size_t GetColumnId(uint64_t key, size_t columnCount);

private:
    IE_LOG_DECLARE();
};

///////////////////////////////////
inline size_t ColumnUtil::TransformColumnId(size_t globalColumnId, size_t currentColumnCount, size_t globalColumnCount)
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
}} // namespace indexlib::util
