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
#ifndef __INDEXLIB_PREFIX_KEY_TABLE_ITERATOR_BASE_H
#define __INDEXLIB_PREFIX_KEY_TABLE_ITERATOR_BASE_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename ValueType>
class PrefixKeyTableIteratorBase
{
public:
    PrefixKeyTableIteratorBase() {}
    virtual ~PrefixKeyTableIteratorBase() {}

public:
    virtual void Reset() = 0;
    virtual bool IsValid() const = 0;
    virtual void MoveToNext() = 0;
    virtual void Get(uint64_t& key, ValueType& value) const = 0;
    virtual void SortByKey() = 0;
    virtual size_t Size() const = 0;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::index

#endif //__INDEXLIB_PREFIX_KEY_TABLE_ITERATOR_BASE_H
