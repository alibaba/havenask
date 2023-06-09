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
#include <utility>

#include "indexlib/base/Status.h"
#include "indexlib/index/orc/RowGroup.h"

#if defined(__clang__) && (__cplusplus >= 202002L)
#include "future_lite/coro/Lazy.h"
#endif

namespace orc {
class Type;
} // namespace orc

namespace indexlibv2::index {

class IOrcIterator
{
public:
    using Result = std::pair<Status, std::unique_ptr<RowGroup>>;

public:
    virtual ~IOrcIterator() = default;

public:
    virtual Status Seek(uint64_t rowId) = 0;

    virtual bool HasNext() const = 0;

    virtual Result Next() = 0;

    virtual const std::shared_ptr<orc::Type>& GetOrcType() const = 0;

    virtual uint64_t GetCurrentRowId() const = 0;

#if defined(__clang__) && (__cplusplus >= 202002L)
    virtual future_lite::coro::Lazy<Status> SeekAsync(uint64_t rowId) { co_return Seek(rowId); }
    virtual future_lite::coro::Lazy<Result> NextAsync() { co_return Next(); }
#endif
};

} // namespace indexlibv2::index
