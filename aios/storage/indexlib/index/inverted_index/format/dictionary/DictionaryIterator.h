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

#include "autil/Log.h"
#include "future_lite/coro/Lazy.h"
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/index/common/DictKeyInfo.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/common/Types.h"

namespace indexlib::index {

class DictionaryIterator
{
public:
    DictionaryIterator() = default;
    virtual ~DictionaryIterator() = default;

    virtual bool HasNext() const = 0;
    virtual void Next(index::DictKeyInfo& key, dictvalue_t& value) = 0;
    virtual void Seek(dictkey_t key) { assert(false); }
    virtual future_lite::coro::Lazy<index::ErrorCode> SeekAsync(dictkey_t key, file_system::ReadOption option) noexcept
    {
        assert(false);
        co_return index::ErrorCode::UnSupported;
    }
    virtual future_lite::coro::Lazy<index::ErrorCode> NextAsync(index::DictKeyInfo& key, file_system::ReadOption option,
                                                                dictvalue_t& value) noexcept
    {
        assert(false);
        co_return index::ErrorCode::UnSupported;
    }
};

} // namespace indexlib::index
