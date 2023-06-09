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

namespace indexlibv2::index {
template <typename SKeyType>
class SKeyIteratorBase
{
public:
    SKeyIteratorBase() = default;
    virtual ~SKeyIteratorBase() = default;

public:
    bool HasPKeyDeleted() const { return _hasPKeyDeleted; }
    uint32_t GetPKeyDeletedTs() const { return _pkeyDeletedTs; }
    virtual SKeyType GetSKey() const = 0;
    virtual bool IsDeleted() const = 0;
    virtual uint32_t GetTs() const = 0;
    virtual uint32_t GetExpireTime() const = 0;
    virtual bool IsValid() const = 0;
    virtual void MoveToNext() = 0;

protected:
    uint32_t _pkeyDeletedTs = 0u;
    bool _hasPKeyDeleted = false;
};

} // namespace indexlibv2::index