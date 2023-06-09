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

#include "indexlib/file_system/ByteSliceReader.h"

namespace indexlib::index {

class SkipListReader
{
public:
    SkipListReader();
    SkipListReader(const SkipListReader& other);
    virtual ~SkipListReader();

public:
    virtual void Load(const util::ByteSliceList* byteSliceList, uint32_t start, uint32_t end);

    virtual void Load(util::ByteSlice* byteSlice, uint32_t start, uint32_t end);

    virtual std::pair<Status, bool> SkipTo(uint32_t queryKey, uint32_t& key, uint32_t& prevKey, uint32_t& prevValue,
                                           uint32_t& valueDelta)
    {
        assert(false);
        return {};
    }

    uint32_t GetStart() const { return _start; }
    uint32_t GetEnd() const { return _end; }

    const util::ByteSliceList* GetByteSliceList() const { return _byteSliceReader.GetByteSliceList(); }

    int32_t GetSkippedItemCount() const { return _skippedItemCount; }

    virtual uint32_t GetPrevTTF() const { return 0; }
    virtual uint32_t GetCurrentTTF() const { return 0; }

    virtual uint32_t GetLastValueInBuffer() const { return 0; }
    virtual uint32_t GetLastKeyInBuffer() const { return 0; }

protected:
    uint32_t _start;
    uint32_t _end;
    file_system::ByteSliceReader _byteSliceReader;
    int32_t _skippedItemCount;

private:
    friend class SkipListReaderTest;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
