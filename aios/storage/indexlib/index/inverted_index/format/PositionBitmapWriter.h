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

#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/util/ExpandableBitmap.h"
#include "indexlib/util/SimplePool.h"

namespace indexlib::index {

class PositionBitmapWriter
{
public:
    using UI32Allocator = autil::mem_pool::pool_allocator<uint32_t>;

    PositionBitmapWriter(util::SimplePool* simplePool)
        : _bitmap(1, false, simplePool)
        , _blockOffsets(UI32Allocator(simplePool))
    {
    }

    virtual ~PositionBitmapWriter() = default;

    // virtual for google_mock
    virtual void Set(uint32_t index);
    virtual void Resize(uint32_t size);
    virtual void EndDocument(uint32_t df, uint32_t totalPosCount);
    virtual uint32_t GetDumpLength(uint32_t bitCount) const;
    virtual void Dump(const std::shared_ptr<file_system::FileWriter>& file, uint32_t bitCount);
    const util::ExpandableBitmap& GetBitmap() const { return _bitmap; }

private:
    // represent positions as bitmap.
    // bit 1 represent first occ in doc.
    // bit 0 represent other position in doc.
    util::ExpandableBitmap _bitmap;
    std::vector<uint32_t, UI32Allocator> _blockOffsets;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
