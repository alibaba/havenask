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

#include "autil/NoCopyable.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/util/Bitmap.h"

namespace indexlibv2::index {

class SegmentUpdateBitmap : private autil::NoCopyable
{
public:
    SegmentUpdateBitmap(size_t docCount, autil::mem_pool::PoolBase* pool) : _docCount(docCount), _bitmap(false, pool) {}

    ~SegmentUpdateBitmap() {}

public:
    Status Set(docid_t localDocId)
    {
        if (unlikely(size_t(localDocId) >= _docCount)) {
            AUTIL_LOG(ERROR, "local docId[%d] out of range[0:%lu)", localDocId, _docCount);
            return Status::InternalError("local docId[%d] out of range[0:%lu)", localDocId, _docCount);
        }
        if (unlikely(_bitmap.GetItemCount() == 0)) {
            _bitmap.Alloc(_docCount, false);
        }

        _bitmap.Set(localDocId);
        return Status::OK();
    }

    uint32_t GetUpdateDocCount() const { return _bitmap.GetSetCount(); }

private:
    size_t _docCount;
    indexlib::util::Bitmap _bitmap;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
