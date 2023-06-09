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
#include "indexlib/index/inverted_index/format/TermMeta.h"

namespace indexlib::index {

class BitmapPostingWriter;

class InMemBitmapIndexDecoder
{
public:
    InMemBitmapIndexDecoder();
    ~InMemBitmapIndexDecoder();

    void Init(const BitmapPostingWriter* postingWriter);

    const TermMeta* GetTermMeta() const { return &_termMeta; }

    uint32_t GetBitmapItemCount() const { return _bitmapItemCount; }
    uint32_t* GetBitmapData() const { return _bitmapData; }

private:
    TermMeta _termMeta;
    uint32_t _bitmapItemCount;
    uint32_t* _bitmapData;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
