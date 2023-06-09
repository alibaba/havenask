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

#include "indexlib/index/inverted_index/InDocPositionIterator.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapInDocPositionState.h"

namespace indexlib::index {

class BitmapInDocPositionIterator : public InDocPositionIterator
{
public:
    BitmapInDocPositionIterator() {}
    ~BitmapInDocPositionIterator() {}

public:
    index::ErrorCode SeekPositionWithErrorCode(pos_t pos, pos_t& nextpos) override
    {
        nextpos = INVALID_POSITION;
        return index::ErrorCode::OK;
    }
    pospayload_t GetPosPayload() override { return 0; }
    sectionid_t GetSectionId() override { return 0; }
    section_len_t GetSectionLength() override { return 0; }
    section_weight_t GetSectionWeight() override { return 0; }
    fieldid_t GetFieldId() override { return INVALID_FIELDID; }
    int32_t GetFieldPosition() override { return 0; }
    bool HasPosPayload() const override { return false; }

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
