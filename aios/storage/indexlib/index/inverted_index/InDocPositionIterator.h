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

#include <assert.h>
#include <memory>

#include "indexlib/base/Types.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/inverted_index/Types.h"

namespace indexlib { namespace index {

/**
 * @class InDocPositionIterator
 * @brief position iterator for in-doc position
 */
class InDocPositionIterator
{
public:
    virtual ~InDocPositionIterator() = default;

    /**
     * Seek position one bye one in current doc
     * @return position equal to or greater than /pos/
     */
    pos_t SeekPosition(pos_t pos)
    {
        pos_t result = 0;
        auto ec = SeekPositionWithErrorCode(pos, result);
        index::ThrowIfError(ec);
        return result;
    }
    virtual index::ErrorCode SeekPositionWithErrorCode(pos_t pos, pos_t& nextpos) = 0;
    /**
     * Get position payload of current sought position
     */
    virtual pospayload_t GetPosPayload() = 0;

    /**
     * Get the Section of current position
     */
    virtual sectionid_t GetSectionId() = 0;

    /**
     * Get the Section length of current position
     */
    virtual section_len_t GetSectionLength() = 0;

    /**
     * Get the section weight of current position
     */
    virtual section_weight_t GetSectionWeight() = 0;

    /**
     * Get the field id  of current position
     */
    virtual fieldid_t GetFieldId() = 0;

    /**
     * Get the field position in index of current position
     */
    virtual int32_t GetFieldPosition() = 0;

    /**
     * Return true if position-payload exists.
     */
    virtual bool HasPosPayload() const = 0;
};

}} // namespace indexlib::index
