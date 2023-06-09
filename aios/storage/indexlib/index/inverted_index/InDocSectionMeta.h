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

#include "indexlib/base/Types.h"
#include "indexlib/index/inverted_index/Types.h"

namespace indexlib { namespace index {

class InDocSectionMeta
{
public:
    InDocSectionMeta() {}
    virtual ~InDocSectionMeta() {}

public:
    // common interface
    virtual section_len_t GetSectionLen(int32_t fieldPosition, sectionid_t sectId) const = 0;
    virtual section_len_t GetSectionLenByFieldId(fieldid_t fieldId, sectionid_t sectId) const = 0;

    virtual section_weight_t GetSectionWeight(int32_t fieldPosition, sectionid_t sectId) const = 0;
    virtual section_weight_t GetSectionWeightByFieldId(fieldid_t fieldId, sectionid_t sectId) const = 0;

    virtual field_len_t GetFieldLen(int32_t fieldPosition) const = 0;
    virtual field_len_t GetFieldLenByFieldId(fieldid_t fieldId) const = 0;

public:
    // interface to iterate
    virtual uint32_t GetSectionCountInField(int32_t fieldPosition) const = 0;
    virtual uint32_t GetSectionCount() const = 0;

    virtual void GetSectionMeta(uint32_t idx, section_weight_t& sectionWeight, int32_t& fieldPosition,
                                section_len_t& sectionLength) const = 0;
};

typedef std::shared_ptr<InDocSectionMeta> InDocSectionMetaPtr;
}} // namespace indexlib::index
