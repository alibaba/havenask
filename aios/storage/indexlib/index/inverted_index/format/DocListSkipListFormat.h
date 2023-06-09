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

#include "indexlib/index/common/AtomicValueTyped.h"
#include "indexlib/index/common/MultiValue.h"
#include "indexlib/index/inverted_index/format/DocListFormatOption.h"

namespace indexlib::index {

class DocListSkipListFormat : public MultiValue
{
public:
    using DocIdValue = AtomicValueTyped<uint32_t>;
    using TotalTFValue = AtomicValueTyped<uint32_t>;
    using OffsetValue = AtomicValueTyped<uint32_t>;

    DocListSkipListFormat(const DocListFormatOption& option, indexlibv2::config::format_versionid_t formatVersion)
        : _DocIdValue(nullptr)
        , _TotalTFValue(nullptr)
        , _OffsetValue(nullptr)
        , _formatVersion(formatVersion)
    {
        Init(option);
    }
    ~DocListSkipListFormat() = default;

    bool HasTfList() const { return _TotalTFValue != nullptr; }

private:
    void Init(const DocListFormatOption& option);

    DocIdValue* _DocIdValue;
    TotalTFValue* _TotalTFValue;
    OffsetValue* _OffsetValue;
    indexlibv2::config::format_versionid_t _formatVersion;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
