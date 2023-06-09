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
#include "indexlib/index/inverted_index/format/PositionListFormatOption.h"

namespace indexlib::index {

class PositionSkipListFormat : public MultiValue
{
public:
    using TotalPosValue = AtomicValueTyped<uint32_t>;
    using OffsetValue = AtomicValueTyped<uint32_t>;

    PositionSkipListFormat(const PositionListFormatOption& option, indexlibv2::config::format_versionid_t formatVersion)
        : _totalPosValue(nullptr)
        , _offsetValue(nullptr)
        , _formatVersion(formatVersion)
    {
        Init(option);
    }
    ~PositionSkipListFormat() = default;

private:
    void Init(const PositionListFormatOption& option);

    TotalPosValue* _totalPosValue;
    OffsetValue* _offsetValue;
    indexlibv2::config::format_versionid_t _formatVersion;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
