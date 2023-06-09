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

#include "indexlib/index/common//AtomicValueTyped.h"
#include "indexlib/index/common/MultiValue.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/inverted_index/format/PositionListFormatOption.h"
#include "indexlib/index/inverted_index/format/PositionSkipListFormat.h"

namespace indexlib::index {

using PosValue = AtomicValueTyped<pos_t>;
using PosPayloadValue = AtomicValueTyped<pospayload_t>;

class PositionListFormat : public MultiValue
{
public:
    PositionListFormat(const PositionListFormatOption& option,
                       format_versionid_t versionId = indexlibv2::config::InvertedIndexConfig::BINARY_FORMAT_VERSION)
        : _positionSkipListFormat(nullptr)
        , _posValue(nullptr)
        , _posPayloadValue(nullptr)
        , _formatVersion(versionId)
    {
        Init(option);
    }
    ~PositionListFormat() { DELETE_AND_SET_NULL(_positionSkipListFormat); }

    PosValue* GetPosValue() const { return _posValue; }
    PosPayloadValue* GetPosPayloadValue() const { return _posPayloadValue; }
    PositionSkipListFormat* GetPositionSkipListFormat() const { return _positionSkipListFormat; }

private:
    void Init(const PositionListFormatOption& option);
    void InitPositionSkipListFormat(const PositionListFormatOption& option);

    PositionSkipListFormat* _positionSkipListFormat;
    PosValue* _posValue;
    PosPayloadValue* _posPayloadValue;
    format_versionid_t _formatVersion;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
