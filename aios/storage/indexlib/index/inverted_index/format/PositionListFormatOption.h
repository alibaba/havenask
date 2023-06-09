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
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/inverted_index/Constant.h"

namespace indexlib::index {

class PositionListFormatOption
{
public:
    inline PositionListFormatOption(optionflag_t optionFlag = OPTION_FLAG_ALL) { Init(optionFlag); }
    ~PositionListFormatOption() {}

    inline void Init(optionflag_t optionFlag)
    {
        // we think string and number etc has no position list
        // they are checked in schema_configurator

        _hasPositionList = optionFlag & of_position_list ? 1 : 0;
        if ((optionFlag & of_position_list) && (optionFlag & of_position_payload)) {
            _hasPositionPayload = 1;
        } else {
            _hasPositionPayload = 0;
        }

        if ((optionFlag & of_term_frequency) && (optionFlag & of_tf_bitmap)) {
            _hasTfBitmap = 1;
        } else {
            _hasTfBitmap = 0;
        }
        _unused = 0;
    }

    bool HasPositionList() const { return _hasPositionList == 1; }
    bool HasPositionPayload() const { return _hasPositionPayload == 1; }
    bool HasTfBitmap() const { return _hasTfBitmap == 1; }
    bool operator==(const PositionListFormatOption& right) const;

private:
    uint8_t _hasPositionList    : 1;
    uint8_t _hasPositionPayload : 1;
    uint8_t _hasTfBitmap        : 1;
    uint8_t _unused             : 5;

    friend class PositionListEncoderTest;
    friend class JsonizablePositionListFormatOption;

    AUTIL_LOG_DECLARE();
};

class JsonizablePositionListFormatOption : public autil::legacy::Jsonizable
{
public:
    JsonizablePositionListFormatOption(PositionListFormatOption option = PositionListFormatOption())
        : _positionListFormatOption(option)
    {
    }

    const PositionListFormatOption& GetPositionListFormatOption() const { return _positionListFormatOption; }
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

private:
    PositionListFormatOption _positionListFormatOption;
};

} // namespace indexlib::index
