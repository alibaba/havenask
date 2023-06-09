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
#include "indexlib/index/inverted_index/format/PositionListFormatOption.h"

namespace indexlib::index {

AUTIL_LOG_SETUP(indexlib.index, PositionListFormatOption);

bool PositionListFormatOption::operator==(const PositionListFormatOption& right) const
{
    return _hasPositionList == right._hasPositionList && _hasPositionPayload == right._hasPositionPayload &&
           _hasTfBitmap == right._hasTfBitmap;
}

void JsonizablePositionListFormatOption::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    bool hasPositionList;
    bool hasPositionPayload;
    bool hasTfBitmap;

    if (json.GetMode() == FROM_JSON) {
        json.Jsonize("has_position_list", hasPositionList);
        json.Jsonize("has_position_payload", hasPositionPayload);
        json.Jsonize("has_term_frequency_bitmap", hasTfBitmap);

        _positionListFormatOption._hasPositionList = hasPositionList ? 1 : 0;
        _positionListFormatOption._hasPositionPayload = hasPositionPayload ? 1 : 0;
        _positionListFormatOption._hasTfBitmap = hasTfBitmap ? 1 : 0;
    } else {
        hasPositionList = _positionListFormatOption._hasPositionList == 1;
        hasPositionPayload = _positionListFormatOption._hasPositionPayload == 1;
        hasTfBitmap = _positionListFormatOption._hasTfBitmap == 1;

        json.Jsonize("has_position_list", hasPositionList);
        json.Jsonize("has_position_payload", hasPositionPayload);
        json.Jsonize("has_term_frequency_bitmap", hasTfBitmap);
    }
}

} // namespace indexlib::index
