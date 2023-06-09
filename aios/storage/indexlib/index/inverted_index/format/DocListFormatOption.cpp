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
#include "indexlib/index/inverted_index/format/DocListFormatOption.h"

namespace indexlib::index {

AUTIL_LOG_SETUP(indexlib.index, DocListFormatOption);

bool DocListFormatOption::operator==(const DocListFormatOption& right) const
{
    return _hasTf == right._hasTf && _hasTfList == right._hasTfList && _hasTfBitmap == right._hasTfBitmap &&
           _hasDocPayload == right._hasDocPayload && _hasFieldMap == right._hasFieldMap &&
           _shortListVbyteCompress == right._shortListVbyteCompress;
}

void JsonizableDocListFormatOption::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    bool hasTf;
    bool hasTfList;
    bool hasTfBitmap;
    bool hasDocPayload;
    bool hasFieldMap;
    bool shortListVbyteCompress = false;

    if (json.GetMode() == FROM_JSON) {
        json.Jsonize("has_term_frequency", hasTf);
        json.Jsonize("has_term_frequency_list", hasTfList);
        json.Jsonize("has_term_frequency_bitmap", hasTfBitmap);
        json.Jsonize("has_doc_payload", hasDocPayload);
        json.Jsonize("has_field_map", hasFieldMap);
        json.Jsonize("is_shortlist_vbyte_compress", shortListVbyteCompress, shortListVbyteCompress);

        _docListFormatOption._hasTf = hasTf ? 1 : 0;
        _docListFormatOption._hasTfList = hasTfList ? 1 : 0;
        _docListFormatOption._hasTfBitmap = hasTfBitmap ? 1 : 0;
        _docListFormatOption._hasDocPayload = hasDocPayload ? 1 : 0;
        _docListFormatOption._hasFieldMap = hasFieldMap ? 1 : 0;
        _docListFormatOption.SetShortListVbyteCompress(shortListVbyteCompress);
    } else {
        hasTf = _docListFormatOption._hasTf == 1;
        hasTfList = _docListFormatOption._hasTfList == 1;
        hasTfBitmap = _docListFormatOption._hasTfBitmap == 1;
        hasDocPayload = _docListFormatOption._hasDocPayload == 1;
        hasFieldMap = _docListFormatOption._hasFieldMap == 1;
        shortListVbyteCompress = _docListFormatOption.IsShortListVbyteCompress();

        json.Jsonize("has_term_frequency", hasTf);
        json.Jsonize("has_term_frequency_list", hasTfList);
        json.Jsonize("has_term_frequency_bitmap", hasTfBitmap);
        json.Jsonize("has_doc_payload", hasDocPayload);
        json.Jsonize("has_field_map", hasFieldMap);
        json.Jsonize("is_shortlist_vbyte_compress", shortListVbyteCompress);
    }
}

} // namespace indexlib::index
