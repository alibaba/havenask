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
#include "indexlib/index/common/field_format/spatial/shape/ShapeCoverer.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, ShapeCoverer);
ShapeCoverer::ShapeCoverer(int8_t indexTopLevel, int8_t indexDetailLevel, bool onlyGetLeafCell)
    : _indexTopLevel(indexTopLevel)
    , _indexDetailLevel(indexDetailLevel)
    , _onlyGetLeafCell(onlyGetLeafCell)
    , _cellCount(0)
{
    assert(GeoHashUtil::IsLevelValid(indexTopLevel));
    assert(GeoHashUtil::IsLevelValid(indexDetailLevel));
    assert(indexDetailLevel >= indexTopLevel);
}
} // namespace indexlib::index
