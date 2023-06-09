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
#include "indexlib/index/inverted_index/InvertedIndexReader.h"

#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/index/inverted_index/config/PackageIndexConfig.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/KeyHasherTyped.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, InvertedIndexReader);

bool InvertedIndexReader::GenFieldMapMask(const std::string& indexName, const std::vector<std::string>& termFieldNames,
                                          fieldmap_t& targetFieldMap)
{
    // TODO: check option
    targetFieldMap = 0;
    std::shared_ptr<indexlibv2::config::PackageIndexConfig> expackIndexConfig =
        std::dynamic_pointer_cast<indexlibv2::config::PackageIndexConfig>(_indexConfig);
    if (!expackIndexConfig) {
        return false;
    }

    int32_t fieldIdxInPack = -1;
    for (size_t i = 0; i < termFieldNames.size(); ++i) {
        // set expack field idx: _indexConfig
        fieldIdxInPack = expackIndexConfig->GetFieldIdxInPack(termFieldNames[i]);
        if (fieldIdxInPack == -1) {
            AUTIL_LOG(WARN, "GenFieldMapMask failed: [%s] not in expack field!", termFieldNames[i].c_str());
            return false;
        }
        targetFieldMap |= (fieldmap_t)(1 << fieldIdxInPack);
    }
    return true;
}

} // namespace indexlib::index
