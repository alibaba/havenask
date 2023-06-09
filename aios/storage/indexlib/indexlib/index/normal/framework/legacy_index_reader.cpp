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
#include "indexlib/index/normal/framework/legacy_index_reader.h"

#include "indexlib/config/package_index_config.h"
#include "indexlib/index/normal/inverted_index/format/index_format_option.h"

using namespace std;
using namespace autil::legacy;

using namespace indexlib::common;
using namespace indexlib::config;

namespace indexlib::index {
IE_LOG_SETUP(index, LegacyIndexReader);

LegacyIndexReader::LegacyIndexReader() { _indexFormatOption = std::make_shared<LegacyIndexFormatOption>(); }

LegacyIndexReader::~LegacyIndexReader() {}

bool LegacyIndexReader::GenFieldMapMask(const string& indexName, const vector<string>& termFieldNames,
                                        fieldmap_t& targetFieldMap)
{
    // TODO: check option
    targetFieldMap = 0;
    config::PackageIndexConfigPtr expackIndexConfig = dynamic_pointer_cast<config::PackageIndexConfig>(_indexConfig);
    if (!expackIndexConfig) {
        return false;
    }

    int32_t fieldIdxInPack = -1;
    for (size_t i = 0; i < termFieldNames.size(); ++i) {
        // set expack field idx: mIndexConfig
        fieldIdxInPack = expackIndexConfig->GetFieldIdxInPack(termFieldNames[i]);
        if (fieldIdxInPack == -1) {
            IE_LOG(WARN, "GenFieldMapMask failed: [%s] not in expack field!", termFieldNames[i].c_str());
            return false;
        }
        targetFieldMap |= (fieldmap_t)(1 << fieldIdxInPack);
    }
    return true;
}

} // namespace indexlib::index
