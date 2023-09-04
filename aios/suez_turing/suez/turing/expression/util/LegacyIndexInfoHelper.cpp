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
#include "suez/turing/expression/util/LegacyIndexInfoHelper.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <map>
#include <utility>

#include "alog/Logger.h"
#include "suez/turing/expression/util/IndexInfos.h"

using namespace std;

namespace suez {
namespace turing {
AUTIL_LOG_SETUP(common, LegacyIndexInfoHelper);

LegacyIndexInfoHelper::LegacyIndexInfoHelper() : _indexInfos(nullptr) {}

LegacyIndexInfoHelper::LegacyIndexInfoHelper(const IndexInfos *indexInfos) : _indexInfos(indexInfos) {}

LegacyIndexInfoHelper::~LegacyIndexInfoHelper() {}

indexid_t LegacyIndexInfoHelper::getIndexId(const string &indexName) const {
    assert(_indexInfos);
    const IndexInfo *indexInfo = _indexInfos->getIndexInfo(indexName.c_str());
    if (indexInfo) {
        return indexInfo->getIndexId();
    } else {
        return INVALID_INDEXID;
    }
}

int32_t LegacyIndexInfoHelper::getFieldPosition(const string &indexName, const string &fieldname) const {
    assert(_indexInfos);
    const IndexInfo *indexInfo = _indexInfos->getIndexInfo(indexName.c_str());
    if (indexInfo) {
        return indexInfo->getFieldPosition(fieldname.c_str());
    } else {
        return -1;
    }
}

std::vector<std::string> LegacyIndexInfoHelper::getFieldList(const std::string &indexName) const {
    assert(_indexInfos);

    std::vector<std::string> fields;
    const IndexInfo *indexInfo = _indexInfos->getIndexInfo(indexName.c_str());
    if (nullptr == indexInfo) {
        AUTIL_LOG(TRACE3, "not found the IndexInfo, indexName: %s", indexName.c_str());
        return fields;
    }

    const map<string, fieldbitmap_t> &fieldMap = indexInfo->getFieldName2Bitmaps();
    for (map<string, fieldbitmap_t>::const_iterator it = fieldMap.begin(); it != fieldMap.end(); it++) {
        fields.push_back(it->first);
    }
    return fields;
}
void LegacyIndexInfoHelper::setIndexInfos(const IndexInfos *indexInfos) { _indexInfos = indexInfos; }

} // namespace turing
} // namespace suez
