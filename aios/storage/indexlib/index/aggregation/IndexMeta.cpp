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
#include "indexlib/index/aggregation/IndexMeta.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, IndexMeta);

void IndexMeta::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("key_count", keyCount, keyCount);
    json.Jsonize("total_value_count", totalValueCount, totalValueCount);
    json.Jsonize("total_value_count_after_unique", totalValueCountAfterUnique, totalValueCountAfterUnique);
    json.Jsonize("max_value_count_of_all_keys", maxValueCountOfAllKeys, maxValueCountOfAllKeys);
    json.Jsonize("max_value_count_of_all_keys_after_unique", maxValueCountOfAllKeysAfterUnique,
                 maxValueCountOfAllKeysAfterUnique);
    json.Jsonize("max_value_bytes_of_all_keys", maxValueBytesOfAllKeys, maxValueBytesOfAllKeys);
    json.Jsonize("max_value_bytes_of_all_keys_after_unique", maxValueBytesOfAllKeysAfterUnique,
                 maxValueBytesOfAllKeysAfterUnique);
    json.Jsonize("accumulated_value_bytes", accmulatedValueBytes, accmulatedValueBytes);
    json.Jsonize("accumulated_value_bytes_after_unique", accmulatedValueBytesAfterUnique,
                 accmulatedValueBytesAfterUnique);
}

std::string IndexMeta::ToString() const { return ToJsonString(*this); }

bool IndexMeta::FromString(const std::string& jsonStr)
{
    try {
        FromJsonString(*this, jsonStr);
        return true;
    } catch (const std::exception& e) {
        return false;
    }
}

} // namespace indexlibv2::index
