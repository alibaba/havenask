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
#include "indexlib/index/orc/OrcGeneralOptions.h"

using namespace autil::legacy;
using namespace autil::legacy::json;

namespace indexlibv2::config {

static constexpr size_t DEFAULT_ROW_GROUP_SIZE = 4096;
static constexpr size_t DEFAULT_BUILD_BUFFER_SIZE = 3;
/* buffer size 至少需要 2，一个在 build 一个在 seal，当 build 和 seal 速度接近时可以设置成 3 多一个缓冲。
当 build 速度明显大于或者小于 seal 速度时调大 buffer > 3 都没意义。
 */

AUTIL_LOG_SETUP(indexlib.index, OrcGeneralOptions);

OrcGeneralOptions::OrcGeneralOptions()
    : _rowGroupSize(DEFAULT_ROW_GROUP_SIZE)
    , _buildBufferSize(DEFAULT_BUILD_BUFFER_SIZE)
{
}

void OrcGeneralOptions::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
#define SET_OPTIONS(key, value)                                                                                        \
    do {                                                                                                               \
        std::string keyStr = key;                                                                                      \
        auto iter = jsonMap.find(keyStr);                                                                              \
        if (iter != jsonMap.end()) {                                                                                   \
            json.Jsonize(keyStr, value);                                                                               \
            _changedOptions.insert(keyStr);                                                                            \
        }                                                                                                              \
    } while (0)

#define OPTIONS_TO_JSON(key, value)                                                                                    \
    do {                                                                                                               \
        std::string keyStr = key;                                                                                      \
        auto iter = _changedOptions.find(keyStr);                                                                      \
        if (iter != _changedOptions.end()) {                                                                           \
            json.Jsonize(keyStr, value);                                                                               \
        }                                                                                                              \
    } while (0)

    auto jsonMap = json.GetMap();
    if (json.GetMode() == FROM_JSON) {
        SET_OPTIONS("row_group_size", _rowGroupSize);
        SET_OPTIONS("build_buffer_size", _buildBufferSize);
    } else if (json.GetMode() == TO_JSON) {
        OPTIONS_TO_JSON("row_group_size", _rowGroupSize);
        OPTIONS_TO_JSON("build_buffer_size", _buildBufferSize);
    }
}

} // namespace indexlibv2::config
