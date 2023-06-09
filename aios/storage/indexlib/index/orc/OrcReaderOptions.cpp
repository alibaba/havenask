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
#include "indexlib/index/orc/OrcReaderOptions.h"

using namespace autil::legacy;
using namespace autil::legacy::json;

namespace indexlibv2::config {
AUTIL_LOG_SETUP(indexlib.index, OrcReaderOptions);

OrcReaderOptions::OrcReaderOptions() {}

OrcReaderOptions::~OrcReaderOptions() {}

void OrcReaderOptions::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
#define SET_OPTIONS(type, key, funcName)                                                                               \
    do {                                                                                                               \
        std::string keyStr = key;                                                                                      \
        auto iter = jsonMap.find(keyStr);                                                                              \
        if (iter != jsonMap.end()) {                                                                                   \
            type value;                                                                                                \
            FromJson(value, iter->second);                                                                             \
            _changedOptions.insert(keyStr);                                                                            \
            orc::ReaderOptions::funcName(value);                                                                       \
        }                                                                                                              \
    } while (0)

#define OPTIONS_TO_JSON(key, funcName)                                                                                 \
    do {                                                                                                               \
        std::string keyStr = key;                                                                                      \
        auto iter = _changedOptions.find(keyStr);                                                                      \
        if (iter != _changedOptions.end()) {                                                                           \
            auto value = funcName();                                                                                   \
            json.Jsonize(keyStr, value);                                                                               \
        }                                                                                                              \
    } while (0)

    if (json.GetMode() == FROM_JSON) {
        JsonMap jsonMap = json.GetMap();
        SET_OPTIONS(uint64_t, "tail_location", setTailLocation);
        SET_OPTIONS(bool, "enable_async_prefetch", setEnableAsyncPrefetch);
        SET_OPTIONS(uint64_t, "max_prefetch_buffer_size", setMaxPrefetchBufferSize);
    } else if (json.GetMode() == TO_JSON) {
        OPTIONS_TO_JSON("tail_location", getTailLocation);
        OPTIONS_TO_JSON("enable_async_prefetch", getEnableAsyncPrefetch);
        OPTIONS_TO_JSON("max_prefetch_buffer_size", getMaxPrefetchBufferSize);
    }
}

} // namespace indexlibv2::config
