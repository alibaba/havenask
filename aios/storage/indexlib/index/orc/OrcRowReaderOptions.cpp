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
#include "indexlib/index/orc/OrcRowReaderOptions.h"

#include "indexlib/util/Exception.h"

using namespace autil::legacy;
using namespace autil::legacy::json;

namespace indexlibv2::config {
AUTIL_LOG_SETUP(indexlib.index, OrcRowReaderOptions);

OrcRowReaderOptions::OrcRowReaderOptions() {}

OrcRowReaderOptions::~OrcRowReaderOptions() {}

void OrcRowReaderOptions::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
#define SET_OPTIONS(type, key, funcName)                                                                               \
    do {                                                                                                               \
        std::string keyStr = key;                                                                                      \
        auto iter = jsonMap.find(keyStr);                                                                              \
        if (iter != jsonMap.end()) {                                                                                   \
            type value;                                                                                                \
            FromJson(value, iter->second);                                                                             \
            _changedOptions.insert(keyStr);                                                                            \
            orc::RowReaderOptions::funcName(value);                                                                    \
        }                                                                                                              \
    } while (0)
#define SET_ENUM_OPTIONS(type, key, funcName, castFunc)                                                                \
    do {                                                                                                               \
        std::string keyStr = key;                                                                                      \
        auto iter = jsonMap.find(key);                                                                                 \
        if (iter != jsonMap.end()) {                                                                                   \
            type value;                                                                                                \
            FromJson(value, iter->second);                                                                             \
            _changedOptions.insert(keyStr);                                                                            \
            orc::RowReaderOptions::funcName(castFunc(value));                                                          \
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
#define ENUM_OPTIONS_TO_JSON(key, funcName, castFunc)                                                                  \
    do {                                                                                                               \
        std::string keyStr = key;                                                                                      \
        auto iter = _changedOptions.find(keyStr);                                                                      \
        if (iter != _changedOptions.end()) {                                                                           \
            auto value = castFunc(funcName());                                                                         \
            json.Jsonize(keyStr, value);                                                                               \
        }                                                                                                              \
    } while (0)

    if (json.GetMode() == FROM_JSON) {
        JsonMap jsonMap = json.GetMap();
        SET_OPTIONS(uint64_t, "stripe_cache_size", setStripeCacheSize);
        SET_OPTIONS(bool, "enable_seek_use_index", setEnableSeekUseIndex);
        SET_OPTIONS(bool, "enable_lazy_decoding", setEnableLazyDecoding);
        SET_OPTIONS(bool, "enable_lazy_load", setEnableLazyLoad);
        SET_OPTIONS(bool, "enable_prefetch_on_lazy_load", setEnablePrefetchOnLazyLoad);
        SET_OPTIONS(bool, "enable_row_group_alignment", setEnableRowGroupAlignment);
        SET_OPTIONS(bool, "enable_column_boundary", setEnableColumnBoundary);
        SET_OPTIONS(bool, "enable_parallel_io", setEnableParallelIO);
        SET_OPTIONS(std::string, "reader_timezone", setReaderTimezone);
        SET_OPTIONS(int32_t, "stripe_id", setStripeId);
        SET_OPTIONS(bool, "force_precise_sort_ranges", setForcePreciseSortRanges);
        SET_ENUM_OPTIONS(std::string, "async_prefetch_mode", setAsyncPrefetchMode, StrToPrefetchMode);
    } else if (json.GetMode() == TO_JSON) {
        OPTIONS_TO_JSON("stripe_cache_size", getStripeCacheSize);
        OPTIONS_TO_JSON("enable_seek_use_index", getEnableSeekUseIndex);
        OPTIONS_TO_JSON("enable_lazy_decoding", getEnableLazyDecoding);
        OPTIONS_TO_JSON("enable_lazy_load", getEnableLazyLoad);
        OPTIONS_TO_JSON("enable_prefetch_on_lazy_load", getEnablePrefetchOnLazyLoad);
        OPTIONS_TO_JSON("enable_row_group_alignment", getEnableRowGroupAlignment);
        OPTIONS_TO_JSON("enable_column_boundary", getEnableColumnBoundary);
        OPTIONS_TO_JSON("enable_parallel_io", getEnableParallelIO);
        OPTIONS_TO_JSON("reader_timezone", getReaderTimezone);
        OPTIONS_TO_JSON("stripe_id", getStripeId);
        OPTIONS_TO_JSON("force_precise_sort_ranges", getForcePreciseSortRanges);
        ENUM_OPTIONS_TO_JSON("async_prefetch_mode", getAsyncPrefetchMode, PrefetchModeToStr);
    }
}

orc::PrefetchMode OrcRowReaderOptions::StrToPrefetchMode(const std::string& mode)
{
    if (mode == "scan") {
        return orc::PrefetchMode::SCAN;
    } else if (mode == "read") {
        return orc::PrefetchMode::READ;
    } else if (mode == "point_lookup") {
        return orc::PrefetchMode::POINT_LOOKUP;
    } else {
        INDEXLIB_FATAL_ERROR(Schema, "invalid prefetch mode [%s].", mode.c_str());
    }
}

std::string OrcRowReaderOptions::PrefetchModeToStr(orc::PrefetchMode mode)
{
    switch (mode) {
    case orc::PrefetchMode::SCAN:
        return "scan";
    case orc::PrefetchMode::READ:
        return "read";
    case orc::PrefetchMode::POINT_LOOKUP:
        return "point_lookup";
    default:
        INDEXLIB_FATAL_ERROR(Schema, "invalid prefetch mode to str.");
    }
}

} // namespace indexlibv2::config
