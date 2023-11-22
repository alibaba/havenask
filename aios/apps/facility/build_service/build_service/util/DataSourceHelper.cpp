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
#include "build_service/util/DataSourceHelper.h"

#include <autil/StringUtil.h>
#include <map>
#include <string>
#include <utility>

#include "build_service/config/CLIOptionNames.h"
#include "build_service/proto/DataDescription.h"

using namespace build_service::config;

namespace build_service { namespace util {

bool DataSourceHelper::isRealtime(const build_service::proto::DataDescription& ds)
{
    auto it_type = ds.find(READ_SRC_TYPE);
    if (ds.end() != it_type && SWIFT_READ_SRC == it_type->second) {
        return true;
    }
    auto it_streaming_source = ds.find(READ_STREAMING_SOURCE);
    bool streaming_source = false;
    if (ds.end() != it_streaming_source &&
        autil::StringUtil::fromString(it_streaming_source->second, streaming_source) && streaming_source) {
        return true;
    }
    return false;
}

}} // namespace build_service::util
