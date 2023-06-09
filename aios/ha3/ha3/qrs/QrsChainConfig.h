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
#pragma once

#include <map>
#include <string>

#include "ha3/config/ProcessorInfo.h"
#include "ha3/config/QrsChainInfo.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace qrs {

/** the data structure parsed from config file. */
struct QrsChainConfig {
    std::map<std::string, config::ProcessorInfo> processorInfoMap;
    std::map<std::string, config::QrsChainInfo> chainInfoMap;
};

} // namespace qrs
} // namespace isearch

