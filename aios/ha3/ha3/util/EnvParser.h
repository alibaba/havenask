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

#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace util {
class EnvParser {
public:
    static bool parseParaWays(const std::string &paraEnv, std::vector<std::string> &paraWaysVec);

private:
    static bool isWayValid(const std::string &paraVal);

private:
    AUTIL_LOG_DECLARE();
};
} // namespace util
} // namespace isearch
