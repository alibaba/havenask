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
#include "ha3/util/EnvParser.h"

#include <iosfwd>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"

using namespace autil;
using namespace std;
namespace isearch {
namespace util {
AUTIL_LOG_SETUP(ha3, EnvParser);

bool EnvParser::parseParaWays(const string &paraEnv,
                              vector<string> &paraWaysVec)
{ //paraWays=2,4,8
    paraWaysVec.clear();
    StringUtil::split(paraWaysVec, paraEnv, ',');
    if (0 == paraWaysVec.size()) {
        return false;
    }
    for (const auto &paraVal : paraWaysVec) {
        if (!isWayValid(paraVal)) {
            AUTIL_LOG(ERROR, "para search ways param [(%s) from (%s)] invalid",
                    paraVal.c_str(), paraEnv.c_str());
            paraWaysVec.clear();
            return false;
        }
    }
    return true;
}

bool EnvParser::isWayValid(const string& paraVal) {
    if ("2" == paraVal || "4" == paraVal || "8" == paraVal || "16" == paraVal) {
        return true;
    }
    return false;
}

} // namespace util
} // namespace isearch
