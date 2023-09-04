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

namespace suez {
struct KMonitorMetaInfo;

struct KMonitorMetaParam {
    std::string partId;
    std::string serviceName;
    std::string amonitorPath;
    std::string roleType;
};

class KMonitorMetaParser {
public:
    KMonitorMetaParser() {}
    KMonitorMetaParser(const KMonitorMetaParser &) = delete;
    KMonitorMetaParser &operator=(const KMonitorMetaParser &) = delete;

public:
    static bool parse(const KMonitorMetaParam &param, KMonitorMetaInfo &result);

private:
    static bool parseDefault(const std::string &serviceName, std::string roleType, KMonitorMetaInfo &result);
    static bool
    parseTisplus(const std::vector<std::string> &patternVec, std::string roleType, KMonitorMetaInfo &result);
};

} // namespace suez
