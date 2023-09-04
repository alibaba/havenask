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

#include "autil/result/Result.h"
#include "suez/admin/ClusterService.pb.h"

namespace catalog {
class Table;
}

namespace suez {

class BizConfigMaker {
public:
    static autil::Result<std::string>
    genSignature(const ClusterDeployment &deployment, const std::string &templatePath, const std::string &storeRoot);
    static autil::Result<std::string>
    make(const ClusterDeployment &deployment, const std::string &templatePath, const std::string &storeRoot);

private:
    static bool genAnalyzerJson(const std::string &templatePath, const std::string &configRoot);
    static bool genSqlJson(const std::string &templatePath,
                           const std::string &configRoot,
                           const std::vector<std::string> &searcherZones);
    static bool genNaviScript(const std::string &templatePath, const std::string &configRoot);
};

} // namespace suez
