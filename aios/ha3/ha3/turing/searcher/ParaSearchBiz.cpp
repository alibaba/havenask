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
#include "ha3/turing/searcher/ParaSearchBiz.h"

#include <cstddef>
#include <map>
#include <memory>
#include <vector>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "autil/legacy/any.h"
#include "autil/legacy/jsonizable.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/misc/common.h"
#include "autil/EnvUtil.h"
#include "suez/sdk/KMonitorMetaInfo.h"
#include "suez/sdk/PathDefine.h"
#include "suez/sdk/ServiceInfo.h"
#include "suez/turing/expression/cava/common/CavaConfig.h"
#include "suez/turing/common/JoinConfigInfo.h"
#include "suez/turing/common/WorkerParam.h"
#include "tensorflow/core/platform/byte_order.h"

#include "ha3/config/ClusterConfigInfo.h"
#include "ha3/config/ConfigAdapter.h"
#include "ha3/config/JoinConfig.h"
#include "ha3/config/TuringOptionsInfo.h"
#include "ha3/util/TypeDefine.h"
#include "ha3/config/VersionConfig.h"
#include "autil/Log.h"

using namespace std;
using namespace suez;
using namespace tensorflow;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace isearch::config;
namespace isearch {
namespace turing {
AUTIL_LOG_SETUP(ha3, ParaSearchBiz);

ParaSearchBiz::ParaSearchBiz() {
}

ParaSearchBiz::~ParaSearchBiz() {
}

string ParaSearchBiz::getDefaultGraphDefPath() {
    const string &workDir = suez::PathDefine::getCurrentPath();
    string binaryPath = autil::EnvUtil::getEnv("binaryPath", "");
    if (binaryPath.empty()) {
        binaryPath = workDir + "/../binary";
    }

    return binaryPath + "/usr/local/etc/ha3/searcher_" + _bizName + ".def";
}

string ParaSearchBiz::getConfigZoneBizName(const string &zoneName) {
    return zoneName + ZONE_BIZ_NAME_SPLITTER + DEFAULT_BIZ_NAME;
}

string ParaSearchBiz::getOutConfName(const string &confName) {
    string resName = "para/"+ _bizName + confName;
    return resName;
}

int ParaSearchBiz::getInterOpThreadPool() {
    return 0;
}

void ParaSearchBiz::patchTuringOptionsInfo(
        const isearch::config::TuringOptionsInfo &turingOptionsInfo,
        JsonMap &jsonMap)
{
    // ignore other options
    auto const &iter = turingOptionsInfo._turingOptionsInfo.find("dependency_table");
    if (iter != turingOptionsInfo._turingOptionsInfo.end()) {
        jsonMap["dependency_table"] = iter->second;
    }

    auto const &iter2 = turingOptionsInfo._turingOptionsInfo.find("module_info");
    if (iter2 != turingOptionsInfo._turingOptionsInfo.end()) {
        jsonMap["module_info"] = iter2->second;
    }
}

} // namespace turing
} // namespace isearch
