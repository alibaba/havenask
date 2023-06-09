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
#include <algorithm>
#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

#include "alog/Logger.h"
#include "suez/sdk/IndexProvider.h"
#include "ha3/version.h"
#include "ha3/common/VersionCalculator.h"
#include "ha3/turing/common/Ha3BizBase.h"

using namespace suez;
using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace suez::turing;
using namespace isearch::common;

namespace isearch {
namespace turing {
AUTIL_LOG_SETUP(ha3, Ha3BizBase);

Ha3BizBase::Ha3BizBase() {
}

Ha3BizBase::~Ha3BizBase() {
}

int64_t Ha3BizBase::calcVersion() const {
    string fullVersionStr = _indexProvider != nullptr ? getFullVersionStr(_indexProvider) : "";
    return VersionCalculator::calcVersion(
        _workerParam.workerConfigVersion,
        getBizInfo()._versionConfig.getDataVersion(),
        getBizMeta().getRemoteConfigPath(), fullVersionStr);
}

uint32_t Ha3BizBase::getProtocolVersion() const {
    auto configProtocolVersion = getBizInfo()._versionConfig.getProtocolVersion();
    string versionStr = HA_PROTOCOL_VERSION;
    if (!configProtocolVersion.empty()) {
        versionStr += ":" + configProtocolVersion;
    }
    uint32_t protocolVersion = autil::HashAlgorithm::hashString(versionStr.c_str(), versionStr.size(), 0);
    AUTIL_LOG(INFO, "ha3 protocol version str: [%s], version [%u]",
            versionStr.c_str(), protocolVersion);
    return protocolVersion;
}


string Ha3BizBase::getFullVersionStr(const IndexProvider *indexProvider) const {
    string ret;
    const auto &tableReader = indexProvider->tableReader;
    try {
        for (const auto &pair : tableReader) {
            ret += FastToJsonString(pair.first.tableId);
        }
    } catch (const ExceptionBase &e) {
        ret = "default hash string";
        AUTIL_LOG(WARN, "getFullVersionStr exception [%s]", e.what());
    }
    return ret;
}

} // namespace turing
} // namespace isearch
