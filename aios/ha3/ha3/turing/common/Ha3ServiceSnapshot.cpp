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
#include "ha3/turing/common/Ha3ServiceSnapshot.h"

namespace isearch {
namespace turing {

AUTIL_LOG_SETUP(ha3, Ha3ServiceSnapshot);

Ha3ServiceSnapshot::Ha3ServiceSnapshot() {}
Ha3ServiceSnapshot::~Ha3ServiceSnapshot() {}

void Ha3ServiceSnapshot::setBasicTuringBizNames(const std::set<std::string> &bizNames) {
    _basicTuringBizNames = bizNames;
}

Ha3BizMeta *Ha3ServiceSnapshot::getHa3BizMeta() {
    return &_ha3BizMeta;
}

bool Ha3ServiceSnapshot::collectCatalogInfo() {
    if (_bizMap.empty()) {
        AUTIL_LOG(INFO, "biz is empty, skip collect catalog info");
        return true;
    }

    for (auto iter : _bizMap) {
        auto &biz = iter.second;
        AUTIL_LOG(INFO, "try to add [%s] user metadata", iter.first.c_str());
        if (!_ha3BizMeta.add(biz->getUserMetadata())) {
            AUTIL_LOG(ERROR, "add [%s] user metadata failed", iter.first.c_str());
            return false;
        }
    }
    _catalogInfo = _ha3BizMeta.generatorCatalogInfo();
    AUTIL_LOG(DEBUG, "generator catalog info [%s]",
              autil::legacy::ToJsonString(*_catalogInfo).c_str());
    return true;
}

} // namespace turing
} // namespace isearch
