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
#include "ha3/queryparser/LayerParser.h"

#include <stdint.h>
#include <limits>
#include <memory>
#include <vector>

#include "alog/Logger.h"
#include "autil/StringUtil.h"

#include "ha3/common/LayerDescription.h"
#include "ha3/common/QueryLayerClause.h"
#include "ha3/isearch.h"
#include "autil/Log.h"

namespace isearch {
namespace queryparser {
AUTIL_LOG_SETUP(ha3, LayerParser);

LayerParser::LayerParser() { 
}

LayerParser::~LayerParser() { 
}

common::QueryLayerClause *LayerParser::createLayerClause() {
    return new common::QueryLayerClause;
}

common::LayerDescription *LayerParser::createLayerDescription() {
    return new common::LayerDescription;
}

void LayerParser::setQuota(common::LayerDescription *layerDesc, const std::string &quotaStr) {
    uint32_t quota = 0;
    const std::vector<std::string>& strVec = autil::StringUtil::split(quotaStr, "#");
    if(strVec.size() > 0) {
        if (strVec[0] == "UNLIMITED") {
            quota = std::numeric_limits<uint32_t>::max();
        } else {
            if (!autil::StringUtil::fromString(strVec[0].c_str(), quota)) {
                AUTIL_LOG(WARN, "Invalid quota string: [%s].", strVec[0].c_str());
                return;
            }
        }
        layerDesc->setQuota(quota);
        if(2 == strVec.size()) {
            uint32_t quotaType = 0;
            if (!autil::StringUtil::fromString(strVec[1].c_str(), quotaType)) {
                AUTIL_LOG(WARN, "Invalid quota string: [%s], use default 0.", strVec[1].c_str());
                quotaType = 0;
            }
            if(quotaType >= QT_UNKNOW) {
                AUTIL_LOG(WARN, "Invalid quota string: [%s], use default 0.", strVec[1].c_str());
                quotaType = 0;
            }
            layerDesc->setQuotaType((QuotaType)quotaType);
        }
    }
}


} // namespace queryparser
} // namespace isearch

