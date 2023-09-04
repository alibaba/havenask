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
#include "navi/ops/ResourceData.h"
#include "navi/util/CommonUtil.h"

namespace navi {

ResourceDataType::ResourceDataType()
    : Type(__FILE__, RESOURCE_DATA_TYPE_ID)
{
}

REGISTER_TYPE(ResourceDataType);

ResourceData::ResourceData()
    : Data(RESOURCE_DATA_TYPE_ID)
    , _require(true)
    , _wait(false)
    , _stage(RS_UNKNOWN)
    , _ec(EC_NONE)
{
}

bool ResourceData::validate(bool inputRequire) const {
    if (_resource) {
        return true;
    }
    auto realRequire = _require || inputRequire;
    if (!realRequire) {
        return true;
    }
    if (_wait) {
        return true;
    }
    if (EC_NEED_REPLACE == _ec) {
        return true;
    }
    if (RS_EXTERNAL == _stage) {
        NAVI_KERNEL_LOG(ERROR,
                        "lack resource [%s] stage [%s] ec [%s]",
                        _name.c_str(),
                        ResourceStage_Name(_stage).c_str(),
                        CommonUtil::getErrorString(_ec));
        return false;
    }
    if (RS_RUN_GRAPH_EXTERNAL == _stage) {
        return true;
    }
    if (_stage < RS_RUN_GRAPH_EXTERNAL) {
        validateDepend();
        NAVI_KERNEL_LOG(ERROR,
                        "lack resource [%s] stage [%s] ec [%s]",
                        _name.c_str(),
                        ResourceStage_Name(_stage).c_str(),
                        CommonUtil::getErrorString(_ec));
        return false;
    }
    if (RS_KERNEL == _stage) {
        return validateDepend();
    }
    if (EC_LACK_RESOURCE_GRAPH == _ec || EC_LACK_RESOURCE_SUB_GRAPH == _ec) {
        return validateDepend();
    }
    if (!(EC_LACK_RESOURCE_EXTERNAL == _ec || EC_BIND_NAMED_DATA == _ec)) {
        validateDepend();
        NAVI_KERNEL_LOG(ERROR,
                        "lack resource [%s] stage [%s] ec [%s]",
                        _name.c_str(),
                        ResourceStage_Name(_stage).c_str(),
                        CommonUtil::getErrorString(_ec));
        return false;
    }
    return true;
}

bool ResourceData::validateDepend() const {
    if (!_dependMap) {
        NAVI_KERNEL_LOG(ERROR, "null depend map");
        return false;
    }
    for (const auto &pair : *_dependMap) {
        const auto &dependName = pair.first;
        auto require = pair.second;
        auto it = _dependDatas.find(dependName);
        if (_dependDatas.end() == it) {
            NAVI_KERNEL_LOG(ERROR,
                            "lack depend resource [%s], depend by [%s] require [1] stage [%s] ec [%s]",
                            dependName.c_str(),
                            _name.c_str(),
                            ResourceStage_Name(_stage).c_str(),
                            CommonUtil::getErrorString(_ec));
            return false;
        }
        if (!require) {
            continue;
        }
        if (!it->second->validate(true)) {
            NAVI_KERNEL_LOG(ERROR,
                            "lack depend resource [%s], depend by [%s] require [1] stage [%s] ec [%s]",
                            dependName.c_str(),
                            _name.c_str(),
                            ResourceStage_Name(_stage).c_str(),
                            CommonUtil::getErrorString(_ec));
            return false;
        }
    }
    return validateReplace();
}

bool ResourceData::validateReplace() const {
    if (!_replaceSet) {
        return true;
    }
    for (const auto &replaceName : *_replaceSet) {
        auto it = _dependDatas.find(replaceName);
        if (_dependDatas.end() == it) {
            NAVI_KERNEL_LOG(WARN,
                            "replacer [%s] not found in replace map, stage [%s] ec [%s]",
                            replaceName.c_str(),
                            ResourceStage_Name(_stage).c_str(),
                            CommonUtil::getErrorString(_ec));
            return false;
        }
        if (!it->second->validate(true)) {
            NAVI_KERNEL_LOG(WARN,
                            "lack replacer resource [%s] to replace [%s], stage [%s] ec [%s]",
                            replaceName.c_str(),
                            _name.c_str(),
                            ResourceStage_Name(_stage).c_str(),
                            CommonUtil::getErrorString(_ec));
            return false;
        }
    }
    return true;
}

}

