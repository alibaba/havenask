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
#ifndef CARBON_GLOBALCONFIG_H
#define CARBON_GLOBALCONFIG_H

#include "common/common.h"
#include "autil/legacy/jsonizable.h"

BEGIN_CARBON_NAMESPACE(master);

JSONIZABLE_CLASS(RouterRatioModel) {
public:
     RouterRatioModel() : targetRatio(-1), srcRatio(100) {}
    
     JSONIZE() {
        JSON_FIELD(group);
        JSON_FIELD(targetRatio);
        JSON_FIELD(srcRatio);
    }
    std::string group;  //名字待定,支持正则匹配
    int32_t targetRatio; // [0, 100], -1 disable  role中k8s节点比例. default:0
    int32_t srcRatio; // [0, 100], -1 disable  role中carbon2节点的比例. default:100
 
};

JSONIZABLE_CLASS(RouterConfig) {
public:
    RouterConfig() : targetRatio(-1), srcRatio(100) {}

    JSONIZE() {
        JSON_FIELD(starRouterList);
        JSON_FIELD(targetRatio);
        JSON_FIELD(srcRatio);
    }

    std::vector<RouterRatioModel> starRouterList;
    int32_t targetRatio; // [0, 100], -1 disable  role中k8s节点比例. default:0
    int32_t srcRatio; // [0, 100], -1 disable  role中carbon2节点的比例. default:100
};

JSONIZABLE_CLASS(GlobalConfig) {
public:
    JSONIZE() {
        JSON_FIELD(router);
    }

    RouterConfig router;
};

END_CARBON_NAMESPACE(master);

#endif //CARBON_GLOBALCONFIG_H
