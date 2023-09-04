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
#ifndef CARBON_MASTER_FLAG_H
#define CARBON_MASTER_FLAG_H
 
#include "common/common.h"
#include "carbon/Log.h"
#include "carbon/CommonDefine.h"

BEGIN_CARBON_NAMESPACE(master);

struct RouterBaseConf {
    std::string host;
    bool single; // single role in 1 group
    bool useJsonEncode; 

    RouterBaseConf() : single(false) {}
};

class Flag 
{
public:
    // don't remove nodes on name services
    static bool isSilentUnpublish(int type);
    static RouterBaseConf getRouterConf();
    static bool getAllK8s();
    static bool isTestPublish();
    static bool isSlotOnC2();
};

END_CARBON_NAMESPACE(master);

#endif
