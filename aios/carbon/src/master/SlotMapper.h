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
#ifndef CARBON_SLOTMAPPER_H
#define CARBON_SLOTMAPPER_H

#include "carbon/CommonDefine.h"
#include "common/common.h"
#include "carbon/Log.h"
#include "master/WorkerNode.h"

BEGIN_CARBON_NAMESPACE(master);

class SlotMapper
{
public:
    SlotMapper();
    ~SlotMapper();
private:
    SlotMapper(const SlotMapper &);
    SlotMapper& operator=(const SlotMapper &);
public:
    static void mapping(const SlotInfoMap &slotInfos,
                        const std::map<tag_t, WorkerNodeVect> &workerNodes);
    
    static void mapping(const SlotInfoMap &slotInfos,
                        const WorkerNodeVect &workerNodes);
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(SlotMapper);

END_CARBON_NAMESPACE(master);

#endif //CARBON_SLOTMAPPER_H
