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

#include "kmonitor/client/MetricsReporter.h"
#include "navi/resource/MemoryPoolR.h"

namespace navi {

class GraphMemoryPoolR : public MemoryPoolRBase {
public:
    GraphMemoryPoolR();
    ~GraphMemoryPoolR();
    GraphMemoryPoolR(const GraphMemoryPoolR &) = delete;
    GraphMemoryPoolR &operator=(const GraphMemoryPoolR &) = delete;
public:
    void def(ResourceDefBuilder &builder) const override;
    bool config(ResourceConfigContext &ctx) override;
    ErrorCode init(ResourceInitContext &ctx) override;
public:
    static const std::string RESOURCE_ID;
public:
    std::shared_ptr<autil::mem_pool::Pool> getPool();
private:
    void putPool(autil::mem_pool::Pool *pool);
    autil::mem_pool::Pool *getPoolInner();
private:
    RESOURCE_DEPEND_DECLARE();
private:
    RESOURCE_DEPEND_ON(MemoryPoolR, _memoryPoolR);
    kmonitor::MetricsReporter *_graphMemoryPoolReporter = nullptr;
};

NAVI_TYPEDEF_PTR(GraphMemoryPoolR);

}
