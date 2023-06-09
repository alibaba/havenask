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
#ifndef NAVI_GRAPHMEMORYPOOLRESOURCE_H
#define NAVI_GRAPHMEMORYPOOLRESOURCE_H

#include "kmonitor/client/MetricsReporter.h"
#include "navi/resource/MemoryPoolResourceBase.h"

namespace navi {

class MemoryPoolResource;

class GraphMemoryPoolResource
    : public MemoryPoolResourceBase,
      public std::enable_shared_from_this<GraphMemoryPoolResource>
{
public:
    GraphMemoryPoolResource();
    ~GraphMemoryPoolResource();
    GraphMemoryPoolResource(const GraphMemoryPoolResource &) = delete;
    GraphMemoryPoolResource &operator=(const GraphMemoryPoolResource &) = delete;
public:
    void def(ResourceDefBuilder &builder) const override;
    bool config(ResourceConfigContext &ctx) override;
    ErrorCode init(ResourceInitContext &ctx) override;
public:
    std::shared_ptr<autil::mem_pool::Pool> getPool();
private:
    void putPool(autil::mem_pool::Pool *pool);
    autil::mem_pool::Pool *getPoolInner();
private:
    MemoryPoolResource *_memoryPoolResource = nullptr;
    kmonitor::MetricsReporter *_graphMemoryPoolReporter = nullptr;
};

NAVI_TYPEDEF_PTR(GraphMemoryPoolResource);

}

#endif //NAVI_GRAPHMEMORYPOOLRESOURCE_H
