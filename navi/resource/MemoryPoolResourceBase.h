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
#ifndef NAVI_MEMORYPOOLRESOURCEBASE_H
#define NAVI_MEMORYPOOLRESOURCEBASE_H

#include "autil/Lock.h"
#include "navi/engine/Resource.h"
#include <arpc/common/LockFreeQueue.h>

namespace navi {

class MemoryPoolResourceBase : public Resource
{
public:
    MemoryPoolResourceBase();
    ~MemoryPoolResourceBase();
private:
    MemoryPoolResourceBase(const MemoryPoolResourceBase &);
    MemoryPoolResourceBase &operator=(const MemoryPoolResourceBase &);
public:
    size_t getPoolFromCache(autil::mem_pool::Pool *&pool);
    size_t putPoolToCache(autil::mem_pool::Pool *pool);
protected:
    arpc::common::LockFreeQueue<autil::mem_pool::Pool *> _poolQueue;
};

}

#endif //NAVI_MEMORYPOOLRESOURCEBASE_H
