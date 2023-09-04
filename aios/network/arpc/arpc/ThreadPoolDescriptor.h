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

#include <string>

#include "aios/autil/autil/ThreadPool.h"
#include "aios/network/arpc/arpc/CommonMacros.h"

ARPC_BEGIN_NAMESPACE(arpc)

/**
 * @ingroup ServerClasses
 * Descriptor of a thread pool.
 * Each thread pool will be defined with a pool name, thread
 * number that the pool can hold and the work item queue limit.
 */
class ThreadPoolDescriptor {
public:
    ThreadPoolDescriptor() : threadPoolName(DEFAULT_TREAHDPOOL_NAME) {
        threadNum = 0;
        queueSize = 0;
        _allowSharedPoolOverride = true;
    }

    ThreadPoolDescriptor(std::string thPoolName, size_t threadNum, size_t queueSize) : threadPoolName(thPoolName) {
        this->threadNum = threadNum;
        this->queueSize = queueSize;
    }

    ThreadPoolDescriptor(std::string thPoolName,
                         size_t threadNum,
                         size_t queueSize,
                         autil::WorkItemQueueFactoryPtr factory)
        : threadPoolName(thPoolName) {
        this->threadNum = threadNum;
        this->queueSize = queueSize;
        this->factory = factory;
    }

    void allowSharedPoolOverride() { _allowSharedPoolOverride = true; }

    std::string threadPoolName;
    size_t threadNum;
    size_t queueSize;
    autil::WorkItemQueueFactoryPtr factory;
    bool _allowSharedPoolOverride = false;
};

ARPC_END_NAMESPACE(arpc)