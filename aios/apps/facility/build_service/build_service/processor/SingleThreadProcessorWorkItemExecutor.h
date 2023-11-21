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

#include <algorithm>
#include <memory>
#include <stdint.h>

#include "build_service/common_define.h"
#include "build_service/processor/ProcessorWorkItem.h"
#include "build_service/processor/ProcessorWorkItemExecutor.h"
#include "build_service/util/Log.h"
#include "build_service/util/StreamQueue.h"

namespace build_service { namespace processor {

class SingleThreadProcessorWorkItemExecutor : public ProcessorWorkItemExecutor
{
public:
    SingleThreadProcessorWorkItemExecutor(uint32_t queueSize);
    ~SingleThreadProcessorWorkItemExecutor();

private:
    SingleThreadProcessorWorkItemExecutor(const SingleThreadProcessorWorkItemExecutor&);
    SingleThreadProcessorWorkItemExecutor& operator=(const SingleThreadProcessorWorkItemExecutor&);

public:
    bool start() override;
    bool push(ProcessorWorkItem* workItem) override;
    ProcessorWorkItem* pop() override;
    void stop(bool instant) override;
    uint32_t getWaitItemCount() override;
    uint32_t getOutputItemCount() override;

private:
    typedef util::StreamQueue<ProcessorWorkItem*> Queue;
    typedef std::unique_ptr<Queue> QueuePtr;

    bool _running;
    QueuePtr _queue;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SingleThreadProcessorWorkItemExecutor);

}} // namespace build_service::processor
