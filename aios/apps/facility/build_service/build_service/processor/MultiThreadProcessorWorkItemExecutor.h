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

#include <atomic>
#include <stdint.h>

#include "autil/OutputOrderedThreadPool.h"
#include "build_service/common_define.h"
#include "build_service/processor/ProcessorWorkItem.h"
#include "build_service/processor/ProcessorWorkItemExecutor.h"
#include "build_service/util/Log.h"

namespace build_service { namespace processor {

class MultiThreadProcessorWorkItemExecutor : public ProcessorWorkItemExecutor
{
public:
    MultiThreadProcessorWorkItemExecutor(uint32_t threadNum, uint32_t queueSize);
    ~MultiThreadProcessorWorkItemExecutor();

private:
    MultiThreadProcessorWorkItemExecutor(const MultiThreadProcessorWorkItemExecutor&);
    MultiThreadProcessorWorkItemExecutor& operator=(const MultiThreadProcessorWorkItemExecutor&);

public:
    bool start() override;
    bool push(ProcessorWorkItem* workItem) override;
    ProcessorWorkItem* pop() override;
    void stop(bool instant) override;
    uint32_t getWaitItemCount() override;
    uint32_t getOutputItemCount() override;

private:
    autil::OutputOrderedThreadPoolPtr _processThreadPool;
    std::atomic<bool> _stopped;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(MultiThreadProcessorWorkItemExecutor);

}} // namespace build_service::processor
