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
#include "build_service/processor/MultiThreadProcessorWorkItemExecutor.h"

using namespace std;

namespace build_service { namespace processor {
BS_LOG_SETUP(processor, MultiThreadProcessorWorkItemExecutor);

MultiThreadProcessorWorkItemExecutor::MultiThreadProcessorWorkItemExecutor(uint32_t threadNum, uint32_t queueSize)
    : _stopped(false)
{
    _processThreadPool.reset(new autil::OutputOrderedThreadPool(threadNum, queueSize));
}

MultiThreadProcessorWorkItemExecutor::~MultiThreadProcessorWorkItemExecutor()
{
    stop(/*instant*/ false);

    ProcessorWorkItem* item = pop();
    if (item != NULL) {
        BS_LOG(WARN, "some processor work itmes still in queue, drop them");
    }
    while (item != NULL) {
        DELETE_AND_SET_NULL(item);
        item = pop();
    }
}

bool MultiThreadProcessorWorkItemExecutor::start() { return _processThreadPool->start("BsProcess"); }

bool MultiThreadProcessorWorkItemExecutor::push(ProcessorWorkItem* workItem)
{
    return _processThreadPool->pushWorkItem(workItem);
}

ProcessorWorkItem* MultiThreadProcessorWorkItemExecutor::pop()
{
    return static_cast<ProcessorWorkItem*>(_processThreadPool->popWorkItem());
}

void MultiThreadProcessorWorkItemExecutor::stop(bool instant)
{
    if (_stopped) {
        return;
    }
    if (_processThreadPool) {
        auto stopType = autil::ThreadPool::STOP_AFTER_QUEUE_EMPTY;
        if (instant) {
            stopType = autil::ThreadPool::STOP_AND_CLEAR_QUEUE_IGNORE_EXCEPTION;
        }
        _processThreadPool->waitStop(stopType);
    }
    _stopped = true;
}

uint32_t MultiThreadProcessorWorkItemExecutor::getWaitItemCount() { return _processThreadPool->getOutputItemCount(); }

uint32_t MultiThreadProcessorWorkItemExecutor::getOutputItemCount() { return _processThreadPool->getOutputItemCount(); }

}} // namespace build_service::processor
