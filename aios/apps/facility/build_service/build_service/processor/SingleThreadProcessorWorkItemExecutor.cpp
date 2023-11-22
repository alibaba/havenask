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
#include "build_service/processor/SingleThreadProcessorWorkItemExecutor.h"

#include <cstddef>
#include <string>

#include "alog/Logger.h"

using namespace std;

namespace build_service { namespace processor {
BS_LOG_SETUP(processor, SingleThreadProcessorWorkItemExecutor);

SingleThreadProcessorWorkItemExecutor::SingleThreadProcessorWorkItemExecutor(uint32_t queueSize) : _running(false)
{
    _queue.reset(new Queue(queueSize));
}

SingleThreadProcessorWorkItemExecutor::~SingleThreadProcessorWorkItemExecutor()
{
    stop(/*instant*/ false);
    if (!_queue->empty()) {
        string errorMsg = "some processor work items still in queue, drop them";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
    }

    while (!_queue->empty()) {
        ProcessorWorkItem* workItem;
        if (_queue->pop(workItem)) {
            delete workItem;
        }
    }
}

bool SingleThreadProcessorWorkItemExecutor::start()
{
    _running = true;
    return true;
}

bool SingleThreadProcessorWorkItemExecutor::push(ProcessorWorkItem* workItem)
{
    if (!_running || workItem == NULL) {
        return false;
    }
    workItem->process();
    _queue->push(workItem);
    return true;
}

ProcessorWorkItem* SingleThreadProcessorWorkItemExecutor::pop()
{
    ProcessorWorkItem* item = NULL;
    if (_queue->pop(item)) {
        return item;
    }
    return NULL;
}

void SingleThreadProcessorWorkItemExecutor::stop(bool instant)
{
    _running = false;
    _queue->setFinish();
    if (instant) {
        _queue->clear();
    }
}

uint32_t SingleThreadProcessorWorkItemExecutor::getWaitItemCount() { return 0; }

uint32_t SingleThreadProcessorWorkItemExecutor::getOutputItemCount() { return _queue->size(); }

}} // namespace build_service::processor
