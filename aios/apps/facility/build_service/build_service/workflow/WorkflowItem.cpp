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
#include "build_service/workflow/WorkflowItem.h"

#include <assert.h>
#include <string>
#include <typeinfo>
#include <unistd.h>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"

namespace build_service { namespace workflow {
BS_LOG_SETUP(workflow, WorkflowItemBase);

using StopOption::SO_NORMAL;

WorkflowItemBase::WorkflowItemBase(ProducerBase* producer, ConsumerBase* consumer)
    : _running(false)
    , _finished(false)
    , _suspended(false)
    , _hasFatalError(false)
    , _fe(FE_OK)
    , _status(WFS_STOPPED)
    , _stopOption(SO_NORMAL)
    , _producer(producer)
    , _consumer(consumer)
{
    BS_LOG(INFO, "WorkflowItemBase: Procucer[%s] --> Consumer[%s]", producer ? typeid(*producer).name() : "",
           consumer ? typeid(*consumer).name() : "");
}

WorkflowItemBase::~WorkflowItemBase()
{
    stop(SO_NORMAL);
    DELETE_AND_SET_NULL(_producer);
    DELETE_AND_SET_NULL(_consumer);
}

bool WorkflowItemBase::start(WorkflowMode mode)
{
    _running = true;
    _status = WFS_START;
    _mode = mode;
    return true;
}

void WorkflowItemBase::stop(StopOption stopOption)
{
    {
        autil::ScopedLock lk(_stopMtx);
        if (!_running && _status == WFS_STOPPED) {
            BS_LOG(INFO, "workflow already stop.");
            return;
        }

        BS_LOG(INFO, "stop workflow begin.");
        _stopOption = stopOption;
        _running = false;
        _producer->notifyStop(stopOption);
        _consumer->notifyStop(stopOption);
    }

    while (_status != WFS_STOPPED) {
        usleep(WAIT_STOP_SLEEP_TIME);
    }
    BS_LOG(INFO, "stop workflow end.");
}

void WorkflowItemBase::drop()
{
    BS_LOG(INFO, "drop WorkflowItem, status[%d]", _status);
    assert(!_running);
    _status = WFS_STOPPING;
    if (processFinished() && !_consumer->getLocator(_stopLocator)) {
        std::string errorMsg = "getLocator from consumer" + std::string(typeid(*_producer).name()) + " failed!";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        _hasFatalError = true;
    }
    if (!_producer->stop(_stopOption)) {
        std::string errorMsg = "stop producer " + std::string(typeid(*_producer).name()) + " failed!";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        _hasFatalError = true;
    }
    if (!_consumer->stop(_fe, _stopOption)) {
        std::string errorMsg = "stop consumer " + std::string(typeid(*_producer).name()) + " failed!";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        _hasFatalError = true;
    }
    _finished = !_hasFatalError && processFinished();
    BS_LOG(INFO, "End WorkflowItem, producer: %s consumer: %s.", typeid(*_producer).name(), typeid(*_consumer).name());
    _status = WFS_STOPPED;
}

void WorkflowItemBase::handleError(FlowError fe)
{
    assert(FE_OK != fe);
    if (FE_EOF == fe) {
        BS_LOG(INFO, "EOF got, exit now");
        _running = false;
    } else if (FE_SEALED == fe) {
        BS_LOG(INFO, "SEALED got, exit now");
        _running = false;
    } else if (FE_DROPFLOW == fe) {
        BS_LOG(INFO, "DROPFLOW got, exit now");
        _running = false;
        _stopOption = SO_INSTANT;
    } else if (FE_FATAL == fe) {
        std::string errorMsg = "fatal error happened! exit";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        _hasFatalError = true;
        _stopOption = SO_INSTANT;
        _running = false;
    } else if (FE_EXCEPTION == fe) {
        std::string errorMsg = "exception happened! exit";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        _hasFatalError = true;
        _stopOption = SO_INSTANT;
        _running = false;
    } else if (FE_RECONSTRUCT == fe) {
        BS_LOG(INFO, "need reconstruct build flow");
        _stopOption = SO_INSTANT;
        _running = false;
    }
}

}} // namespace build_service::workflow
