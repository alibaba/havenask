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

#include <stdint.h>

#include "autil/Diagnostic.h"
#include "autil/Lock.h"
#include "build_service/common/Locator.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/Consumer.h"
#include "build_service/workflow/FlowError.h"
#include "build_service/workflow/Producer.h"
#include "build_service/workflow/StopOption.h"

namespace build_service { namespace workflow {

enum WorkflowStatus {
    WFS_START,
    WFS_STOPPING,
    WFS_STOPPED,
};

enum WorkflowMode {
    JOB = 0,
    SERVICE = 1,
    REALTIME = 2,
};

class WorkflowItemBase
{
public:
    WorkflowItemBase(ProducerBase* producer, ConsumerBase* consumer);
    virtual ~WorkflowItemBase();

public:
    virtual bool start(WorkflowMode mode = JOB);
    virtual void stop(StopOption stopOption);
    bool isSuspended() const { return _suspended; }
    bool isRunning() const { return _running; }
    FlowError getFlowError() const { return _fe; }
    void suspend() { _suspended = true; }
    void resume() { _suspended = false; }
    bool isFinished() const { return _finished; }
    bool hasFatalError() const { return _hasFatalError; }
    bool needReconstruct() const { return _fe == FE_RECONSTRUCT; }

    ProducerBase* getProducer() { return _producer; }
    ConsumerBase* getConsumer() { return _consumer; }
    const common::Locator& getStopLocator() const { return _stopLocator; }
    void handleError(FlowError fe);

public:
    virtual void process() = 0;
    virtual void drop();

private:
    bool processFinished() const { return _fe == FE_EOF || _fe == FE_SEALED || _fe == FE_DROPFLOW; }

protected:
    static const int64_t WAIT_STOP_SLEEP_TIME = 10 * 1000; // 10 ms
    volatile bool _running;
    volatile bool _finished;
    volatile bool _suspended;
    volatile bool _hasFatalError;
    volatile FlowError _fe;
    volatile WorkflowStatus _status;
    volatile StopOption _stopOption;
    autil::ThreadMutex _stopMtx;
    ProducerBase* _producer;
    ConsumerBase* _consumer;
    WorkflowMode _mode;
    common::Locator _stopLocator;

private:
    BS_LOG_DECLARE();
};

template <typename T>
class WorkflowItem : public WorkflowItemBase
{
public:
    WorkflowItem(ProducerBase* producer, ConsumerBase* consumer);
    ~WorkflowItem() {}

private:
    WorkflowItem(const WorkflowItem&);
    WorkflowItem& operator=(const WorkflowItem&);

public:
    void process() override;

private:
    BS_LOG_DECLARE();
};

BS_LOG_SETUP_TEMPLATE(workflowitem, WorkflowItem, T);

template <typename T>
WorkflowItem<T>::WorkflowItem(ProducerBase* producer, ConsumerBase* consumer) : WorkflowItemBase(producer, consumer)
{
}

#ifdef __clang__
DIAGNOSTIC_PUSH
DIAGNOSTIC_IGNORE("-Wdeprecated-volatile")
#endif
template <typename T>
void WorkflowItem<T>::process()
{
    T item;
    while (_running && (_fe = ((Producer<T>*)_producer)->produce(item)) != FE_OK) {
        handleError(_fe);
        return;
    }
    while (_running && (_fe = ((Consumer<T>*)_consumer)->consume(item)) != FE_OK) {
        handleError(_fe);
    }
}
#ifdef __clang__
DIAGNOSTIC_POP
#endif
}} // namespace build_service::workflow
