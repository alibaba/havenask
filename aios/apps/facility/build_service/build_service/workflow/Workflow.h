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
#ifndef ISEARCH_BS_WORKFLOW_H
#define ISEARCH_BS_WORKFLOW_H

#include <functional>
#include <unistd.h>

#include "autil/Lock.h"
#include "autil/Thread.h"
#include "build_service/common_define.h"
#include "build_service/document/RawDocument.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/StopOption.h"
#include "build_service/workflow/WorkflowItem.h"
#include "build_service/workflow/WorkflowThreadPool.h"

namespace build_service { namespace document {

class ProcessedDocument;
BS_TYPEDEF_PTR(ProcessedDocument);
typedef std::vector<ProcessedDocumentPtr> ProcessedDocumentVec;
BS_TYPEDEF_PTR(ProcessedDocumentVec);

}} // namespace build_service::document

namespace build_service { namespace workflow {

class Workflow
{
public:
    Workflow(ProducerBase* producer, ConsumerBase* consumer);
    Workflow(WorkflowItemBase* item);

    Workflow(Workflow&& workflow);
    ~Workflow();
    Workflow(const Workflow&) = delete;
    Workflow& operator=(const Workflow&) = delete;

    template <typename T>
    static Workflow create(ProducerBase* producer, ConsumerBase* consumer)
    {
        WorkflowItemBase* item = new WorkflowItem<T>(producer, consumer);
        return Workflow(item);
    }

private:
    static WorkflowItemBase* createItem(ProducerBase* producer, ConsumerBase* consumer)
    {
        if (dynamic_cast<ProcessedDocProducer*>(producer) && dynamic_cast<ProcessedDocConsumer*>(consumer)) {
            return new WorkflowItem<document::ProcessedDocumentPtr>(producer, consumer);
        }
        if (dynamic_cast<RawDocProducer*>(producer) && dynamic_cast<RawDocConsumer*>(consumer)) {
            return new WorkflowItem<document::RawDocumentPtr>(producer, consumer);
        }
        assert(false);
        return nullptr;
    }

public:
    bool start(WorkflowMode mode = JOB, const WorkflowThreadPoolPtr& pool = WorkflowThreadPoolPtr());
    void stop(StopOption stopOption);
    bool isFinished() const;
    bool hasFatalError() const;
    bool needReconstruct() const;

    ProducerBase* getProducer() const { return _workflowItem->getProducer(); }
    ConsumerBase* getConsumer() const { return _workflowItem->getConsumer(); }
    void suspend() { _workflowItem->suspend(); }
    void resume() { _workflowItem->resume(); }

    bool isSuspended() const { return _workflowItem->isSuspended(); }
    bool isRunning() const { return _workflowItem->isRunning(); }

    const common::Locator& getStopLocator() const { return _workflowItem->getStopLocator(); }

private:
    WorkflowItemBase* _workflowItem;
    WorkflowThreadPoolPtr _threadPool;

private:
    BS_LOG_DECLARE();
};
//////////////////////////////////////////////
}} // namespace build_service::workflow

#endif // ISEARCH_BS_WORKFLOW_H
