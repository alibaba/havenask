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
#ifndef ISEARCH_BS_SRCWORKITEM_H
#define ISEARCH_BS_SRCWORKITEM_H

#include "autil/WorkItem.h"
#include "build_service/common_define.h"
#include "build_service/document/ProcessedDocument.h"
#include "build_service/processor/Processor.h"
#include "build_service/util/Log.h"
#include "build_service/util/StreamQueue.h"
#include "build_service/workflow/SourceOpConverter.h"
#include "build_service/workflow/SrcDataNode.h"
#include "indexlib/indexlib.h"

namespace build_service { namespace workflow {

class SrcWorkItem : autil::WorkItem
{
public:
    typedef util::StreamQueue<std::pair<document::ProcessedDocumentPtr, SourceOpConverter::SolvedDocType>>
        DocumentStreamQueue;

public:
    struct SrcWorkItemMetricGroup {
        SrcWorkItemMetricGroup()
            : processQpsMetric(NULL)
            , buildQueueSizeMetric(NULL)
            , skipQpsMetric(NULL)
            , exceptionQpsMetric(NULL)
        {
        }
        SrcWorkItemMetricGroup(indexlib::util::Metric* processQpsMetric_, indexlib::util::Metric* buildQueueSizeMetric_,
                               indexlib::util::Metric* skipQpsMetric_, indexlib::util::Metric* exceptionQpsMetric_)
        {
            processQpsMetric = processQpsMetric_;
            buildQueueSizeMetric = buildQueueSizeMetric_;
            skipQpsMetric = skipQpsMetric_;
            exceptionQpsMetric = exceptionQpsMetric_;
        }
        indexlib::util::Metric* processQpsMetric;
        indexlib::util::Metric* buildQueueSizeMetric;
        indexlib::util::Metric* skipQpsMetric;
        indexlib::util::Metric* exceptionQpsMetric;
    };

public:
    SrcWorkItem(document::ProcessedDocumentPtr doc, SrcDataNode* srcNode, SourceOpConverter* converter,
                DocumentStreamQueue* buildDocQueue, SrcWorkItemMetricGroup* metricGroup);

    ~SrcWorkItem();

private:
    SrcWorkItem(const SrcWorkItem&);
    SrcWorkItem& operator=(const SrcWorkItem&);

public:
    void process() override;
    void destroy() override;
    void drop() override;

private:
    bool runWithTry(std::function<bool(void)> func, uint64_t retryTime);

private:
    document::ProcessedDocumentPtr _doc;
    SrcDataNode* _srcNode;
    SourceOpConverter* _converter;
    DocumentStreamQueue* _buildDocQueue;
    SrcWorkItemMetricGroup* _metricGroup;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SrcWorkItem);

}} // namespace build_service::workflow

#endif // ISEARCH_BS_SRCWORKITEM_H
