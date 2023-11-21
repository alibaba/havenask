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
#include "build_service/workflow/SrcWorkItem.h"

#include <assert.h>
#include <exception>
#include <memory>
#include <string>

#include "alog/Logger.h"
#include "build_service/config/SrcNodeConfig.h"
#include "build_service/util/Monitor.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Types.h"
#include "indexlib/document/document.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_builder.h"
#include "indexlib/partition/index_partition.h"

namespace build_service { namespace workflow {
BS_LOG_SETUP(workflow, SrcWorkItem);

SrcWorkItem::SrcWorkItem(document::ProcessedDocumentPtr doc, SrcDataNode* srcNode, SourceOpConverter* converter,
                         DocumentStreamQueue* buildDocQueue, SrcWorkItemMetricGroup* metricGroup)
    : WorkItem()
    , _doc(doc)
    , _srcNode(srcNode)
    , _converter(converter)
    , _buildDocQueue(buildDocQueue)
    , _metricGroup(metricGroup)
{
}

SrcWorkItem::~SrcWorkItem() {}

bool SrcWorkItem::runWithTry(std::function<bool(void)> func, uint64_t retryTime)
{
    for (uint32_t i = 0; i < retryTime; ++i) {
        try {
            if (func()) {
                return true;
            }
        } catch (const std::exception& e) {
            INCREASE_QPS(_metricGroup->exceptionQpsMetric);
        }
    }
    drop();
    return false;
}

#define FINISH_DOC(doc, op)                                                                                            \
    do {                                                                                                               \
        _buildDocQueue->push(std::make_pair(doc, op));                                                                 \
        REPORT_METRIC(_metricGroup->buildQueueSizeMetric, _buildDocQueue->size());                                     \
    } while (0)

void SrcWorkItem::process()
{
    config::SrcNodeConfig* config = _srcNode->getConfig();
    assert(config);
    uint64_t retryTime = config->exceptionRetryTime;
    indexlib::document::DocumentPtr document =
        std::dynamic_pointer_cast<indexlib::document::Document>(_doc->getDocument());
    if (!document) {
        FINISH_DOC(_doc, SourceOpConverter::SDT_PROCESSED_DOC);
        return;
    }
    auto reader = _srcNode->getReader();
    if (!reader) {
        BS_LOG(ERROR, "get reader failed");
        FINISH_DOC(_doc, SourceOpConverter::SDT_FAILED_DOC);
        return;
    }
    docid_t docid = INVALID_DOCID;

    // getPk
    if (!runWithTry(
            [&reader, &docid, &document, srcNode = _srcNode]() mutable {
                docid = srcNode->search(reader, document->GetPrimaryKey());
                return true;
            },
            retryTime)) {
        BS_LOG(ERROR, "search doc [%s] failed", document->GetPrimaryKey().c_str());
        return;
    }
    auto originalOp = document->GetDocOperateType();
    if (config->enableOrderPreserving && docid != INVALID_DOCID) {
        bool skiped = false;
        if (!runWithTry(
                [&reader, &skiped, &docid, &document, srcNode = _srcNode]() mutable {
                    if (!srcNode->isDocNewerThanIndex(reader, docid, document)) {
                        skiped = true;
                        document->ModifyDocOperateType(SKIP_DOC);
                    }
                    return true;
                },
                retryTime)) {
            BS_LOG(ERROR, "check doc [%s] preserving order failed", document->GetPrimaryKey().c_str());
            return;
        }
        if (skiped) {
            INCREASE_QPS(_metricGroup->skipQpsMetric);
            FINISH_DOC(_doc, SourceOpConverter::SDT_ORDER_PRESERVE_FAILED);
            return;
        }
    }
    if (config->enableUpdateToAdd) {
        if (originalOp == UPDATE_FIELD) {
            if (docid == INVALID_DOCID) {
                BS_LOG(ERROR, "doc [%s] update to add failed as doc not exist in index",
                       document->GetPrimaryKey().c_str());
                document->ModifyDocOperateType(SKIP_DOC);
                INCREASE_QPS(_metricGroup->skipQpsMetric);
                FINISH_DOC(_doc, SourceOpConverter::SDT_UPDATE_FAILED);
                return;
            }
            if (!runWithTry(
                    [&reader, &docid, &document, srcNode = _srcNode]() mutable {
                        bool needRetry = false;
                        if (!srcNode->updateSrcDoc(reader, docid, document.get(), needRetry)) {
                            document->ModifyDocOperateType(SKIP_DOC);
                            if (needRetry) {
                                return false;
                            }
                            BS_LOG(ERROR, "doc [%s] update to add failed as source doc merge failed",
                                   document->GetPrimaryKey().c_str());
                            return true;
                        }
                        document->ModifyDocOperateType(ADD_DOC);
                        return true;
                    },
                    retryTime)) {
                BS_LOG(ERROR, "doc [%s] update to add failed", document->GetPrimaryKey().c_str());
                FINISH_DOC(_doc, _converter->convert(originalOp));
                return;
            }
        }
    }
    INCREASE_QPS(_metricGroup->processQpsMetric);
    FINISH_DOC(_doc, _converter->convert(originalOp));
}

void SrcWorkItem::drop()
{
    BS_LOG(ERROR, "exception happened when process doc pk = [%s]",
           std::dynamic_pointer_cast<indexlib::document::Document>(_doc->getDocument())->GetPrimaryKey().c_str());
    FINISH_DOC(_doc, SourceOpConverter::SDT_FAILED_DOC);
}

void SrcWorkItem::destroy() { delete this; }
}} // namespace build_service::workflow
