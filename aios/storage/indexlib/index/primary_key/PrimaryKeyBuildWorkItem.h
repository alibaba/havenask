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

#include "indexlib/index/common/BuildWorkItem.h"
#include "indexlib/index/primary_key/SinglePrimaryKeyBuilder.h"

namespace indexlib::index {
// User of work item should ensure lifetime of indexer and data out live the work item.
class PrimaryKeyBuildWorkItem : public BuildWorkItem
{
public:
    PrimaryKeyBuildWorkItem(ISinglePrimaryKeyBuilder* builder, indexlibv2::document::IDocumentBatch* documentBatch);
    ~PrimaryKeyBuildWorkItem();

public:
    Status doProcess() override;

private:
    ISinglePrimaryKeyBuilder* _builder;

private:
    AUTIL_LOG_DECLARE();
};

////////////////////////////////////////////////////////////////////////////////
AUTIL_LOG_SETUP(indexlib.index, PrimaryKeyBuildWorkItem);

PrimaryKeyBuildWorkItem::PrimaryKeyBuildWorkItem(ISinglePrimaryKeyBuilder* builder,
                                                 indexlibv2::document::IDocumentBatch* documentBatch)
    : BuildWorkItem(("_PRIMARY_KEY_"), BuildWorkItemType::PRIMARY_KEY, documentBatch)
    , _builder(builder)
{
}

PrimaryKeyBuildWorkItem::~PrimaryKeyBuildWorkItem() {}

Status PrimaryKeyBuildWorkItem::doProcess()
{
    for (size_t i = 0; i < _documentBatch->GetBatchSize(); ++i) {
        std::shared_ptr<indexlibv2::document::IDocument> iDoc = (*_documentBatch)[i];
        DocOperateType opType = iDoc->GetDocOperateType();
        assert(opType != UNKNOWN_OP);
        if (opType == ADD_DOC) {
            RETURN_STATUS_DIRECTLY_IF_ERROR(_builder->AddDocument(iDoc.get()));
        }
    }
    return Status::OK();
}

} // namespace indexlib::index
