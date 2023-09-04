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

#include "indexlib/document/DocumentIterator.h"
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
    auto iter = indexlibv2::document::DocumentIterator<indexlibv2::document::IDocument>::Create(_documentBatch);
    while (iter->HasNext()) {
        std::shared_ptr<indexlibv2::document::IDocument> doc = iter->Next();
        DocOperateType opType = doc->GetDocOperateType();
        assert(opType != UNKNOWN_OP);
        if (opType == ADD_DOC) {
            auto st = _builder->AddDocument(doc.get());
            if (!st.IsOK()) {
                AUTIL_LOG(ERROR, "build one doc failed, docId[%d] [%s]", doc->GetDocId(), st.ToString().c_str());
            }
        }
    }
    return Status::OK();
}

} // namespace indexlib::index
