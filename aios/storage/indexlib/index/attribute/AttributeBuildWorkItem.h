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
#include "indexlib/document/normal/AttributeDocument.h"
#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/index/attribute/SingleAttributeBuilder.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/BuildWorkItem.h"

namespace indexlib::index {

// User of work item should ensure lifetime of indexer and documentBatch out live the work item.
// TODO: As of 2023-01, PackAttribute does not support update in indexlibv2 yet.
template <typename DiskIndexerType, typename MemIndexerType>
class AttributeBuildWorkItem : public BuildWorkItem
{
public:
    AttributeBuildWorkItem(SingleAttributeBuilder<DiskIndexerType, MemIndexerType>* builder,
                           indexlibv2::document::IDocumentBatch* documentBatch);
    ~AttributeBuildWorkItem();

public:
    Status doProcess() override;

private:
    Status BuildOneDoc(indexlibv2::document::IDocument* doc);

private:
    SingleAttributeBuilder<DiskIndexerType, MemIndexerType>* _builder;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE_2(indexlib.index, AttributeBuildWorkItem, T1, T2);

template <typename DiskIndexerType, typename MemIndexerType>
AttributeBuildWorkItem<DiskIndexerType, MemIndexerType>::AttributeBuildWorkItem(
    SingleAttributeBuilder<DiskIndexerType, MemIndexerType>* builder,
    indexlibv2::document::IDocumentBatch* documentBatch)
    : BuildWorkItem(("_ATTRIBUTE_") + builder->GetIndexName(), BuildWorkItem::BuildWorkItemType::ATTRIBUTE,
                    documentBatch)
    , _builder(builder)

{
}

template <typename DiskIndexerType, typename MemIndexerType>
AttributeBuildWorkItem<DiskIndexerType, MemIndexerType>::~AttributeBuildWorkItem()
{
}

template <typename DiskIndexerType, typename MemIndexerType>
Status AttributeBuildWorkItem<DiskIndexerType, MemIndexerType>::doProcess()
{
    auto iter = indexlibv2::document::DocumentIterator<indexlibv2::document::IDocument>::Create(_documentBatch);
    while (iter->HasNext()) {
        std::shared_ptr<indexlibv2::document::IDocument> doc = iter->Next();
        auto st = BuildOneDoc(doc.get());
        if (!st.IsOK()) {
            AUTIL_LOG(ERROR, "build one doc failed, docId[%d] [%s]", doc->GetDocId(), st.ToString().c_str());
        }
    }
    return Status::OK();
}

template <typename DiskIndexerType, typename MemIndexerType>
Status AttributeBuildWorkItem<DiskIndexerType, MemIndexerType>::BuildOneDoc(indexlibv2::document::IDocument* iDoc)
{
    DocOperateType opType = iDoc->GetDocOperateType();
    assert(opType != UNKNOWN_OP);
    if (opType == ADD_DOC) {
        return _builder->AddDocument(iDoc);
    }
    if (opType != UPDATE_FIELD) {
        return Status::OK();
    }
    RETURN_STATUS_DIRECTLY_IF_ERROR(_builder->UpdateDocument(iDoc));
    return Status::OK();
}

} // namespace indexlib::index
