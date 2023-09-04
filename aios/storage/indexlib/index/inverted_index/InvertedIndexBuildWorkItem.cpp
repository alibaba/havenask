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
#include "indexlib/index/inverted_index/InvertedIndexBuildWorkItem.h"

#include "indexlib/document/DocumentIterator.h"
namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, InvertedIndexBuildWorkItem);

InvertedIndexBuildWorkItem::InvertedIndexBuildWorkItem(SingleInvertedIndexBuilder* builder,
                                                       indexlibv2::document::IDocumentBatch* documentBatch)
    : BuildWorkItem(("_INVERTED_INDEX_") + builder->GetIndexName(), BuildWorkItemType::INVERTED_INDEX, documentBatch)
    , _builder(builder)
{
}

InvertedIndexBuildWorkItem::~InvertedIndexBuildWorkItem() {}

Status InvertedIndexBuildWorkItem::doProcess()
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

Status InvertedIndexBuildWorkItem::BuildOneDoc(indexlibv2::document::IDocument* iDoc)
{
    DocOperateType opType = iDoc->GetDocOperateType();
    assert(opType != UNKNOWN_OP);
    if (opType == ADD_DOC) {
        return _builder->AddDocument(iDoc);
    }
    if (opType != UPDATE_FIELD) {
        return Status::OK();
    }
    return _builder->UpdateDocument(iDoc);
}

} // namespace indexlib::index
