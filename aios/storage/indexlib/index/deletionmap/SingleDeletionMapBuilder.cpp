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
#include "indexlib/index/deletionmap/SingleDeletionMapBuilder.h"

#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/index/deletionmap/DeletionMapMemIndexer.h"
namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, SingleDeletionMapBuilder);

SingleDeletionMapBuilder::SingleDeletionMapBuilder() {}

SingleDeletionMapBuilder::~SingleDeletionMapBuilder() {}

Status SingleDeletionMapBuilder::AddDocument(indexlibv2::document::IDocument* doc)
{
    assert(doc->GetDocOperateType() == ADD_DOC);
    auto normalDoc = dynamic_cast<indexlibv2::document::NormalDocument*>(doc);
    assert(normalDoc != nullptr);
    docid_t deleteDocId = normalDoc->GetDeleteDocId();
    if (deleteDocId != INVALID_DOCID) {
        auto st = DeletionMapIndexerOrganizerUtil::Delete(deleteDocId, &_buildInfoHolder);
        if (!st.IsOK()) {
            AUTIL_LOG(ERROR, "delete doc failed for deletion map, docid:%d, %s", deleteDocId, st.ToString().c_str());
        }
    }
    _buildInfoHolder.buildingIndexer->ProcessAddDocument(doc);
    return Status::OK();
}

Status SingleDeletionMapBuilder::DeleteDocument(indexlibv2::document::IDocument* doc)
{
    docid_t docId = doc->GetDocId();
    if (docId == INVALID_DOCID) {
        return Status::OK();
    }
    return DeletionMapIndexerOrganizerUtil::Delete(docId, &_buildInfoHolder);
}

Status SingleDeletionMapBuilder::Init(const indexlibv2::framework::TabletData& tabletData,
                                      const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig)
{
    return DeletionMapIndexerOrganizerUtil::InitFromTabletData(indexConfig, &tabletData, &_buildInfoHolder);
}
} // namespace indexlib::index