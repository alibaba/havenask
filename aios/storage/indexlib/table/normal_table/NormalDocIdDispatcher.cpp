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
#include "indexlib/table/normal_table/NormalDocIdDispatcher.h"

#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/index/primary_key/PrimaryKeyReader.h"

namespace indexlib::table {
AUTIL_LOG_SETUP(indexlib.table, NormalDocIdDispatcher);

NormalDocIdDispatcher::NormalDocIdDispatcher(const std::string& tableName,
                                             const std::shared_ptr<indexlib::index::PrimaryKeyIndexReader>& pkReader,
                                             docid_t buildingSegmentBaseDocId, docid_t currentSegmentDocCount)
    : _tableName(tableName)
    , _pkReader(pkReader)
    , _buildingSegmentBaseDocId(buildingSegmentBaseDocId)
    , _currentSegmentDocCount(currentSegmentDocCount)
{
}

void NormalDocIdDispatcher::DispatchDocId(const std::shared_ptr<indexlibv2::document::NormalDocument>& doc)
{
    // assert(doc->GetDocId() == INVALID_DOCID);  // for som ut
    doc->SetDocId(INVALID_DOCID);
    switch (doc->GetDocOperateType()) {
    case ADD_DOC: {
        return ProcessAddDocument(doc);
    }
    case DELETE_DOC: {
        return ProcessDeleteDocument(doc);
    }
    case UPDATE_FIELD: {
        return ProcessUpdateDocument(doc);
    }
    default:
        return;
    }
}

void NormalDocIdDispatcher::ProcessAddDocument(const std::shared_ptr<indexlibv2::document::NormalDocument>& doc)
{
    const std::string& pkStr = doc->GetPrimaryKey();
    docid_t oldDocId = Lookup(pkStr);
    if (oldDocId != INVALID_DOCID) {
        doc->SetDeleteDocId(oldDocId);
    }
    docid_t docid = _currentSegmentDocCount;
    doc->SetDocId(docid);
    Update(pkStr, _buildingSegmentBaseDocId + docid);
    _currentSegmentDocCount++;
}

void NormalDocIdDispatcher::ProcessUpdateDocument(const std::shared_ptr<indexlibv2::document::NormalDocument>& doc)
{
    assert(doc->GetDocId() == INVALID_DOCID);
    const std::string& pkStr = doc->GetIndexDocument()->GetPrimaryKey();
    docid_t docId = Lookup(pkStr);
    if (docId == INVALID_DOCID) {
        AUTIL_SLOG_EVERY_T(WARN, 60) << _tableName << " target update document pk: " << pkStr
                                     << " hash: " << doc->GetDocInfo().hashId << " ts: " << doc->GetDocInfo().timestamp
                                     << " not exist!";
    }
    doc->SetDocId(docId);
}

void NormalDocIdDispatcher::ProcessDeleteDocument(const std::shared_ptr<indexlibv2::document::NormalDocument>& doc)
{
    // assert(doc->GetDocOperateType() == DELETE_DOC);  // may be MainSubDocIdManager delete sub doc
    const std::string& pkStr = doc->GetIndexDocument()->GetPrimaryKey();
    docid_t docId = Lookup(pkStr);
    if (docId != INVALID_DOCID) {
        Remove(pkStr);
    } else {
        AUTIL_SLOG_EVERY_T(WARN, 60) << _tableName << " target delete document pk: " << pkStr
                                     << " hash: " << doc->GetDocInfo().hashId << " ts: " << doc->GetDocInfo().timestamp
                                     << " not exist!";
    }
    doc->SetDocId(docId);
}

docid_t NormalDocIdDispatcher::Lookup(const std::string& pkStr) const
{
    if (!_pkReader) {
        return INVALID_DOCID;
    }
    if (_pkToDocIdMap.find(pkStr) != _pkToDocIdMap.end()) {
        return _pkToDocIdMap.at(pkStr);
    }
    return _pkReader->Lookup(pkStr, _pkReader->GetBuildExecutor());
}

void NormalDocIdDispatcher::Update(const std::string& pk, docid_t docId) { _pkToDocIdMap[pk] = docId; }

void NormalDocIdDispatcher::Remove(const std::string& pk)
{
    if (_pkToDocIdMap.find(pk) != _pkToDocIdMap.end()) {
        _pkToDocIdMap.erase(pk);
    }
}

} // namespace indexlib::table
