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
                                             docid_t baseDocId)
    : _tableName(tableName)
    , _pkReader(pkReader)
    , _baseDocId(baseDocId)
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
        Remove(oldDocId);
    }
    docid_t docid = _baseDocId++;
    doc->SetDocId(docid);
    Update(pkStr, docid);
}

void NormalDocIdDispatcher::ProcessUpdateDocument(const std::shared_ptr<indexlibv2::document::NormalDocument>& doc)
{
    assert(doc->GetDocId() == INVALID_DOCID);
    const std::string& pkStr = doc->GetIndexDocument()->GetPrimaryKey();
    docid_t docId = Lookup(pkStr);
    if (docId == INVALID_DOCID) {
        AUTIL_LOG(ERROR, "[%s] target update document [pk:%s] [hash:%u] [ts:%ld] not exist!", _tableName.c_str(),
                  pkStr.c_str(), doc->GetDocInfo().hashId, doc->GetDocInfo().timestamp);
    }
    doc->SetDocId(docId);
}

void NormalDocIdDispatcher::ProcessDeleteDocument(const std::shared_ptr<indexlibv2::document::NormalDocument>& doc)
{
    // assert(doc->GetDocOperateType() == DELETE_DOC);  // may be MainSubDocIdManager delete sub doc
    const std::string& pkStr = doc->GetIndexDocument()->GetPrimaryKey();
    docid_t docId = Lookup(pkStr);
    if (docId != INVALID_DOCID) {
        Remove(docId);
    } else {
        AUTIL_LOG(ERROR, "[%s] target delete document [pk:%s] [hash:%u] [ts:%ld] not exist!", _tableName.c_str(),
                  pkStr.c_str(), doc->GetDocInfo().hashId, doc->GetDocInfo().timestamp);
    }
    doc->SetDocId(docId);
}

docid_t NormalDocIdDispatcher::Lookup(const std::string& pkStr) const
{
    if (_pkToDocIdMap.find(pkStr) != _pkToDocIdMap.end()) {
        return _pkToDocIdMap.at(pkStr);
    }
    return _pkReader->Lookup(pkStr, _pkReader->GetBuildExecutor());
}

void NormalDocIdDispatcher::Update(std::string pk, docid_t docId)
{
    auto iter = _pkToDocIdMap.find(pk);
    if (iter != _pkToDocIdMap.end()) {
        _docIdToPkMap.erase(iter->second);
    }
    _pkToDocIdMap[pk] = docId;
    _docIdToPkMap[docId] = pk;

    assert(_pkToDocIdMap.size() == _docIdToPkMap.size());
}

void NormalDocIdDispatcher::Remove(docid_t docId)
{
    auto iter = _docIdToPkMap.find(docId);
    if (iter != _docIdToPkMap.end()) {
        _pkToDocIdMap.erase(iter->second);
        _docIdToPkMap.erase(iter);
    }

    assert(_pkToDocIdMap.size() == _docIdToPkMap.size());
}

} // namespace indexlib::table
