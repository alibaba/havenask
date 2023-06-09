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
#include "indexlib/index/summary/SummaryMemIndexer.h"

#include "indexlib/document/IDocument.h"
#include "indexlib/document/extractor/IDocumentInfoExtractor.h"
#include "indexlib/document/extractor/IDocumentInfoExtractorFactory.h"
#include "indexlib/document/extractor/plain/SummaryDocInfoExtractor.h"
#include "indexlib/document/normal/GroupFieldFormatter.h"
#include "indexlib/document/normal/SerializedSummaryDocument.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/framework/DumpParams.h"
#include "indexlib/index/summary/Common.h"
#include "indexlib/index/summary/LocalDiskSummaryMemIndexer.h"
#include "indexlib/index/summary/SummaryMemReader.h"
#include "indexlib/index/summary/SummaryMemReaderContainer.h"
#include "indexlib/index/summary/config/SummaryGroupConfig.h"
#include "indexlib/index/summary/config/SummaryIndexConfig.h"
#include "indexlib/util/PathUtil.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, SummaryMemIndexer);

Status SummaryMemIndexer::Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                               document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory)
{
    _summaryIndexConfig = std::dynamic_pointer_cast<config::SummaryIndexConfig>(indexConfig);

    assert(_summaryIndexConfig != nullptr);
    for (summarygroupid_t groupId = 0; groupId < _summaryIndexConfig->GetSummaryGroupConfigCount(); ++groupId) {
        const auto& groupConfig = _summaryIndexConfig->GetSummaryGroupConfig(groupId);
        if (groupConfig->NeedStoreSummary()) {
            _groupMemIndexers.push_back(std::shared_ptr<LocalDiskSummaryMemIndexer>(new LocalDiskSummaryMemIndexer()));
            _groupMemIndexers[groupId]->Init(groupConfig);
        } else {
            _groupMemIndexers.push_back(std::shared_ptr<LocalDiskSummaryMemIndexer>());
        }
    }

    std::any emptyFieldHint;
    assert(docInfoExtractorFactory);
    _docInfoExtractor = docInfoExtractorFactory->CreateDocumentInfoExtractor(
        indexConfig, document::extractor::DocumentInfoExtractorType::SUMMARY_DOC, emptyFieldHint);

    return Status::OK();
}

bool SummaryMemIndexer::ShouldSkipBuild(config::SummaryIndexConfig* summaryIndexConfig)
{
    if (summaryIndexConfig->GetSummaryGroupConfigCount() == 0) {
        return true;
    }
    return !(summaryIndexConfig->NeedStoreSummary());
}
Status SummaryMemIndexer::Build(const document::IIndexFields* indexFields, size_t n)
{
    assert(0);
    return Status::OK();
}
Status SummaryMemIndexer::Build(document::IDocumentBatch* docBatch)
{
    if (ShouldSkipBuild(_summaryIndexConfig.get())) {
        return Status::OK();
    }

    for (size_t i = 0; i < docBatch->GetBatchSize(); ++i) {
        if (!docBatch->IsDropped(i)) {
            std::shared_ptr<document::IDocument> doc = (*docBatch)[i];
            docid_t docId = doc->GetDocId();
            if (doc->GetDocOperateType() != ADD_DOC) {
                AUTIL_LOG(DEBUG, "doc[%d] isn't add_doc", docId);
                continue;
            }
            RETURN_STATUS_DIRECTLY_IF_ERROR(AddDocument(doc.get()));
        }
    }
    return Status::OK();
}

bool SummaryMemIndexer::IsDirty() const
{
    for (auto& groupMemIndexer : _groupMemIndexers) {
        if (groupMemIndexer && groupMemIndexer->GetNumDocuments() > 0) {
            return true;
        }
    }
    return false;
}

Status SummaryMemIndexer::Dump(autil::mem_pool::PoolBase* dumpPool,
                               const std::shared_ptr<indexlib::file_system::Directory>& indexDirectory,
                               const std::shared_ptr<framework::DumpParams>& params)
{
    if (_groupMemIndexers.empty()) {
        return Status::OK();
    }
    assert(indexDirectory != nullptr);
    auto directory = indexDirectory->GetIDirectory();
    assert(directory != nullptr);
    assert((size_t)_summaryIndexConfig->GetSummaryGroupConfigCount() == _groupMemIndexers.size());
    if (_groupMemIndexers[0]) {
        assert(_summaryIndexConfig->GetSummaryGroupConfig(0)->IsDefaultGroup());
        auto status = _groupMemIndexers[0]->Dump(directory, dumpPool, params);
        RETURN_IF_STATUS_ERROR(status, "dump summary index fail for the first group");
    }

    for (size_t i = 1; i < _groupMemIndexers.size(); ++i) {
        if (!_groupMemIndexers[i]) {
            continue;
        }
        const std::string& groupName = _groupMemIndexers[i]->GetGroupName();
        auto [status, realDirectory] =
            directory->MakeDirectory(groupName, indexlib::file_system::DirectoryOption()).StatusWith();
        RETURN_IF_STATUS_ERROR(status, "make summary group directory for dump fail, group = [%s]", groupName.c_str());
        status = _groupMemIndexers[i]->Dump(realDirectory, dumpPool, params);
        RETURN_IF_STATUS_ERROR(status, "dump summary index fail for group [%zu]", i);
    }
    return Status::OK();
}

void SummaryMemIndexer::ValidateDocumentBatch(document::IDocumentBatch* docBatch)
{
    for (size_t i = 0; i < docBatch->GetBatchSize(); ++i) {
        if (!docBatch->IsDropped(i)) {
            if (!_docInfoExtractor->IsValidDocument((*docBatch)[i].get())) {
                docBatch->DropDoc(i);
            }
        }
    }
}
bool SummaryMemIndexer::IsValidDocument(document::IDocument* doc) { return _docInfoExtractor->IsValidDocument(doc); }
bool SummaryMemIndexer::IsValidField(const document::IIndexFields* fields)
{
    assert(false);
    return false;
}
void SummaryMemIndexer::UpdateMemUse(BuildingIndexMemoryUseUpdater* memUpdater)
{
    // un-implemented
    for (size_t i = 0; i < _groupMemIndexers.size(); ++i) {
        if (!_groupMemIndexers[i]) {
            continue;
        }
        _groupMemIndexers[i]->UpdateMemUse(memUpdater);
    }
}

std::string SummaryMemIndexer::GetIndexName() const
{
    assert(_summaryIndexConfig != nullptr);
    return _summaryIndexConfig->GetIndexName();
}
autil::StringView SummaryMemIndexer::GetIndexType() const { return SUMMARY_INDEX_TYPE_STR; }

Status SummaryMemIndexer::AddDocument(document::IDocument* doc)
{
    indexlib::document::SerializedSummaryDocument* summaryDoc = nullptr;
    RETURN_STATUS_DIRECTLY_IF_ERROR(_docInfoExtractor->ExtractField(doc, (void**)&summaryDoc));

    // ScopeBuildProfilingReporter reporter(mProfilingMetrics);

    assert(summaryDoc);
    if (_groupMemIndexers.size() == 1) {
        // only default group
        assert(summaryDoc->GetDocId() == (docid_t)_groupMemIndexers[0]->GetNumDocuments());
        assert(_groupMemIndexers[0]);
        autil::StringView data(summaryDoc->GetValue(), summaryDoc->GetLength());
        _groupMemIndexers[0]->AddDocument(data);
        return Status::OK();
    }

    // mv to SummaryFormatter::DeserializeSummaryDoc
    const char* cursor = summaryDoc->GetValue();
    for (size_t i = 0; i < _groupMemIndexers.size(); ++i) {
        if (!_groupMemIndexers[i]) {
            continue;
        }
        assert(summaryDoc->GetDocId() == (docid_t)_groupMemIndexers[i]->GetNumDocuments());
        uint32_t len = indexlib::document::GroupFieldFormatter::ReadVUInt32(cursor);
        autil::StringView data(cursor, len);
        cursor += len;
        _groupMemIndexers[i]->AddDocument(data);
    }
    assert(cursor - summaryDoc->GetValue() == (int64_t)summaryDoc->GetLength());
    return Status::OK();
}

const std::shared_ptr<SummaryMemReaderContainer> SummaryMemIndexer::CreateInMemReader()
{
    auto summaryMemReaderContainer = std::make_shared<SummaryMemReaderContainer>();
    for (size_t i = 0; i < _groupMemIndexers.size(); ++i) {
        if (_groupMemIndexers[i]) {
            summaryMemReaderContainer->AddReader(_groupMemIndexers[i]->CreateInMemReader());
        } else {
            summaryMemReaderContainer->AddReader(nullptr);
        }
    }
    return summaryMemReaderContainer;
}

} // namespace indexlibv2::index
