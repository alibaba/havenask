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
#include "indexlib/document/document_collector.h"

#include "indexlib/config/index_partition_options.h"
#include "indexlib/document/index_document/normal_document/index_document.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"

namespace indexlib { namespace document {
IE_LOG_SETUP(document, DocumentCollector);

DocumentCollector::DocumentCollector(const config::IndexPartitionOptions& options) : _isDirty(false)
{
    _batchSize = options.GetBuildConfig().GetBatchBuildMaxBatchSize();
    assert(_batchSize > 0);
    _documents.reserve(_batchSize);
    _deletedDocuments.reserve(_batchSize);
}

DocumentCollector::~DocumentCollector() {}

void DocumentCollector::Add(const document::DocumentPtr& doc)
{
    if (doc) {
        _documents.push_back(doc);
    }
}

void DocumentCollector::LogicalDelete(size_t i)
{
    if (unlikely(i > _documents.size())) {
        assert(false);
        return;
    }
    _isDirty = true;
    _deletedDocuments.push_back(_documents[i]);
    _documents[i].reset();
}

void DocumentCollector::RemoveNullDocuments()
{
    if (_isDirty) {
        std::vector<document::DocumentPtr> validDocuments;
        validDocuments.reserve(_documents.size());
        for (const document::DocumentPtr& doc : _documents) {
            if (doc) {
                validDocuments.push_back(doc);
            }
        }
        _documents.swap(validDocuments);
        _isDirty = false;
    }
}

bool DocumentCollector::ShouldTriggerBuild() const
{
    if (Size() >= _batchSize) {
        IE_LOG(INFO, "Trigger batch build with batch size: [%lu], max batch size[%d]", Size(), _batchSize);
        return true;
    }
    return false;
}

size_t DocumentCollector::GetTotalMemoryUse() const
{
    size_t memUse = 0;
    for (size_t i = 0; i < _documents.size(); ++i) {
        if (_documents[i]) {
            memUse += _documents[i]->EstimateMemory();
        }
    }
    return memUse;
}

std::map<std::string, int> DocumentCollector::GetBatchStatistics(const std::vector<document::DocumentPtr>& documents,
                                                                 config::IndexPartitionSchemaPtr schema)
{
    std::map<std::string, int> statistics;
    statistics["add"] = 0;
    statistics["update"] = 0;
    statistics["delete"] = 0;
    statistics["failed"] = 0;
    statistics["other"] = 0;
    for (int i = 0; i < documents.size(); i++) {
        document::DocumentPtr doc = documents[i];
        if (doc == nullptr) {
            statistics["failed"]++;
            continue;
        }
        DocOperateType type = doc->GetDocOperateType();
        switch (type) {
        case ADD_DOC:
            statistics["add"]++;
            statistics["kg_hashid_c2c_add"]++;
            break;
        case UPDATE_FIELD:
            statistics["update"]++;
            StatisticsForUpdate(doc, schema, &statistics);
            break;
        case DELETE_DOC:
        case DELETE_SUB_DOC:
            statistics["delete"]++;
            break;
        default:
            statistics["other"]++;
            break;
        }
        document::NormalDocumentPtr normalDoc = std::dynamic_pointer_cast<document::NormalDocument>(doc);
    }
    return statistics;
}

void DocumentCollector::StatisticsForUpdate(document::DocumentPtr document, config::IndexPartitionSchemaPtr schema,
                                            std::map<std::string, int>* indexNameToTokenCount)
{
    document::NormalDocumentPtr doc = std::dynamic_pointer_cast<document::NormalDocument>(document);
    if (doc == nullptr) {
        return;
    }
    const document::IndexDocumentPtr& indexDoc = doc->GetIndexDocument();
    const auto& multiFieldModifiedTokens = indexDoc->GetModifiedTokens();
    const config::IndexSchemaPtr indexSchmea = schema->GetIndexSchema();

    for (const document::ModifiedTokens& modifiedTokens : multiFieldModifiedTokens) {
        fieldid_t fieldId = modifiedTokens.FieldId();
        std::vector<indexid_t> indexIds = indexSchmea->GetIndexIdList(fieldId);
        for (indexid_t indexId : indexIds) {
            config::IndexConfigPtr indexConfig = indexSchmea->GetIndexConfig(indexId);
            if (indexConfig->GetIndexName().find("kg_hashid_c2c") != std::string::npos) {
                (*indexNameToTokenCount)["kg_hashid_c2c_update"] += modifiedTokens.NonNullTermSize();
            }
            if (indexConfig->IsIndexUpdatable()) {
                (*indexNameToTokenCount)["index_" + indexConfig->GetIndexName()] += modifiedTokens.NonNullTermSize();
            }
        }
    }
}

size_t DocumentCollector::GetSuggestDestructParallelNum(size_t threadCount)
{
    int32_t destructDocBatchSize = std::max(1, _batchSize / 8);
    size_t destructDocParallelNum =
        std::min(threadCount, (_documents.size() + _deletedDocuments.size()) / destructDocBatchSize);
    return std::max((size_t)1, destructDocParallelNum);
}

void DocumentCollector::DestructDocumentsForParallel(size_t parallelNum, size_t parallelIdx)
{
    assert(parallelIdx < parallelNum);
    for (size_t i = parallelIdx; i < _documents.size(); i += parallelNum) {
        _documents[i].reset();
    }
    for (size_t i = parallelIdx; i < _deletedDocuments.size(); i += parallelNum) {
        _deletedDocuments[i].reset();
    }
}

int64_t DocumentCollector::EstimateMemory() const
{
    int64_t totalMemory = 0;
    for (const document::DocumentPtr& doc : _documents) {
        if (doc) {
            totalMemory += doc->EstimateMemory();
        }
    }
    for (const document::DocumentPtr& doc : _deletedDocuments) {
        if (doc) {
            totalMemory += doc->EstimateMemory();
        }
    }
    return totalMemory;
}

}} // namespace indexlib::document
