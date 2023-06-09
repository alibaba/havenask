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
#include "indexlib/index/statistics_term/StatisticsTermMemIndexer.h"

#include "indexlib/document/IDocument.h"
#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/BuildingIndexMemoryUseUpdater.h"
#include "indexlib/index/statistics_term/Constant.h"
#include "indexlib/index/statistics_term/StatisticsTermFormatter.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, StatisticsTermMemIndexer);

StatisticsTermMemIndexer::StatisticsTermMemIndexer(std::map<std::string, size_t>& initialKeyCount)
    : _initialKeyCount(initialKeyCount)
    , _allocator(new TermAllocator(&_simplePool))
{
}

Status StatisticsTermMemIndexer::Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                      document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory)
{
    _indexConfig = std::dynamic_pointer_cast<StatisticsTermIndexConfig>(indexConfig);
    if (!_indexConfig) {
        return Status::InvalidArgs("expect a StatisticsTermIndexConfig");
    }

    for (const auto& [name, keyCount] : _initialKeyCount) {
        for (const auto& invertedIndexName : _indexConfig->GetInvertedIndexNames()) {
            if (name == GetStatisticsTermKey(_indexConfig->GetIndexName(), invertedIndexName)) {
                auto hashMap = std::make_unique<TermHashMap>(*_allocator);
                hashMap->reserve(keyCount);
                _termOriginValueMap[invertedIndexName] = std::move(hashMap);
                _termOriginValueMap[invertedIndexName]->reserve(keyCount);
                AUTIL_LOG(INFO, "index[%s] initial keycount[%lu]", invertedIndexName.c_str(), keyCount);
                break;
            }
        }
    }
    return Status::OK();
}

Status StatisticsTermMemIndexer::AddDocument(const std::shared_ptr<indexlib::document::IndexDocument>& indexDoc)
{
    auto termOriginValueMap = indexDoc->GetTermOriginValueMap();
    for (const auto& [indexName, newHashMap] : termOriginValueMap) {
        auto it = _termOriginValueMap.find(indexName);
        if (it == _termOriginValueMap.end()) {
            auto termHashMap = std::make_unique<TermHashMap>(*_allocator);
            for (const auto& [termHash, termStr] : newHashMap) {
                (*termHashMap)[termHash] = termStr;
            }
            _termOriginValueMap.insert(it, std::make_pair(indexName, std::move(termHashMap)));
        } else {
            auto& currentHashMap = it->second;
            currentHashMap->insert(newHashMap.begin(), newHashMap.end());
        }
    }
    return Status::OK();
}
Status StatisticsTermMemIndexer::Build(const document::IIndexFields* indexFields, size_t n)
{
    assert(0);
    return Status::OK();
}
Status StatisticsTermMemIndexer::Build(document::IDocumentBatch* docBatch)
{
    for (size_t i = 0; i < docBatch->GetBatchSize(); ++i) {
        if (docBatch->IsDropped(i)) {
            continue;
        }
        auto doc = (*docBatch)[i];
        if (doc->GetDocOperateType() != ADD_DOC) {
            continue;
        }
        auto normalDoc = std::dynamic_pointer_cast<indexlibv2::document::NormalDocument>(doc);
        if (normalDoc == nullptr) {
            continue;
        }
        auto indexDoc = normalDoc->GetIndexDocument();
        RETURN_IF_STATUS_ERROR(AddDocument(indexDoc), "add index doc failed.");
    }
    return Status::OK();
}

Status StatisticsTermMemIndexer::Dump(autil::mem_pool::PoolBase* dumpPool,
                                      const std::shared_ptr<indexlib::file_system::Directory>& directory,
                                      const std::shared_ptr<framework::DumpParams>& params)
{
    auto idirectory = directory->GetIDirectory();
    auto [s, indexDir] =
        idirectory->MakeDirectory(GetIndexName(), indexlib::file_system::DirectoryOption()).StatusWith();
    if (!s.IsOK()) {
        AUTIL_LOG(ERROR, "make index directory %s failed, error: %s", GetIndexName().c_str(), s.ToString().c_str());
        return s;
    }

    std::shared_ptr<indexlib::file_system::FileWriter> dataFile;
    std::tie(s, dataFile) = CreateFileWriter(indexDir, DATA_FILE);
    if (!s.IsOK()) {
        AUTIL_LOG(ERROR, "create data file for %s failed, error: %s", GetIndexName().c_str(), s.ToString().c_str());
        return s;
    }
    size_t offset = 0;
    for (const auto& [invertedIndexName, hashMap] : _termOriginValueMap) {
        std::string line = StatisticsTermFormatter::GetHeaderLine(invertedIndexName, hashMap->size());
        size_t blockLen = line.size();
        RETURN_IF_STATUS_ERROR(dataFile->Write((void*)line.c_str(), line.length()).Status(),
                               "write invert index failed, line[%s]", line.c_str());
        for (const auto& [termHash, termStr] : *hashMap) {
            line = StatisticsTermFormatter::GetDataLine(termStr, termHash);
            RETURN_IF_STATUS_ERROR(dataFile->Write((void*)line.c_str(), line.length()).Status(),
                                   "write term value failed, line[%s]", line.c_str());
            blockLen += line.size();
        }
        _termMetas[invertedIndexName] = std::make_pair(offset, blockLen);
        offset += blockLen;
    }
    RETURN_IF_STATUS_ERROR(dataFile->Close().Status(), "close data file failed.");

    // write meta file
    std::string termMeta = autil::legacy::ToJsonString(_termMetas);
    auto st = indexDir->Store(META_FILE, termMeta, indexlib::file_system::WriterOption::Buffer()).Status();
    RETURN_IF_STATUS_ERROR(st, "write term meta failed");

    return Status::OK();
}

void StatisticsTermMemIndexer::ValidateDocumentBatch(document::IDocumentBatch* docBatch)
{
    for (size_t i = 0; i < docBatch->GetBatchSize(); ++i) {
        auto doc = (*docBatch)[i].get();
        if (!IsValidDocument(doc)) {
            docBatch->DropDoc(i);
        }
    }
}

bool StatisticsTermMemIndexer::IsValidDocument(document::IDocument* doc) { return doc->GetDocOperateType() == ADD_DOC; }

bool StatisticsTermMemIndexer::IsValidField(const document::IIndexFields* fields)
{
    assert(false);
    return false;
}

void StatisticsTermMemIndexer::FillStatistics(
    const std::shared_ptr<indexlib::framework::SegmentMetrics>& segmentMetrics)
{
    for (const auto& invertedIndexName : _indexConfig->GetInvertedIndexNames()) {
        auto key = GetStatisticsTermKey(_indexConfig->GetIndexName(), invertedIndexName);
        auto it = _termOriginValueMap.find(key);
        if (it != _termOriginValueMap.end()) {
            segmentMetrics->SetDistinctTermCount(key, it->second->size());
        }
    }
}

void StatisticsTermMemIndexer::UpdateMemUse(BuildingIndexMemoryUseUpdater* memUpdater)
{
    int64_t currentMemUse = _simplePool.getUsedBytes();
    memUpdater->UpdateCurrentMemUse(currentMemUse);

    memUpdater->EstimateDumpTmpMemUse(currentMemUse);
    memUpdater->EstimateDumpExpandMemUse(0);
    memUpdater->EstimateDumpedFileSize(currentMemUse);
}

std::string StatisticsTermMemIndexer::GetIndexName() const { return _indexConfig->GetIndexName(); }
autil::StringView StatisticsTermMemIndexer::GetIndexType() const { return STATISTICS_TERM_INDEX_TYPE_STR; }

void StatisticsTermMemIndexer::Seal() {}

bool StatisticsTermMemIndexer::IsDirty() const { return _termOriginValueMap.size() > 0; }

std::pair<Status, std::shared_ptr<indexlib::file_system::FileWriter>>
StatisticsTermMemIndexer::CreateFileWriter(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                                           const std::string& fileName) const
{
    indexlib::file_system::WriterOption opts;
    opts.openType = indexlib::file_system::FSOT_BUFFERED;
    return directory->CreateFileWriter(fileName, opts).StatusWith();
}

std::string StatisticsTermMemIndexer::GetStatisticsTermKey(const std::string& indexName,
                                                           const std::string& invertedIndexName)
{
    return indexName + "_" + invertedIndexName;
}
} // namespace indexlibv2::index
