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
#include "indexlib/index/orc/OrcMemIndexer.h"

#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/document/extractor/IDocumentInfoExtractor.h"
#include "indexlib/document/extractor/IDocumentInfoExtractorFactory.h"
#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/MemDirectory.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/BuildingIndexMemoryUseUpdater.h"
#include "indexlib/index/orc/OrcMemBuffer.h"
#include "indexlib/index/orc/OrcMemBufferContainer.h"
#include "indexlib/index/orc/TypeUtils.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"
#include "orc/MemoryPool.hh"

namespace indexlibv2::index {

AUTIL_LOG_SETUP(indexlib.index, OrcMemIndexer);

OrcMemIndexer::OrcMemIndexer()
    : _memWriter(&_pool)
    , _outputStream(&_memWriter)
    // BlockBuffer in orc use reference, we need to hold pool lifetime
    , _orcPool(orc::getMemoryPool())
    , _totalRecordCount(0)
{
}

OrcMemIndexer::~OrcMemIndexer()
{
    if (_memBufferContainer) {
        _memBufferContainer->Stop();
    }
}
Status OrcMemIndexer::Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                           document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory)
{
    std::any emptyFieldHint;
    _docInfoExtractor = docInfoExtractorFactory->CreateDocumentInfoExtractor(
        indexConfig, document::extractor::DocumentInfoExtractorType::ATTRIBUTE_DOC, /*fieldHint=*/emptyFieldHint);
    if (_docInfoExtractor == nullptr) {
        return Status::InternalError("create document info extractor failed.");
    }
    _config = std::dynamic_pointer_cast<config::OrcConfig>(indexConfig);
    if (_config == nullptr) {
        return Status::InvalidArgs("invalid index config, dynamic cast to orc config failed.");
    }
    std::unique_ptr<orc::Type> schema = TypeUtils::MakeOrcTypeFromConfig(_config.get(), /*fieldIds=*/ {});
    if (schema == nullptr) {
        return Status::InvalidArgs("make orc schema from orc config failed: %s", _config->DebugString().c_str());
    }
    orc::WriterOptions options = _config->GetWriterOptions();
    options.setMemoryPool(_orcPool);
    _writer = orc::createWriter(*schema, &_outputStream, options);
    if (_writer == nullptr) {
        return Status::InternalError("fail to create orc writer.");
    }
    auto numRowsPerBatch = _config->GetRowGroupSize();
    if (numRowsPerBatch <= 0) {
        return Status::InvalidArgs("invalid num rows per batch: ", numRowsPerBatch);
    }
    _memBufferContainer =
        std::make_unique<OrcMemBufferContainer>(_config->GetOrcGeneralOptions().GetBuildBufferSize(), numRowsPerBatch);
    auto status = _memBufferContainer->Init(*_config, _writer.get());
    if (!status.IsOK()) {
        return status;
    }
    _memBuffer = _memBufferContainer->Pop();
    AUTIL_LOG(INFO, "orc mem indexer init success, use total memory: %lu bytes", _pool.getTotalBytes());
    return Status::OK();
}
Status OrcMemIndexer::Build(const document::IIndexFields* indexFields, size_t n)
{
    assert(0);
    return {};
}
Status OrcMemIndexer::Build(document::IDocumentBatch* docBatch)
{
    std::vector<indexlib::document::AttributeDocument*> docs;
    auto status = DocBatchToDocs(docBatch, &docs);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "convert doc batch to docs failed");
        return status;
    }
    for (const auto& doc : docs) {
        if (unlikely(_memBuffer == nullptr)) {
            _memBuffer = _memBufferContainer->Pop();
        }
        _memBuffer->Append(doc);
        // alway check whether the batch is full after increment rowIdx
        if (unlikely(_memBuffer->IsFull())) {
            _memBufferContainer->Push(_memBuffer);
            _memBuffer = nullptr;
        }
    }
    _totalRecordCount.fetch_add(docs.size());
    return Status::OK();
}

Status OrcMemIndexer::Dump(autil::mem_pool::PoolBase* dumpPool,
                           const std::shared_ptr<indexlib::file_system::Directory>& directory,
                           const std::shared_ptr<framework::DumpParams>&)
{
    AUTIL_LOG(INFO, "start dump orc file, orc pool memory: %lu bytes, writer stream memory: %lu bytes, doc count %lu",
              _orcPool->currentUsage(), _pool.getTotalBytes(), _totalRecordCount.load());
    auto subDir = directory->MakeDirectory(_config->GetIndexName());
    auto fileWriter =
        subDir->CreateFileWriter(config::OrcConfig::GetOrcFileName(), indexlib::file_system::WriterOption::Buffer());
    // still have some rows left.
    if (_memBufferContainer != nullptr) {
        if (_memBuffer != nullptr && _memBuffer->Size() > 0) {
            _memBufferContainer->Push(_memBuffer);
        }
        _memBuffer = nullptr;
        _memBufferContainer->Stop();
        _memBufferContainer.reset();
    }
    _writer->close();
    RETURN_IF_STATUS_ERROR(_memWriter.Dump(fileWriter).Status(), "dump to file [%s] failed",
                           fileWriter->DebugString().c_str());
    RETURN_IF_STATUS_ERROR(fileWriter->Close().Status(), "close file writer [%s] failed",
                           fileWriter->DebugString().c_str());
    _memWriter.Close();
    _pool.release();

    AUTIL_LOG(INFO, "dump orc succeeded. path[%s]", subDir->DebugString().c_str());
    return Status::OK();
}

Status OrcMemIndexer::DocBatchToDocs(document::IDocumentBatch* docBatch,
                                     std::vector<indexlib::document::AttributeDocument*>* docs) const
{
    docs->reserve(docBatch->GetBatchSize());
    for (size_t i = 0; i < docBatch->GetBatchSize(); i++) {
        if (!docBatch->IsDropped(i)) {
            indexlib::document::AttributeDocument* doc = nullptr;
            Status s = _docInfoExtractor->ExtractField((*docBatch)[i].get(), (void**)&doc);
            if (!s.IsOK()) {
                return s;
            }
            (*docs).emplace_back(doc);
        }
    }
    return Status::OK();
}

void OrcMemIndexer::UpdateMemUse(BuildingIndexMemoryUseUpdater* memUpdater)
{
    uint64_t currentUsage = _orcPool->currentUsage() + _pool.getTotalBytes();
    memUpdater->UpdateCurrentMemUse(currentUsage);
    memUpdater->EstimateDumpExpandMemUse(0);
    size_t recordCount = _totalRecordCount.load();
    if (recordCount > 0) {
        size_t tempBufferSize = 0;
        if (recordCount < _config->GetRowGroupSize()) {
            tempBufferSize = currentUsage;
        } else {
            double bytesPerRecord = currentUsage * 1.0 / recordCount;
            tempBufferSize = std::ceil(bytesPerRecord * _config->GetRowGroupSize());
        }
        memUpdater->EstimateDumpTmpMemUse(tempBufferSize);
        static constexpr double COMPRESSION_FACTOR = 0.6f;
        memUpdater->EstimateDumpedFileSize(std::ceil(currentUsage * COMPRESSION_FACTOR));
    }
}

void OrcMemIndexer::ValidateDocumentBatch(document::IDocumentBatch* docBatch)
{
    for (size_t i = 0; i < docBatch->GetBatchSize(); i++) {
        if (!docBatch->IsDropped(i)) {
            if (!_docInfoExtractor->IsValidDocument((*docBatch)[i].get())) {
                docBatch->DropDoc(i);
            }
        }
    }
}

bool OrcMemIndexer::IsValidDocument(document::IDocument* doc) { return _docInfoExtractor->IsValidDocument(doc); }
bool OrcMemIndexer::IsValidField(const document::IIndexFields* fields)
{
    assert(0);
    return false;
}
} // namespace indexlibv2::index
