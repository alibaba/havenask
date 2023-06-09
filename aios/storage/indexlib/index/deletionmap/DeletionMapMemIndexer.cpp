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
#include "indexlib/index/deletionmap/DeletionMapMemIndexer.h"

#include "autil/mem_pool/Pool.h"
#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/BuildingIndexMemoryUseUpdater.h"
#include "indexlib/index/DocMapDumpParams.h"
#include "indexlib/index/deletionmap/Common.h"
#include "indexlib/index/deletionmap/DeletionMapMetrics.h"
#include "indexlib/index/deletionmap/DeletionMapUtil.h"
#include "indexlib/util/MMapAllocator.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, DeletionMapMemIndexer);

DeletionMapMemIndexer::DeletionMapMemIndexer(segmentid_t segmentId, bool isSortDump)
    : _docCount(0)
    , _segmentId(segmentId)
    , _sortDump(isSortDump)
{
}

DeletionMapMemIndexer::~DeletionMapMemIndexer()
{
    _dumpingBitmap.reset();
    if (_metrics) {
        _metrics->Stop();
        _metrics.reset();
    }
    _bitmap.reset();
    _pool.reset();
    _allocator.reset();
}

Status DeletionMapMemIndexer::Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                   document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory)
{
    _allocator.reset(new indexlib::util::MMapAllocator);
    _pool.reset(new autil::mem_pool::Pool(_allocator.get(), 1 * 1024 * 1024));
    _bitmap.reset(new indexlib::util::ExpandableBitmap(DeletionMapUtil::DEFAULT_BITMAP_SIZE, false, _pool.get()));
    if (_metrics) {
        _metrics->Start();
    }
    return Status::OK();
}
Status DeletionMapMemIndexer::Build(const document::IIndexFields* indexFields, size_t n)
{
    assert(0);
    return {};
}
Status DeletionMapMemIndexer::Build(document::IDocumentBatch* docBatch)
{
    assert(docBatch);
    for (size_t i = 0; i < docBatch->GetBatchSize(); ++i) {
        if (docBatch->IsDropped(i)) {
            continue;
        }
        indexlibv2::document::IDocument* doc = (*docBatch)[i].get();
        if (doc->GetDocOperateType() != ADD_DOC) {
            continue;
        }
        ProcessAddDocument(doc);
    }
    return Status::OK();
}

void DeletionMapMemIndexer::ProcessAddDocument(indexlibv2::document::IDocument* doc)
{
    assert(doc->GetDocOperateType() == ADD_DOC);
    assert(dynamic_cast<document::NormalDocument*>(doc) != nullptr);
    ++_docCount;
}

uint32_t DeletionMapMemIndexer::GetDeletedDocCount() const
{
    auto bitmap = _bitmap.get();
    if (bitmap) {
        return bitmap->GetSetCount();
    }
    return 0;
}

void DeletionMapMemIndexer::RegisterMetrics(const std::shared_ptr<DeletionMapMetrics>& metrics) { _metrics = metrics; }

Status DeletionMapMemIndexer::Delete(docid_t docid)
{
    _bitmap->Set(docid);
    return Status::OK();
}

Status DeletionMapMemIndexer::Dump(autil::mem_pool::PoolBase* dumpPool,
                                   const std::shared_ptr<indexlib::file_system::Directory>& indexDirectory,
                                   const std::shared_ptr<framework::DumpParams>& dumpParams)
{
    if (!_dumpingBitmap) {
        AUTIL_LOG(ERROR, "_dumpingBitmap is nullptr, need Call Seal before Dump");
        return Status::Corruption("_dumpingBitmap is nullptr, need Call Seal before Dump");
    }

    std::unique_ptr<indexlib::util::ExpandableBitmap> sortDumpBitmap;
    auto params = std::dynamic_pointer_cast<DocMapDumpParams>(dumpParams);
    if (params) {
        sortDumpBitmap.reset(new indexlib::util::ExpandableBitmap(_dumpingBitmap->GetItemCount(), false, _pool.get()));
        for (docid_t oldDeleteDocId = _dumpingBitmap->Begin(); oldDeleteDocId != indexlib::util::Bitmap::INVALID_INDEX;
             oldDeleteDocId = _dumpingBitmap->Next(oldDeleteDocId)) {
            assert(oldDeleteDocId < params->old2new.size());
            sortDumpBitmap->Set(params->old2new[oldDeleteDocId]);
        }
    }
    std::string fileName = DeletionMapUtil::GetDeletionMapFileName(_segmentId);
    indexlib::file_system::WriterOption writerParam;
    writerParam.copyOnDump = true;
    std::shared_ptr<indexlib::file_system::FileWriter> writer = indexDirectory->CreateFileWriter(fileName, writerParam);
    RETURN_IF_STATUS_ERROR(DeletionMapUtil::DumpBitmap(writer, params ? sortDumpBitmap.get() : _dumpingBitmap.get(),
                                                       _dumpingBitmap->GetValidItemCount()),
                           "dump bitmap fail");

    auto ret = writer->Close();
    if (!ret.OK()) {
        AUTIL_LOG(ERROR, "file writer close fail, fileName[%s]", fileName.c_str());
        return Status::IOError("close file writer failed");
    }
    return Status::OK();
}

bool DeletionMapMemIndexer::IsDirty() const
{
    if (_dumpingBitmap) {
        return _dumpingBitmap->GetSetCount() > 0;
    }

    assert(_dumpingBitmap); // need Call Seal before
    return true;
}

segmentid_t DeletionMapMemIndexer::GetSegmentId() const { return _segmentId; }

void DeletionMapMemIndexer::Seal()
{
    _bitmap->ReSize(_docCount);
    _dumpingBitmap.reset(_bitmap->CloneWithOnlyValidData());
}

void DeletionMapMemIndexer::ValidateDocumentBatch(document::IDocumentBatch* docBatch) {}
bool DeletionMapMemIndexer::IsValidDocument(document::IDocument* doc) { return true; }
bool DeletionMapMemIndexer::IsValidField(const document::IIndexFields* fields) { return true; }
void DeletionMapMemIndexer::FillStatistics(const std::shared_ptr<indexlib::framework::SegmentMetrics>& segmentMetrics)
{
    // do nothing
}

void DeletionMapMemIndexer::UpdateMemUse(BuildingIndexMemoryUseUpdater* memUpdater)
{
    size_t poolSize = _allocator->getUsedBytes();
    memUpdater->UpdateCurrentMemUse(poolSize);
    memUpdater->EstimateDumpTmpMemUse(_sortDump ? poolSize * 2 : poolSize);
    memUpdater->EstimateDumpExpandMemUse(0);
    memUpdater->EstimateDumpedFileSize(poolSize);
}

std::string DeletionMapMemIndexer::GetIndexName() const { return DELETION_MAP_INDEX_NAME; }
autil::StringView DeletionMapMemIndexer::GetIndexType() const { return DELETION_MAP_INDEX_TYPE_STR; }

} // namespace indexlibv2::index
