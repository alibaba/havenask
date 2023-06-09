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
#include "indexlib/index/deletionmap/DeletionMapDiskIndexer.h"

#include "autil/mem_pool/Pool.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/deletionmap/Common.h"
#include "indexlib/index/deletionmap/DeletionMapMetrics.h"
#include "indexlib/index/deletionmap/DeletionMapUtil.h"
#include "indexlib/util/MMapAllocator.h"
using std::string;

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, DeletionMapDiskIndexer);
DeletionMapDiskIndexer::DeletionMapDiskIndexer(size_t docCount, segmentid_t segmentId)
    : _docCount(docCount)
    , _segmentId(segmentId)
{
}
DeletionMapDiskIndexer::~DeletionMapDiskIndexer()
{
    // no bitmap means not opened
    if (_metrics) {
        _metrics->Stop();
        _metrics.reset();
    }
    _bitmap.reset();
    _pool.reset();
    _allocator.reset();
}

void DeletionMapDiskIndexer::TEST_InitWithoutOpen()
{
    _allocator.reset(new indexlib::util::MMapAllocator);
    _pool.reset(new autil::mem_pool::Pool(_allocator.get(), 1 * 1024 * 1024));
    _bitmap.reset(new indexlib::util::Bitmap(_docCount, false, _pool.get()));
}

Status DeletionMapDiskIndexer::Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                    const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory)
{
    if (!indexDirectory) {
        AUTIL_LOG(ERROR, "directory is null");
        return Status::InvalidArgs("directory is null");
    }
    _directory = indexDirectory;
    _allocator.reset(new indexlib::util::MMapAllocator);
    _pool.reset(new autil::mem_pool::Pool(_allocator.get(), 1 * 1024 * 1024));
    _bitmap.reset(new indexlib::util::Bitmap(_docCount, false, _pool.get()));
    auto [status, diskBitmap] = GetDeletionMapPatch(_segmentId);
    RETURN_IF_STATUS_ERROR(status, "read disk bitmap for segment [%d] failed", _segmentId);
    if (diskBitmap) {
        RETURN_IF_STATUS_ERROR(ApplyDeletionMapPatch(diskBitmap.get()), "apply deletionmap patch failed");
    }
    if (_metrics) {
        _metrics->Start();
    }
    return Status::OK();
}

Status DeletionMapDiskIndexer::Delete(docid_t docid)
{
    _bitmap->Set(docid);
    return Status::OK();
}
uint32_t DeletionMapDiskIndexer::GetDeletedDocCount() const
{
    auto bitmap = _bitmap.get();
    if (bitmap) {
        return bitmap->GetSetCount();
    }
    return 0;
}

segmentid_t DeletionMapDiskIndexer::GetSegmentId() const { return _segmentId; }

void DeletionMapDiskIndexer::RegisterMetrics(const std::shared_ptr<DeletionMapMetrics>& metrics) { _metrics = metrics; }
Status DeletionMapDiskIndexer::LoadFileHeader(const indexlib::file_system::FileReaderPtr& fileReader,
                                              DeletionMapFileHeader& fileHeader)
{
    assert(fileReader);
    size_t offset = 0;
    try {
        offset += fileReader->Read(&fileHeader.bitmapItemCount, sizeof(fileHeader.bitmapItemCount), 0).GetOrThrow();
        offset += fileReader->Read(&fileHeader.bitmapDataSize, sizeof(fileHeader.bitmapDataSize), offset).GetOrThrow();
    } catch (...) {
        AUTIL_LOG(ERROR, "load deletionmap file header failed, segmentid[%u]", _segmentId);
        return Status::IOError("load deletionmap file header failed");
    }
    return Status::OK();
}
size_t DeletionMapDiskIndexer::EstimateMemUsed(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                               const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory)
{
    return indexlib::util::Bitmap::GetDumpSize(_docCount);
}

size_t DeletionMapDiskIndexer::EvaluateCurrentMemUsed() { return indexlib::util::Bitmap::GetDumpSize(_docCount); }

Status DeletionMapDiskIndexer::ApplyDeletionMapPatch(indexlib::util::Bitmap* bitmap)
{
    if (!bitmap) {
        AUTIL_LOG(ERROR, "apply failed, bit map is null");
        return Status::InvalidArgs("bit map is null");
    }
    if (bitmap->GetValidItemCount() != _docCount) {
        AUTIL_LOG(ERROR, "apply deletionmap patch failed, bitmap item count[%u] != doc count[%lu]",
                  bitmap->GetValidItemCount(), _docCount);
        return Status::InvalidArgs("apply deletionmap patch failed");
    }
    *_bitmap |= *bitmap;
    return Status::OK();
}

std::pair<Status, std::unique_ptr<indexlib::util::Bitmap>>
DeletionMapDiskIndexer::GetDeletionMapPatch(segmentid_t segmentId)
{
    string fileName = DeletionMapUtil::GetDeletionMapFileName(segmentId);
    auto existEc = _directory->IsExist(fileName);
    if (!existEc.OK()) {
        AUTIL_LOG(ERROR, "check patch from segment [%d] to segment [%d] failed", _segmentId, segmentId);
        return {existEc.Status(), nullptr};
    }
    if (existEc.result) {
        auto fileReaderEc = _directory->CreateFileReader(
            fileName, indexlib::file_system::ReaderOption::NoCache(indexlib::file_system::FSOT_BUFFERED));
        if (!fileReaderEc.OK()) {
            AUTIL_LOG(ERROR, "create patch file reader  from segment [%d] to segment [%d] failed", _segmentId,
                      segmentId);
            return {fileReaderEc.Status(), nullptr};
        }
        indexlib::file_system::FileReaderPtr fileReader = fileReaderEc.result;

        int64_t fileLength = fileReader->GetLength();
        DeletionMapFileHeader fileHeader;

        if (fileLength < sizeof(fileHeader)) {
            AUTIL_LOG(ERROR, "file length [%ld] is less than required [%lu]", fileLength, sizeof(fileHeader));
            return {Status::Corruption("file length invalid"), nullptr};
        }
        RETURN2_IF_STATUS_ERROR(LoadFileHeader(fileReader, fileHeader), nullptr, "load file header failed");
        bool useLegacyDeletionMap = false;
        size_t expectedDataSize = indexlib::util::Bitmap::GetDumpSize(fileHeader.bitmapItemCount);
        if (fileHeader.bitmapDataSize > expectedDataSize) {
            AUTIL_LOG(INFO, "Trying load legacy deletion map, shrink bitmap data size from [%u] to [%lu].",
                      fileHeader.bitmapDataSize, expectedDataSize);
            fileHeader.bitmapDataSize = expectedDataSize;
            useLegacyDeletionMap = true;
        }
        size_t expectedLength = sizeof(fileHeader) + fileHeader.bitmapDataSize;
        if (useLegacyDeletionMap && fileLength < expectedLength) {
            AUTIL_LOG(ERROR, "File length [%ld] is less than expected [%lu]. use legacy deletion map [%d].", fileLength,
                      expectedLength, useLegacyDeletionMap);
            return {Status::Corruption("file length invalid"), nullptr};
        } else if (!useLegacyDeletionMap && fileLength != expectedLength) {
            AUTIL_LOG(ERROR, "File length [%ld] is not equal to expected [%lu]. use legacy deletion map [%d].",
                      fileLength, expectedLength, useLegacyDeletionMap);
            return {Status::Corruption("file length invalid"), nullptr};
        }
        std::unique_ptr<indexlib::util::Bitmap> diskBitmap(new indexlib::util::Bitmap(fileHeader.bitmapItemCount));

        auto [st, readed] =
            fileReader->Read(diskBitmap->GetData(), fileHeader.bitmapDataSize, sizeof(fileHeader)).StatusWith();
        RETURN2_IF_STATUS_ERROR(st, nullptr, "read file failed, offset[%lu] file[%s]", sizeof(fileHeader),
                                fileReader->GetLogicalPath().c_str());
        if (readed != fileHeader.bitmapDataSize) {
            AUTIL_LOG(ERROR, "read file failed, readed[%lu] != expect[%u], offset[%lu] file[%s]", readed,
                      fileHeader.bitmapDataSize, sizeof(fileHeader), fileReader->GetLogicalPath().c_str());
            return {Status::Corruption(), nullptr};
        }
        return {Status::OK(), std::move(diskBitmap)};
    }
    return {Status::OK(), nullptr};
}

Status DeletionMapDiskIndexer::Dump(const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory)
{
    if (!_bitmap) {
        AUTIL_LOG(ERROR, "_bitmap is nullptr");
        return Status::Corruption("_bitmap is nullptr");
    }
    if (_bitmap->GetSetCount() == 0) {
        return Status::OK();
    }
    std::string fileName = DeletionMapUtil::GetDeletionMapFileName(_segmentId);
    indexlib::file_system::WriterOption writerParam;
    writerParam.copyOnDump = true;
    auto writerResult = indexDirectory->CreateFileWriter(fileName, writerParam);
    if (!writerResult.OK()) {
        AUTIL_LOG(ERROR, "create file writer [%s] failed", fileName.c_str());
        return Status::IOError("create file writer failed");
    }
    std::shared_ptr<indexlib::file_system::FileWriter> writer = writerResult.Value();
    RETURN_IF_STATUS_ERROR(DeletionMapUtil::DumpBitmap(writer, _bitmap.get(), _bitmap->GetValidItemCount()),
                           "dump bitmap fail");

    auto ret = writer->Close();
    if (!ret.OK()) {
        AUTIL_LOG(ERROR, "file writer close fail, fileName[%s]", fileName.c_str());
        return Status::IOError("close file writer fail");
    }
    return Status::OK();
}
} // namespace indexlibv2::index
