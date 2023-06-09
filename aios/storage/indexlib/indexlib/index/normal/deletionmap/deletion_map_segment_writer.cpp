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
#include "indexlib/index/normal/deletionmap/deletion_map_segment_writer.h"

#include "fslib/fslib.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/normal/deletionmap/in_mem_deletion_map_reader.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/in_memory_segment_reader.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/index_define.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/MMapAllocator.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

#define DELETIONMAP_SNAPSHOT_FILE_SUFFIX ".snapshot"

using namespace std;
using namespace fslib;
using namespace fslib::fs;
using namespace autil::mem_pool;

using namespace indexlib::util;
using namespace indexlib::common;
using namespace indexlib::index_base;
using namespace indexlib::file_system;
using namespace indexlib::file_system;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, DeletionMapSegmentWriter);

uint32_t DeletionMapSegmentWriter::DEFAULT_BITMAP_SIZE = 8 * 1024 * 1024;
DeletionMapSegmentWriter::DeletionMapSegmentWriter()
    : mOrigDelDocCount(0)
    , mBitmap(NULL)
    , mBuildResourceMetricsNode(NULL)
{
    mAllocator.reset(new util::MMapAllocator);
    // default chunk size : 1MB
    mPool.reset(new autil::mem_pool::Pool(mAllocator.get(), 1 * 1024 * 1024));
}

DeletionMapSegmentWriter::~DeletionMapSegmentWriter() { DELETE_AND_SET_NULL(mBitmap); }

void DeletionMapSegmentWriter::Init(uint32_t docCount, util::BuildResourceMetrics* buildResourceMetrics)
{
    if (docCount > 0) {
        mBitmap = new ExpandableBitmap(docCount, false, mPool.get());
    } else {
        mBitmap = new ExpandableBitmap(false, mPool.get());
    }
    mOrigDelDocCount = GetDeletedCount();

    if (buildResourceMetrics) {
        mBuildResourceMetricsNode = buildResourceMetrics->AllocateNode();
        IE_LOG(INFO, "allocate build resource node [id:%d] for deletionmap segment writer",
               mBuildResourceMetricsNode->GetNodeId());
        UpdateBuildResourceMetrics();
    }
}

void DeletionMapSegmentWriter::Init(const file_system::DirectoryPtr& directory, segmentid_t segmentId,
                                    uint32_t docCount, bool needCopy)
{
    string bitmapFileName = GetDeletionMapFileName(segmentId);
    file_system::FileReaderPtr fileReader = CreateSliceFileReader(directory, bitmapFileName, docCount);
    assert(fileReader);

    string snapShotFileName = bitmapFileName + DELETIONMAP_SNAPSHOT_FILE_SUFFIX;
    MergeSnapshotIfExist(fileReader, directory, snapShotFileName);
    MountBitmap(fileReader, needCopy);
}

void DeletionMapSegmentWriter::Init(const file_system::DirectoryPtr& directory, const string& fileName, bool needCopy)
{
    if (!directory) {
        INDEXLIB_FATAL_ERROR(NonExist, "directory is NULL");
    }

    file_system::FileReaderPtr fileReader;
    fileReader = directory->CreateFileReader(fileName, file_system::FSOT_SLICE);
    if (!fileReader) {
        fileReader = directory->CreateFileReader(fileName, file_system::FSOT_MEM);
    }
    assert(fileReader);
    string snapShotFileName = fileName + DELETIONMAP_SNAPSHOT_FILE_SUFFIX;
    MergeSnapshotIfExist(fileReader, directory, snapShotFileName);
    MountBitmap(fileReader, needCopy);
}

void DeletionMapSegmentWriter::Init(const InMemorySegmentPtr& inMemSegment, bool needCopy)
{
    DirectoryPtr directory = inMemSegment->GetDirectory();
    assert(directory);

    file_system::DirectoryPtr deletionmapDirectory = directory->GetDirectory(DELETION_MAP_DIR_NAME, false);
    if (!deletionmapDirectory) {
        deletionmapDirectory = directory->MakeDirectory(DELETION_MAP_DIR_NAME);
    }

    segmentid_t segId = inMemSegment->GetSegmentData().GetSegmentId();
    string bitmapFileName = GetDeletionMapFileName(segId);
    string snapshotFileName = bitmapFileName + DELETIONMAP_SNAPSHOT_FILE_SUFFIX;

    file_system::FileReaderPtr fileReader;
    if (deletionmapDirectory->IsExist(snapshotFileName)) {
        fileReader = deletionmapDirectory->CreateFileReader(snapshotFileName, file_system::FSOT_SLICE);
    } else {
        assert(inMemSegment);
        InMemDeletionMapReaderPtr inMemSegReader = inMemSegment->GetSegmentReader()->GetInMemDeletionMapReader();
        ExpandableBitmap* bitmap = inMemSegReader->GetBitmap();
        uint32_t docCount = inMemSegment->GetSegmentWriter()->GetSegmentInfo()->docCount;
        fileReader = CreateSliceFileReader(deletionmapDirectory, snapshotFileName, bitmap, docCount);
    }
    assert(fileReader);
    MountBitmap(fileReader, needCopy);
}

void DeletionMapSegmentWriter::UpdateBuildResourceMetrics()
{
    if (!mBuildResourceMetricsNode) {
        return;
    }
    int64_t poolSize = mPool->getUsedBytes();
    // use poolsize to estimate dump file size
    int64_t dumpFileSize = 0;
    if (IsDirty()) {
        dumpFileSize = poolSize + sizeof(DeletionMapFileHeader);
    }

    mBuildResourceMetricsNode->Update(util::BMT_CURRENT_MEMORY_USE, poolSize);
    mBuildResourceMetricsNode->Update(util::BMT_DUMP_TEMP_MEMORY_SIZE, dumpFileSize);
    mBuildResourceMetricsNode->Update(util::BMT_DUMP_EXPAND_MEMORY_SIZE, 0);
    mBuildResourceMetricsNode->Update(util::BMT_DUMP_FILE_SIZE, dumpFileSize);
}

void DeletionMapSegmentWriter::EndSegment(uint32_t docCount) { mBitmap->ReSize(docCount); }

void DeletionMapSegmentWriter::Dump(const file_system::DirectoryPtr& directory, segmentid_t segId, bool force)
{
    // don't dump if nothing be modified
    if (!IsDirty() && !force) {
        return;
    }
    string fileName = GetDeletionMapFileName(segId);
    file_system::WriterOption writerParam;
    writerParam.copyOnDump = true;
    file_system::FileWriterPtr writer = directory->CreateFileWriter(fileName, writerParam);
    DumpBitmap(writer, mBitmap, mBitmap->GetValidItemCount());
    writer->Close().GetOrThrow();
}

InMemDeletionMapReaderPtr DeletionMapSegmentWriter::CreateInMemDeletionMapReader(SegmentInfo* segmentInfo,
                                                                                 segmentid_t segId)
{
    return InMemDeletionMapReaderPtr(new InMemDeletionMapReader(mBitmap, segmentInfo, segId));
}

uint32_t* DeletionMapSegmentWriter::GetData() const { return mBitmap->GetData(); }

void DeletionMapSegmentWriter::MountBitmap(const file_system::FileReaderPtr& fileReader, bool needCopy)
{
    DeletionMapFileHeader fileHeader;
    LoadFileHeader(fileReader, fileHeader);

    int64_t fileLength = fileReader->GetLength();
    int64_t expectLength = fileHeader.bitmapDataSize + sizeof(DeletionMapFileHeader);
    if (fileLength != expectLength) {
        const string& filePath = fileReader->GetLogicalPath();
        INDEXLIB_FATAL_ERROR(InconsistentState, "deletion map file incomplete, file path: [%s]", filePath.c_str());
    }

    uint8_t* baseAddress = (uint8_t*)fileReader->GetBaseAddress();
    uint32_t* bitmapMountAddress = (uint32_t*)(baseAddress + sizeof(DeletionMapFileHeader));

    // mount to bitmap. this bitmap should never expand.
    assert(mBitmap == NULL);
    mBitmap = new ExpandableBitmap(false, mPool.get());
    if (needCopy) {
        uint32_t bitmapSlotCount = fileHeader.bitmapDataSize / sizeof(uint32_t);
        uint32_t* tmpBuffer = IE_POOL_COMPATIBLE_NEW_VECTOR(mPool.get(), uint32_t, bitmapSlotCount);
        assert(tmpBuffer);
        memcpy(tmpBuffer, bitmapMountAddress, fileHeader.bitmapDataSize);
        mBitmap->Mount(fileHeader.bitmapItemCount, tmpBuffer, false);
    } else {
        mBitmap->Mount(fileHeader.bitmapItemCount, bitmapMountAddress, true);
        mLoadFileReader = fileReader;
    }
    mOrigDelDocCount = GetDeletedCount();
}

bool DeletionMapSegmentWriter::MergeBitmap(const ExpandableBitmap* src, bool allowExpand)
{
    if (!src) {
        IE_LOG(ERROR, "source bitmap is NULL");
        return false;
    }
    if (!mBitmap) {
        IE_LOG(ERROR, "mBitmap is NULL");
        return false;
    }
    if (src->Size() < mBitmap->Size()) {
        ExpandableBitmap* alignedBitmap = src->Clone();
        alignedBitmap->ReSize(mBitmap->GetItemCount());
        assert(src->Size() == alignedBitmap->Size());
        *mBitmap |= *alignedBitmap;
        delete alignedBitmap;
    } else if (src->Size() > mBitmap->Size()) {
        if (allowExpand) {
            mBitmap->ReSize(mBitmap->GetItemCount());
            assert(src->Size() == mBitmap->Size());
            *mBitmap |= *src;
        } else {
            ExpandableBitmap* alignedBitmap = new ExpandableBitmap;
            alignedBitmap->MountWithoutRefreshSetCount(mBitmap->GetItemCount(), src->GetData());
            assert(alignedBitmap->Size() == mBitmap->Size());
            *mBitmap |= *alignedBitmap;
            delete alignedBitmap;
        }

    } else {
        *mBitmap |= *src;
    }
    return true;
}

file_system::FileReaderPtr DeletionMapSegmentWriter::CreateSliceFileReader(const file_system::DirectoryPtr& directory,
                                                                           const string& fileName, uint32_t docCount)
{
    assert(docCount > 0);
    uint32_t bitmapDumpSize = Bitmap::GetDumpSize(docCount);
    uint32_t sliceLen = bitmapDumpSize + sizeof(DeletionMapFileHeader);

    file_system::FileReaderPtr fileReader = directory->CreateFileReader(fileName, FSOT_SLICE);
    if (fileReader) {
        return fileReader;
    }
    FileWriterPtr fileWriter = directory->CreateFileWriter(fileName, WriterOption::Slice(sliceLen, 1));
    assert(fileWriter);

    DeletionMapFileHeader fileHeader(docCount, bitmapDumpSize);
    DumpFileHeader(fileWriter, fileHeader);

    assert((bitmapDumpSize % sizeof(uint32_t)) == 0);
    uint32_t slotNum = bitmapDumpSize / sizeof(uint32_t);
    uint32_t slotInitValue = 0;
    for (uint32_t i = 0; i < slotNum; i++) {
        fileWriter->Write((void*)(&slotInitValue), sizeof(uint32_t)).GetOrThrow();
    }
    fileReader = directory->CreateFileReader(fileName, FSOT_SLICE);
    fileWriter->Close().GetOrThrow();
    return fileReader;
}

FileReaderPtr DeletionMapSegmentWriter::CreateSliceFileReader(const DirectoryPtr& directory, const string& fileName,
                                                              ExpandableBitmap* bitmap, uint32_t docCount)
{
    assert(bitmap->Size() > 0);
    uint32_t bitmapDumpSize = bitmap->Size();
    uint32_t sliceLen = bitmapDumpSize + sizeof(DeletionMapFileHeader);

    file_system::FileReaderPtr fileReader = directory->CreateFileReader(fileName, FSOT_SLICE);
    if (fileReader) {
        return fileReader;
    }
    FileWriterPtr fileWriter = directory->CreateFileWriter(fileName, WriterOption::Slice(sliceLen, 1));
    assert(fileWriter);
    DumpBitmap(fileWriter, bitmap, docCount);
    fileReader = directory->CreateFileReader(fileName, FSOT_SLICE);
    fileWriter->Close().GetOrThrow();
    return fileReader;
}

void DeletionMapSegmentWriter::DumpFileHeader(const file_system::FileWriterPtr& fileWriter,
                                              const DeletionMapFileHeader& fileHeader)
{
    assert(fileWriter);
    fileWriter->Write((void*)(&fileHeader.bitmapItemCount), sizeof(uint32_t)).GetOrThrow();
    fileWriter->Write((void*)(&fileHeader.bitmapDataSize), sizeof(uint32_t)).GetOrThrow();
}

void DeletionMapSegmentWriter::LoadFileHeader(const file_system::FileReaderPtr& fileReader,
                                              DeletionMapFileHeader& fileHeader)
{
    assert(fileReader);
    uint32_t* baseAddress = (uint32_t*)fileReader->GetBaseAddress();
    fileHeader.bitmapItemCount = baseAddress[0];
    fileHeader.bitmapDataSize = baseAddress[1];
}

string DeletionMapSegmentWriter::GetDeletionMapFileName(segmentid_t segmentId)
{
    stringstream ss;
    ss << DELETION_MAP_FILE_NAME_PREFIX << '_' << segmentId;
    return ss.str();
}

void DeletionMapSegmentWriter::MergeSnapshotIfExist(const FileReaderPtr& fileReader, const DirectoryPtr& directory,
                                                    const string& snapshotFileName)
{
    if (!directory->IsExist(snapshotFileName)) {
        return;
    }

    IE_LOG(INFO, "Begin merge deletionmap file [%s] with snapshot file [%s]", fileReader->DebugString().c_str(),
           snapshotFileName.c_str());
    file_system::FileReaderPtr snapshotFileReader =
        directory->CreateFileReader(snapshotFileName, file_system::FSOT_SLICE);
    assert(snapshotFileReader);
    MergeDeletionMapFile(fileReader, snapshotFileReader);
    IE_LOG(INFO, "End merge deletionmap file [%s] with snapshot file [%s]", fileReader->DebugString().c_str(),
           snapshotFileName.c_str());
}

void DeletionMapSegmentWriter::MergeDeletionMapFile(const file_system::FileReaderPtr& otherFileReader)
{
    if (!mLoadFileReader) {
        INDEXLIB_FATAL_ERROR(UnSupported, "unsupport merge deletionmapfile when "
                                          "segmentwriter has not load self deletionmap");
    }
    if (otherFileReader) {
        MergeDeletionMapFile(mLoadFileReader, otherFileReader);
    }
}

void DeletionMapSegmentWriter::MergeDeletionMapFile(const FileReaderPtr& fileReader,
                                                    const FileReaderPtr& snapshotFileReader)
{
    assert(snapshotFileReader);
    size_t dataSize = GetValidBitmapDataSize(snapshotFileReader);
    if (fileReader->GetLength() < dataSize + sizeof(DeletionMapFileHeader)) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "deletionmap file[%s] length[%lu] less than expect [%lu]",
                             fileReader->DebugString().c_str(), fileReader->GetLength(),
                             dataSize + sizeof(DeletionMapFileHeader));
    }

    MergeBitmapData(fileReader, snapshotFileReader, dataSize);
}

void DeletionMapSegmentWriter::DumpBitmap(const FileWriterPtr& writer, ExpandableBitmap* bitmap, uint32_t itemCount)
{
    assert(bitmap);
    assert(bitmap->GetValidItemCount() >= itemCount);
    size_t reserveSize = sizeof(DeletionMapFileHeader) + bitmap->Size();
    writer->ReserveFile(reserveSize).GetOrThrow();

    DeletionMapFileHeader fileHeader(itemCount, bitmap->Size());
    DumpFileHeader(writer, fileHeader);

    uint32_t* data = bitmap->GetData();
    writer->Write((void*)data, fileHeader.bitmapDataSize).GetOrThrow();
    assert(reserveSize == writer->GetLength());
}

size_t DeletionMapSegmentWriter::GetValidBitmapDataSize(const FileReaderPtr& snapshotFileReader)
{
    if (snapshotFileReader->GetLength() < sizeof(DeletionMapFileHeader)) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "snapshot file[%s] length[%lu] too short!",
                             snapshotFileReader->DebugString().c_str(), snapshotFileReader->GetLength());
    }

    DeletionMapFileHeader* snapshotHeader = (DeletionMapFileHeader*)snapshotFileReader->GetBaseAddress();
    if (snapshotFileReader->GetLength() != snapshotHeader->bitmapDataSize + sizeof(DeletionMapFileHeader)) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "snapshot file[%s] length[%lu] not match [%lu]!",
                             snapshotFileReader->DebugString().c_str(), snapshotFileReader->GetLength(),
                             snapshotHeader->bitmapDataSize + sizeof(DeletionMapFileHeader));
    }
    return Bitmap::GetDumpSize(snapshotHeader->bitmapItemCount);
}

void DeletionMapSegmentWriter::MergeBitmapData(const FileReaderPtr& fileReader, const FileReaderPtr& snapshotFileReader,
                                               size_t dataSize)
{
    uint32_t slotCount = dataSize / sizeof(uint32_t);
    uint32_t* targetAddr = (uint32_t*)((uint8_t*)fileReader->GetBaseAddress() + sizeof(DeletionMapFileHeader));
    uint32_t* snapshotAddr =
        (uint32_t*)((uint8_t*)snapshotFileReader->GetBaseAddress() + sizeof(DeletionMapFileHeader));

    uint32_t batchNum = slotCount / 32;
    uint32_t remainCount = slotCount % 32;
    for (size_t i = 0; i < batchNum; i++) {
        targetAddr[0] |= snapshotAddr[0];
        targetAddr[1] |= snapshotAddr[1];
        targetAddr[2] |= snapshotAddr[2];
        targetAddr[3] |= snapshotAddr[3];
        targetAddr[4] |= snapshotAddr[4];
        targetAddr[5] |= snapshotAddr[5];
        targetAddr[6] |= snapshotAddr[6];
        targetAddr[7] |= snapshotAddr[7];
        targetAddr[8] |= snapshotAddr[8];
        targetAddr[9] |= snapshotAddr[9];
        targetAddr[10] |= snapshotAddr[10];
        targetAddr[11] |= snapshotAddr[11];
        targetAddr[12] |= snapshotAddr[12];
        targetAddr[13] |= snapshotAddr[13];
        targetAddr[14] |= snapshotAddr[14];
        targetAddr[15] |= snapshotAddr[15];
        targetAddr[16] |= snapshotAddr[16];
        targetAddr[17] |= snapshotAddr[17];
        targetAddr[18] |= snapshotAddr[18];
        targetAddr[19] |= snapshotAddr[19];
        targetAddr[20] |= snapshotAddr[20];
        targetAddr[21] |= snapshotAddr[21];
        targetAddr[22] |= snapshotAddr[22];
        targetAddr[23] |= snapshotAddr[23];
        targetAddr[24] |= snapshotAddr[24];
        targetAddr[25] |= snapshotAddr[25];
        targetAddr[26] |= snapshotAddr[26];
        targetAddr[27] |= snapshotAddr[27];
        targetAddr[28] |= snapshotAddr[28];
        targetAddr[29] |= snapshotAddr[29];
        targetAddr[30] |= snapshotAddr[30];
        targetAddr[31] |= snapshotAddr[31];

        targetAddr += 32;
        snapshotAddr += 32;
    }
    for (size_t i = 0; i < remainCount; i++) {
        targetAddr[i] |= snapshotAddr[i];
    }
}
}} // namespace indexlib::index
