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
#ifndef __INDEXLIB_DELETION_MAP_SEGMENT_WRITER_H
#define __INDEXLIB_DELETION_MAP_SEGMENT_WRITER_H

#include <memory>

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/ExpandableBitmap.h"

DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
DECLARE_REFERENCE_STRUCT(index_base, SegmentInfo);
DECLARE_REFERENCE_CLASS(file_system, FileReader);
DECLARE_REFERENCE_CLASS(file_system, FileWriter);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(util, MMapAllocator);
DECLARE_REFERENCE_CLASS(util, BuildResourceMetrics);
DECLARE_REFERENCE_CLASS(util, BuildResourceMetricsNode);
DECLARE_REFERENCE_CLASS(index, InMemDeletionMapReader);

namespace indexlib { namespace index {

class DeletionMapSegmentWriter
{
public:
    struct DeletionMapFileHeader {
        DeletionMapFileHeader(uint32_t itemCount = 0, uint32_t dataSize = 0)
            : bitmapItemCount(itemCount)
            , bitmapDataSize(dataSize)
        {
        }
        uint32_t bitmapItemCount;
        uint32_t bitmapDataSize;
    };

public:
    DeletionMapSegmentWriter();
    ~DeletionMapSegmentWriter();

public:
    // for building segment write
    void Init(uint32_t initDocCount, util::BuildResourceMetrics* buildResourceMetrics = NULL);

    // for built segment. init from file
    void Init(const file_system::DirectoryPtr& directory, const std::string& fileName, bool needCopy);

    // for built segment. init deletionmap from docCount when file not exist
    void Init(const file_system::DirectoryPtr& directory, segmentid_t segmentId, uint32_t docCount, bool needCopy);

    // for dumping in memory segment
    void Init(const index_base::InMemorySegmentPtr& inMemSegment, bool needCopy);

    void Delete(docid_t docId);
    void EndSegment(uint32_t docCount);
    void Dump(const file_system::DirectoryPtr& directory, segmentid_t segId, bool force = false);
    bool IsDirty() const;
    uint32_t GetDeletedCount() const;
    uint32_t* GetData() const;

    InMemDeletionMapReaderPtr CreateInMemDeletionMapReader(index_base::SegmentInfo* segmentInfo, segmentid_t segId);

    static uint32_t GetDeletionMapSize() { return DEFAULT_BITMAP_SIZE; }
    static std::string GetDeletionMapFileName(segmentid_t segmentId);
    void MergeDeletionMapFile(const file_system::FileReaderPtr& otherFileReader);
    const util::ExpandableBitmap* GetBitmap() const { return mBitmap; }
    bool MergeBitmap(const util::ExpandableBitmap* src, bool allowExpand);

public:
    // aone 40392358
    static void TEST_SetDeletionMapSize(uint32_t size) { DEFAULT_BITMAP_SIZE = size; };

private:
    file_system::FileReaderPtr CreateSliceFileReader(const file_system::DirectoryPtr& directory,
                                                     const std::string& fileName, uint32_t docCount);

    file_system::FileReaderPtr CreateSliceFileReader(const file_system::DirectoryPtr& directory,
                                                     const std::string& fileName, util::ExpandableBitmap* bitmap,
                                                     uint32_t docCount);

    void MergeSnapshotIfExist(const file_system::FileReaderPtr& fileReader, const file_system::DirectoryPtr& directory,
                              const std::string& snapshotFileName);

    void MountBitmap(const file_system::FileReaderPtr& fileReader, bool needCopy);

    void LoadFileHeader(const file_system::FileReaderPtr& fileReader, DeletionMapFileHeader& fileHeader);

    void UpdateBuildResourceMetrics();

    void MergeBitmapData(const file_system::FileReaderPtr& fileReader,
                         const file_system::FileReaderPtr& snapshotFileReader, size_t dataSize);

    void MergeDeletionMapFile(const file_system::FileReaderPtr& fileReader,
                              const file_system::FileReaderPtr& otherFileReader);

    size_t GetValidBitmapDataSize(const file_system::FileReaderPtr& fileReader);

    static void DumpFileHeader(const file_system::FileWriterPtr& fileWriter, const DeletionMapFileHeader& fileHeader);

    static void DumpBitmap(const file_system::FileWriterPtr& fileWriter, util::ExpandableBitmap* bitmap,
                           uint32_t itemCount);

private:
    static uint32_t DEFAULT_BITMAP_SIZE;

private:
    uint32_t mOrigDelDocCount;
    file_system::FileReaderPtr mLoadFileReader;
    util::ExpandableBitmap* mBitmap;
    util::MMapAllocatorPtr mAllocator;
    std::shared_ptr<autil::mem_pool::Pool> mPool;
    util::BuildResourceMetricsNode* mBuildResourceMetricsNode;

private:
    IE_LOG_DECLARE();
};

inline bool DeletionMapSegmentWriter::IsDirty() const { return mOrigDelDocCount != GetDeletedCount(); }

inline void DeletionMapSegmentWriter::Delete(docid_t docId)
{
    mBitmap->Set(docId);
    UpdateBuildResourceMetrics();
}

inline uint32_t DeletionMapSegmentWriter::GetDeletedCount() const
{
    if (!mBitmap) {
        return 0;
    }
    return mBitmap->GetSetCount();
}

DEFINE_SHARED_PTR(DeletionMapSegmentWriter);
}} // namespace indexlib::index

#endif //__INDEXLIB_DELETION_MAP_SEGMENT_WRITER_H
