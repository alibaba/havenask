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
#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, DeletionMapSegmentWriter);
DECLARE_REFERENCE_CLASS(index, InMemDeletionMapReader);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index_base, SegmentData);
DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(util, Bitmap);
DECLARE_REFERENCE_CLASS(util, ExpandableBitmap);

namespace indexlib { namespace index {

class DeletionMapWriter
{
public:
    typedef index_base::SegmentData SegmentData;
    typedef std::pair<SegmentData, DeletionMapSegmentWriterPtr> SegmentWriter;

public:
    // needCopy = false : use copy data for single deletionmap file,
    //     no shared for patch modifier
    // needCopy = true : use fileNode in fileSystem for single deletionmap file
    //     shared for inplace modifier
    DeletionMapWriter(bool needCopy);
    virtual ~DeletionMapWriter();

public:
    void Init(index_base::PartitionData* partitionData);
    bool Delete(docid_t docId);
    void Dump(const file_system::DirectoryPtr& directory);
    bool IsDirty() const;

    util::BitmapPtr CreateGlobalBitmap();

    void GetPatchedSegmentIds(std::vector<segmentid_t>& patchSegIds) const;
    DeletionMapSegmentWriterPtr GetSegmentWriter(segmentid_t segId) const;
    const util::ExpandableBitmap* GetSegmentDeletionMap(segmentid_t segId) const;

private:
    DeletionMapSegmentWriterPtr CreateInMemSharedSegmentWriter(const SegmentData& segData);

    DeletionMapSegmentWriterPtr
    CreateDumpingDeletionMapSegmentWriter(const index_base::InMemorySegmentPtr& inMemSegment);

protected:
    std::vector<SegmentWriter> mSegmentWriters;
    std::map<segmentid_t, uint32_t> mSegIdToIdx;
    InMemDeletionMapReaderPtr mBuildingSegmentReader;
    bool mNeedCopy;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DeletionMapWriter);
}} // namespace indexlib::index
