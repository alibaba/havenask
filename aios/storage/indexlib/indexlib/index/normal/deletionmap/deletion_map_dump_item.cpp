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
#include "indexlib/index/normal/deletionmap/deletion_map_dump_item.h"

#include "indexlib/index/normal/deletionmap/deletion_map_segment_writer.h"

using namespace std;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, DeletionMapDumpItem);

DeletionMapDumpItem::DeletionMapDumpItem(const file_system::DirectoryPtr& directory,
                                         const DeletionMapSegmentWriterPtr& writer, segmentid_t segId)
    : mDirectory(directory)
    , mWriter(writer)
    , mSegmentId(segId)
{
}

DeletionMapDumpItem::~DeletionMapDumpItem() {}

void DeletionMapDumpItem::process(autil::mem_pool::PoolBase* pool) { mWriter->Dump(mDirectory, mSegmentId); }

void DeletionMapDumpItem::destroy() { delete this; }

void DeletionMapDumpItem::drop() { destroy(); }
}} // namespace indexlib::index
