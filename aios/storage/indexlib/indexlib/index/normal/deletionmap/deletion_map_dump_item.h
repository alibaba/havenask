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
#ifndef __INDEXLIB_DELETION_MAP_DUMP_ITEM_H
#define __INDEXLIB_DELETION_MAP_DUMP_ITEM_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/util/dump_item_typed.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, DeletionMapSegmentWriter);
DECLARE_REFERENCE_CLASS(file_system, Directory);

namespace indexlib { namespace index {

class DeletionMapDumpItem : public common::DumpItem
{
public:
    DeletionMapDumpItem(const file_system::DirectoryPtr& directory, const DeletionMapSegmentWriterPtr& writer,
                        segmentid_t segId);

    ~DeletionMapDumpItem();

public:
    void process(autil::mem_pool::PoolBase* pool) override;
    void destroy() override;
    void drop() override;

private:
    file_system::DirectoryPtr mDirectory;
    DeletionMapSegmentWriterPtr mWriter;
    segmentid_t mSegmentId;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DeletionMapDumpItem);
}} // namespace indexlib::index

#endif //__INDEXLIB_DELETION_MAP_DUMP_ITEM_H
