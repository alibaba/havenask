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
#ifndef __INDEXLIB_ON_DISK_INDEX_ITERATOR_CREATOR_H
#define __INDEXLIB_ON_DISK_INDEX_ITERATOR_CREATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/merge_io_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/inverted_index/OnDiskIndexIterator.h"
#include "indexlib/index/inverted_index/format/PostingFormatOption.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index { namespace legacy {

class OnDiskIndexIteratorCreator
{
public:
    OnDiskIndexIteratorCreator();
    virtual ~OnDiskIndexIteratorCreator();

public:
    virtual OnDiskIndexIterator* Create(const index_base::SegmentData& segmentData) const = 0;

    virtual OnDiskIndexIterator* CreateBitmapIterator(const file_system::DirectoryPtr& indexDirectory) const;
};

DEFINE_SHARED_PTR(OnDiskIndexIteratorCreator);
}}} // namespace indexlib::index::legacy

#endif //__INDEXLIB_ON_DISK_INDEX_ITERATOR_CREATOR_H
