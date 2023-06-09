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
#ifndef __INDEXLIB_ON_DISK_RANGE_INDEX_ITERATOR_CREATOR_H
#define __INDEXLIB_ON_DISK_RANGE_INDEX_ITERATOR_CREATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/on_disk_index_iterator_creator.h"
#include "indexlib/index/normal/inverted_index/builtin_index/pack/on_disk_pack_index_iterator.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index { namespace legacy {

class OnDiskRangeIndexIteratorCreator : public OnDiskIndexIteratorCreator
{
public:
    typedef OnDiskPackIndexIterator OnDiskRangeIndexIterator;
    OnDiskRangeIndexIteratorCreator(const PostingFormatOption postingFormatOption,
                                    const config::MergeIOConfig& ioConfig, const config::IndexConfigPtr& indexConfig,
                                    const std::string& parentIndexName)
        : mPostingFormatOption(postingFormatOption)
        , mIOConfig(ioConfig)
        , mIndexConfig(indexConfig)
        , mParentIndexName(parentIndexName)
    {
    }

    ~OnDiskRangeIndexIteratorCreator() {}

public:
    OnDiskIndexIterator* Create(const index_base::SegmentData& segData) const override;

    OnDiskIndexIterator* CreateBitmapIterator(const file_system::DirectoryPtr& indexDirectory) const override
    {
        assert(false);
        return NULL;
    }

private:
    PostingFormatOption mPostingFormatOption;
    config::MergeIOConfig mIOConfig;
    config::IndexConfigPtr mIndexConfig;
    std::string mParentIndexName;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OnDiskRangeIndexIteratorCreator);
///////////////////////////////////////////////////////////
}}} // namespace indexlib::index::legacy

#endif //__INDEXLIB_ON_DISK_RANGE_INDEX_ITERATOR_CREATOR_H
