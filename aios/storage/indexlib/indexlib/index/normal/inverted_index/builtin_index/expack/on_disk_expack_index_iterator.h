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
#ifndef __INDEXLIB_ON_DISK_EXPACK_INDEX_ITERATOR_H
#define __INDEXLIB_ON_DISK_EXPACK_INDEX_ITERATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/merge_io_config.h"
#include "indexlib/index/inverted_index/format/PostingDecoderImpl.h"
#include "indexlib/index/normal/inverted_index/builtin_index/pack/on_disk_pack_index_iterator.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index { namespace legacy {

class OnDiskExpackIndexIterator : public OnDiskPackIndexIterator
{
public:
    OnDiskExpackIndexIterator(const config::IndexConfigPtr& indexConfig,
                              const file_system::DirectoryPtr& indexDirectory,
                              const PostingFormatOption& postingFormatOption,
                              const config::MergeIOConfig& ioConfig = config::MergeIOConfig());

    virtual ~OnDiskExpackIndexIterator();

    class Creator : public OnDiskIndexIteratorCreator
    {
    public:
        Creator(const PostingFormatOption postingFormatOption, const config::MergeIOConfig& ioConfig,
                const config::IndexConfigPtr& indexConfig)
            : mPostingFormatOption(postingFormatOption)
            , mIOConfig(ioConfig)
            , mIndexConfig(indexConfig)
        {
        }
        OnDiskIndexIterator* Create(const index_base::SegmentData& segData) const
        {
            file_system::DirectoryPtr indexDirectory = segData.GetIndexDirectory(mIndexConfig->GetIndexName(), false);
            if (indexDirectory && indexDirectory->IsExist(DICTIONARY_FILE_NAME)) {
                return (new OnDiskExpackIndexIterator(mIndexConfig, indexDirectory, mPostingFormatOption, mIOConfig));
            }
            return NULL;
        }

    private:
        PostingFormatOption mPostingFormatOption;
        config::MergeIOConfig mIOConfig;
        config::IndexConfigPtr mIndexConfig;
    };

protected:
    void CreatePostingDecoder() override { mDecoder.reset(new PostingDecoderImpl(_postingFormatOption)); }

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OnDiskExpackIndexIterator);
}}} // namespace indexlib::index::legacy

#endif //__INDEXLIB_ON_DISK_EXPACK_INDEX_ITERATOR_H
