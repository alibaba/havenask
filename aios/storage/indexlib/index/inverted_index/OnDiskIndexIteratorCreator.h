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

#include "indexlib/file_system/Directory.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/OnDiskIndexIterator.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/OnDiskBitmapIndexIterator.h"
#include "indexlib/index/inverted_index/format/PostingFormatOption.h"

namespace indexlib::index {
// TODO(makuo.mnb) remove v2 suffix when v2 migrate completed
#define DECLARE_ON_DISK_INDEX_ITERATOR_CREATOR_V2(classname)                                                           \
    class Creator : public OnDiskIndexIteratorCreator                                                                  \
    {                                                                                                                  \
    public:                                                                                                            \
        Creator(const PostingFormatOption& postingFormatOption, const file_system::IOConfig& ioConfig,                 \
                const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig)                           \
            : _postingFormatOption(postingFormatOption)                                                                \
            , _ioConfig(ioConfig)                                                                                      \
            , _indexConfig(indexConfig)                                                                                \
        {                                                                                                              \
        }                                                                                                              \
        OnDiskIndexIterator* Create(const std::shared_ptr<file_system::Directory>& segDir) const override              \
        {                                                                                                              \
            auto subDir = segDir->GetDirectory(INVERTED_INDEX_PATH, /*throwExceptionIfNotExist=*/false);               \
            if (!subDir) {                                                                                             \
                return nullptr;                                                                                        \
            }                                                                                                          \
            auto indexDirectory =                                                                                      \
                subDir->GetDirectory(_indexConfig->GetIndexName(), /*throwExceptionIfNotExist=*/false);                \
            return CreateByIndexDir(indexDirectory);                                                                   \
        }                                                                                                              \
        OnDiskIndexIterator*                                                                                           \
        CreateByIndexDir(const std::shared_ptr<file_system::Directory>& indexDirectory) const override                 \
        {                                                                                                              \
            if (indexDirectory && indexDirectory->IsExist(DICTIONARY_FILE_NAME)) {                                     \
                return (new classname(_indexConfig, indexDirectory, _postingFormatOption, _ioConfig));                 \
            }                                                                                                          \
            return nullptr;                                                                                            \
        }                                                                                                              \
                                                                                                                       \
    private:                                                                                                           \
        PostingFormatOption _postingFormatOption;                                                                      \
        file_system::IOConfig _ioConfig;                                                                               \
        std::shared_ptr<indexlibv2::config::InvertedIndexConfig> _indexConfig;                                         \
    };

class OnDiskIndexIteratorCreator
{
public:
    OnDiskIndexIteratorCreator() = default;
    virtual ~OnDiskIndexIteratorCreator() = default;

public:
    virtual OnDiskIndexIterator* Create(const std::shared_ptr<file_system::Directory>& segmentDir) const = 0;

    virtual OnDiskIndexIterator* CreateByIndexDir(const std::shared_ptr<file_system::Directory>& indexDir) const = 0;

    virtual OnDiskIndexIterator*
    CreateBitmapIterator(const std::shared_ptr<file_system::Directory>& indexDirectory) const
    {
        return new OnDiskBitmapIndexIterator(indexDirectory);
    }
};

} // namespace indexlib::index
