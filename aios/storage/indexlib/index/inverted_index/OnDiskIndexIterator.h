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

#include "fslib/common/common_type.h"
#include "indexlib/file_system/fslib/IoConfig.h"
#include "indexlib/index/inverted_index/IndexIterator.h"
#include "indexlib/index/inverted_index/format/PostingFormatOption.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryReader.h"

namespace indexlib::index {

class OnDiskIndexIterator : public IndexIterator
{
public:
    OnDiskIndexIterator(const file_system::DirectoryPtr& indexDirectory, const PostingFormatOption& postingFormatOption,
                        const file_system::IOConfig& ioConfig = file_system::IOConfig())
        : _indexDirectory(indexDirectory)
        , _postingFormatOption(postingFormatOption)
        , _ioConfig(ioConfig)
    {
    }

    virtual ~OnDiskIndexIterator() {}

public:
    virtual void Init() = 0;
    virtual size_t GetPostingFileLength() const = 0;

protected:
    file_system::DirectoryPtr _indexDirectory;
    PostingFormatOption _postingFormatOption;
    file_system::IOConfig _ioConfig;
};

} // namespace indexlib::index
