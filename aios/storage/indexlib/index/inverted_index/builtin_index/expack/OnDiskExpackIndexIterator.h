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

#include "indexlib/file_system/fslib/IoConfig.h"
#include "indexlib/index/inverted_index/builtin_index/pack/OnDiskPackIndexIterator.h"
#include "indexlib/index/inverted_index/format/PostingDecoderImpl.h"

namespace indexlib::index {

class OnDiskExpackIndexIterator : public OnDiskPackIndexIteratorTyped<dictkey_t>
{
public:
    OnDiskExpackIndexIterator(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                              const std::shared_ptr<file_system::Directory>& indexDirectory,
                              const PostingFormatOption& postingFormatOption,
                              const file_system::IOConfig& ioConfig = file_system::IOConfig())
        : OnDiskPackIndexIteratorTyped<dictkey_t>(indexConfig, indexDirectory, postingFormatOption, ioConfig)
    {
    }

    virtual ~OnDiskExpackIndexIterator() = default;

    DECLARE_ON_DISK_INDEX_ITERATOR_CREATOR_V2(OnDiskExpackIndexIterator);

protected:
    void CreatePostingDecoder() override { _decoder.reset(new PostingDecoderImpl(_postingFormatOption)); }

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
