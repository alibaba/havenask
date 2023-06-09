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

#include "autil/Log.h"
#include "indexlib/index/inverted_index/OnDiskIndexIteratorCreator.h"
#include "indexlib/index/inverted_index/builtin_index/pack/OnDiskPackIndexIterator.h"

namespace indexlib::index {

class OnDiskRangeIndexIteratorCreator : public OnDiskIndexIteratorCreator
{
public:
    typedef OnDiskPackIndexIterator OnDiskRangeIndexIterator;
    OnDiskRangeIndexIteratorCreator(const PostingFormatOption postingFormatOption,
                                    const file_system::IOConfig& ioConfig,
                                    const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                                    const std::string& parentIndexName);
    ~OnDiskRangeIndexIteratorCreator();

    OnDiskIndexIterator* Create(const std::shared_ptr<indexlib::file_system::Directory>& segmentDir) const override;

    OnDiskIndexIterator* CreateByIndexDir(const std::shared_ptr<file_system::Directory>& indexDirectory) const override
    {
        return nullptr;
    }

    OnDiskIndexIterator*
    CreateBitmapIterator(const std::shared_ptr<file_system::Directory>& indexDirectory) const override;

private:
    PostingFormatOption _postingFormatOption;
    file_system::IOConfig _ioConfig;
    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> _indexConfig;
    std::string _parentIndexName;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
