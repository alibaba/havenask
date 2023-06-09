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
#include "indexlib/index/inverted_index/builtin_index/range/OnDiskRangeIndexIteratorCreator.h"

#include "indexlib/index/inverted_index/Common.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, OnDiskRangeIndexIteratorCreator);

OnDiskRangeIndexIteratorCreator::OnDiskRangeIndexIteratorCreator(
    const PostingFormatOption postingFormatOption, const file_system::IOConfig& ioConfig,
    const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig, const std::string& parentIndexName)
    : _postingFormatOption(postingFormatOption)
    , _ioConfig(ioConfig)
    , _indexConfig(indexConfig)
    , _parentIndexName(parentIndexName)

{
}

OnDiskRangeIndexIteratorCreator::~OnDiskRangeIndexIteratorCreator() {}

OnDiskIndexIterator*
OnDiskRangeIndexIteratorCreator::Create(const std::shared_ptr<file_system::Directory>& segmentDir) const
{
    auto subDir = segmentDir->GetDirectory(INVERTED_INDEX_PATH, /*throwExceptionIfNotExist=*/false);
    if (!subDir) {
        return nullptr;
    }
    auto parentDir = subDir->GetDirectory(_parentIndexName, /*throwExceptionIfNotExist=*/false);
    if (!parentDir) {
        return nullptr;
    }
    auto indexDirectory = parentDir->GetDirectory(_indexConfig->GetIndexName(), /*throwExceptionIfNotExist=*/false);
    if (indexDirectory && indexDirectory->IsExist(DICTIONARY_FILE_NAME)) {
        return (new OnDiskRangeIndexIterator(_indexConfig, indexDirectory, _postingFormatOption, _ioConfig));
    }
    return nullptr;
}

OnDiskIndexIterator* OnDiskRangeIndexIteratorCreator::CreateBitmapIterator(
    const std::shared_ptr<file_system::Directory>& indexDirectory) const
{
    assert(false);
    return nullptr;
}

} // namespace indexlib::index
