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
#include "indexlib/index/normal/inverted_index/builtin_index/expack/on_disk_expack_index_iterator.h"

#include "fslib/fs/FileSystem.h"

using namespace std;

using namespace indexlib::file_system;

namespace indexlib { namespace index { namespace legacy {
IE_LOG_SETUP(index, OnDiskExpackIndexIterator);

OnDiskExpackIndexIterator::OnDiskExpackIndexIterator(const config::IndexConfigPtr& indexConfig,
                                                     const DirectoryPtr& indexDirectory,
                                                     const PostingFormatOption& postingFormatOption,
                                                     const config::MergeIOConfig& ioConfig)
    : OnDiskPackIndexIterator(indexConfig, indexDirectory, postingFormatOption, ioConfig)
{
}

OnDiskExpackIndexIterator::~OnDiskExpackIndexIterator() {}
}}} // namespace indexlib::index::legacy
