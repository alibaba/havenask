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
#include "indexlib/index/normal/inverted_index/builtin_index/range/on_disk_range_index_iterator_creator.h"

using namespace std;

namespace indexlib { namespace index { namespace legacy {
IE_LOG_SETUP(index, OnDiskRangeIndexIteratorCreator);

OnDiskIndexIterator* OnDiskRangeIndexIteratorCreator::Create(const index_base::SegmentData& segData) const
{
    file_system::DirectoryPtr parentDirectory = segData.GetIndexDirectory(mParentIndexName, false);
    if (!parentDirectory) {
        return NULL;
    }

    file_system::DirectoryPtr indexDirectory = parentDirectory->GetDirectory(mIndexConfig->GetIndexName(), false);
    if (indexDirectory && indexDirectory->IsExist(DICTIONARY_FILE_NAME)) {
        return (new OnDiskRangeIndexIterator(mIndexConfig, indexDirectory, mPostingFormatOption, mIOConfig));
    }
    return NULL;
}
}}} // namespace indexlib::index::legacy
