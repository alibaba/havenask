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
#include "indexlib/index/normal/trie/trie_index_segment_reader.h"

#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/index/normal/trie/trie_index_define.h"
#include "indexlib/index_define.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::file_system;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, TrieIndexSegmentReader);

DirectoryPtr TrieIndexSegmentReader::GetDirectory(const DirectoryPtr& segmentDirectory,
                                                  const IndexConfigPtr& indexConfig)
{
    assert(segmentDirectory);
    DirectoryPtr indexDirectory = segmentDirectory->GetDirectory(INDEX_DIR_NAME, true);
    assert(indexDirectory);
    return indexDirectory->GetDirectory(indexConfig->GetIndexName(), true);
}

void TrieIndexSegmentReader::Open(const IndexConfigPtr& indexConfig, const SegmentData& segmentData)
{
    assert(indexConfig);
    DirectoryPtr directory = GetDirectory(segmentData.GetDirectory(), indexConfig);
    assert(directory);
    mDataFile = directory->CreateFileReader(PRIMARY_KEY_DATA_FILE_NAME, FSOT_MMAP);
    assert(mDataFile);
    uint8_t* trieIndexVersion = (uint8_t*)mDataFile->GetBaseAddress();
    assert(*trieIndexVersion == TRIE_INDEX_VERSION);
    mData = (char*)(trieIndexVersion + 1);
    assert(mData);
}
}} // namespace indexlib::index
