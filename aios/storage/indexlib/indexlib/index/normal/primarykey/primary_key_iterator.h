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
#ifndef __INDEXLIB_PRIMARY_KEY_ITERATOR_H
#define __INDEXLIB_PRIMARY_KEY_ITERATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/normal/primarykey/primary_key_pair.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(file_system, FileReader);

namespace indexlib { namespace index {

template <typename Key>
class PrimaryKeyIterator
{
private:
    typedef PKPair<Key> TypedPKPair;

public:
    PrimaryKeyIterator(const config::IndexConfigPtr& indexConfig) : mIndexConfig(indexConfig) {}
    virtual ~PrimaryKeyIterator() {}

public:
    virtual void Init(const std::vector<index_base::SegmentData>& segmentDatas) = 0;

    virtual uint64_t GetPkCount() const = 0;
    virtual uint64_t GetDocCount() const = 0;

    virtual bool HasNext() const = 0;
    // docid in pkPair is globle docid
    virtual void Next(TypedPKPair& pkPair) = 0;

protected:
    file_system::FileReaderPtr OpenPKDataFile(const file_system::DirectoryPtr& segmentDirectory) const;

protected:
    config::IndexConfigPtr mIndexConfig;
};

template <typename Key>
file_system::FileReaderPtr
PrimaryKeyIterator<Key>::OpenPKDataFile(const file_system::DirectoryPtr& segmentDirectory) const
{
    assert(segmentDirectory);
    file_system::DirectoryPtr indexDirectory = segmentDirectory->GetDirectory(INDEX_DIR_NAME, true);
    assert(indexDirectory);
    file_system::DirectoryPtr pkDirectory = indexDirectory->GetDirectory(mIndexConfig->GetIndexName(), true);
    assert(pkDirectory);
    file_system::FileReaderPtr fileReader = pkDirectory->CreateFileReader(
        PRIMARY_KEY_DATA_FILE_NAME, file_system::ReaderOption::NoCache(file_system::FSOT_BUFFERED));
    assert(fileReader);
    return fileReader;
}
}} // namespace indexlib::index

#endif //__INDEXLIB_PRIMARY_KEY_ITERATOR_H
