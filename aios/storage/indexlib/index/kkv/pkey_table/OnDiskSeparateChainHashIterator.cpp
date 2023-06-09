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
#include "indexlib/index/kkv/pkey_table/OnDiskSeparateChainHashIterator.h"

#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/kkv/Constant.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::file_system;

namespace indexlibv2::index {

AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib.index, OnDiskSeparateChainHashIterator);

OnDiskSeparateChainHashIterator::OnDiskSeparateChainHashIterator() : _size(0), _cursor(0), _key(0) {}

OnDiskSeparateChainHashIterator::~OnDiskSeparateChainHashIterator() {}

void OnDiskSeparateChainHashIterator::Open(const std::shared_ptr<indexlibv2::config::KKVIndexConfig>& config,
                                           const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory)
{
    assert(config);
    auto readOption = indexlib::file_system::ReaderOption::CacheFirst(indexlib::file_system::FSOT_BUFFERED);
    _fileReader = indexDirectory->CreateFileReader(PREFIX_KEY_FILE_NAME, readOption).GetOrThrow();
    uint32_t bucketCount = 0;
    HashTable::DecodeMeta(_fileReader, _size, bucketCount);
    _cursor = 0;
    if (_size > 0) {
        ReadCurrentKeyValue();
    }
}

bool OnDiskSeparateChainHashIterator::IsValid() const { return _cursor < _size; }

void OnDiskSeparateChainHashIterator::MoveToNext()
{
    ++_cursor;
    if (_cursor < _size) {
        ReadCurrentKeyValue();
    }
}

void OnDiskSeparateChainHashIterator::SortByKey()
{
    AUTIL_LOG(INFO, "sort will do nothing for on-disk separate chain hash table");
}

size_t OnDiskSeparateChainHashIterator::Size() const { return _size; }

uint64_t OnDiskSeparateChainHashIterator::Key() const { return _key; }

OnDiskPKeyOffset OnDiskSeparateChainHashIterator::Value() const { return _value; }

void OnDiskSeparateChainHashIterator::ReadCurrentKeyValue()
{
    size_t cursor = _cursor * sizeof(HashNode);
    assert(_fileReader);
    HashNode node;
    _fileReader->Read(&node, sizeof(HashNode), cursor).GetOrThrow();
    _key = node.key;
    _value = node.value;
}

} // namespace indexlibv2::index
