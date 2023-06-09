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
#include "indexlib/index/kv/KeyMergeWriter.h"

#include "indexlib/index/common/hash_table/HashTableBase.h"
#include "indexlib/index/common/hash_table/HashTableOptions.h"

namespace indexlibv2::index {

KeyMergeWriter::KeyMergeWriter() {}

KeyMergeWriter::~KeyMergeWriter() {}

Status KeyMergeWriter::Add(uint64_t key, const autil::StringView& value, uint32_t timestamp)
{
    auto s = KeyWriter::Add(key, value, timestamp);
    if (s.IsOK()) {
        return s;
    }
    if (!_hashTable->Stretch()) {
        return Status::InternalError("stretch hash table failed");
    }
    return KeyWriter::Add(key, value, timestamp);
}

Status KeyMergeWriter::Delete(uint64_t key, uint32_t timestamp)
{
    auto s = KeyWriter::Delete(key, timestamp);
    if (s.IsOK()) {
        return s;
    }
    if (!_hashTable->Stretch()) {
        return Status::InternalError("stretch hash table failed");
    }
    return KeyWriter::Delete(key, timestamp);
}

void KeyMergeWriter::FillHashTableOptions(HashTableOptions& opts) const { opts.mayStretch = true; }

} // namespace indexlibv2::index
