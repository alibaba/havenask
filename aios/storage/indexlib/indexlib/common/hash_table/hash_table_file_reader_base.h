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
#ifndef __INDEXLIB_HASH_TABLE_FILE_READER_BASE_H
#define __INDEXLIB_HASH_TABLE_FILE_READER_BASE_H

#include <memory>

#include "autil/ConstString.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/common/hash_table/hash_table_base.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Status.h"

namespace indexlib { namespace common {

class HashTableFileReaderBase : public HashTableAccessor
{
public:
    HashTableFileReaderBase();
    virtual ~HashTableFileReaderBase();

public:
    virtual FL_LAZY(util::Status)
        Find(uint64_t key, autil::StringView& value, util::BlockAccessCounter* blockCounter,
             autil::mem_pool::Pool* pool, autil::TimeoutTerminator* timeoutTerminator) const = 0;
    virtual bool Init(const file_system::DirectoryPtr& directory, const file_system::FileReaderPtr& fileReader) = 0;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(HashTableFileReaderBase);
}} // namespace indexlib::common

#endif //__INDEXLIB_HASH_TABLE_FILE_READER_BASE_H
