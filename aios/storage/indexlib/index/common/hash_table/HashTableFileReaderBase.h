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

#include "autil/ConstString.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/common/hash_table/HashTableBase.h"
#include "indexlib/util/Status.h"

namespace indexlibv2::index {

class HashTableFileReaderBase : public HashTableAccessor
{
public:
    HashTableFileReaderBase();
    virtual ~HashTableFileReaderBase();

public:
    virtual FL_LAZY(indexlib::util::Status)
        Find(uint64_t key, autil::StringView& value, indexlib::util::BlockAccessCounter* blockCounter,
             autil::mem_pool::Pool* pool, autil::TimeoutTerminator* timeoutTerminator) const = 0;
    virtual bool Init(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                      const indexlib::file_system::FileReaderPtr& fileReader) = 0;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
