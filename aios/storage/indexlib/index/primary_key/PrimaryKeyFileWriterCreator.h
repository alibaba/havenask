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

#include "indexlib/index/primary_key/BlockPrimaryKeyFileWriter.h"
#include "indexlib/index/primary_key/HashPrimaryKeyFileWriter.h"
#include "indexlib/index/primary_key/PrimaryKeyFileWriter.h"
#include "indexlib/index/primary_key/SortedPrimaryKeyFileWriter.h"
#include "indexlib/index/primary_key/config/PrimaryKeyIndexConfig.h"

namespace indexlibv2 { namespace index {

template <typename Key>
class PrimaryKeyFileWriterCreator
{
public:
    typedef std::shared_ptr<PrimaryKeyFileWriter<Key>> PrimaryKeyFileWriterPtr;

public:
    PrimaryKeyFileWriterCreator() {}
    ~PrimaryKeyFileWriterCreator() {}

public:
    static PrimaryKeyFileWriterPtr
    CreatePKFileWriter(const std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig>& indexConfig);

private:
    AUTIL_LOG_DECLARE();
};

///////////////////////////////////////////////////////////////
template <typename Key>
typename PrimaryKeyFileWriterCreator<Key>::PrimaryKeyFileWriterPtr PrimaryKeyFileWriterCreator<Key>::CreatePKFileWriter(
    const std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig>& indexConfig)
{
    PrimaryKeyFileWriterPtr pkFileWriter;
    assert(indexConfig);
    switch (indexConfig->GetPrimaryKeyIndexType()) {
    case pk_hash_table:
        return PrimaryKeyFileWriterPtr(new HashPrimaryKeyFileWriter<Key>());
    case pk_sort_array:
        return PrimaryKeyFileWriterPtr(new SortedPrimaryKeyFileWriter<Key>());
    case pk_block_array:
        return PrimaryKeyFileWriterPtr(new BlockPrimaryKeyFileWriter<Key>(indexConfig->GetPrimaryKeyDataBlockSize()));
    default:
        assert(false);
        return PrimaryKeyFileWriterPtr();
    }
    return PrimaryKeyFileWriterPtr();
}
}} // namespace indexlibv2::index
