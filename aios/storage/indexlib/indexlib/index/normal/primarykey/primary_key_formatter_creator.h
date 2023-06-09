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
#ifndef __INDEXLIB_PRIMARY_KEY_FORMATTER_CREATOR_H
#define __INDEXLIB_PRIMARY_KEY_FORMATTER_CREATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/index/normal/primarykey/block_primary_key_formatter.h"
#include "indexlib/index/normal/primarykey/hash_primary_key_formatter.h"
#include "indexlib/index/normal/primarykey/primary_key_formatter.h"
#include "indexlib/index/normal/primarykey/sorted_primary_key_formatter.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename Key>
class PrimaryKeyFormatterCreator
{
public:
    typedef std::shared_ptr<PrimaryKeyFormatter<Key>> PrimaryKeyFormatterPtr;

public:
    PrimaryKeyFormatterCreator() {}
    ~PrimaryKeyFormatterCreator() {}

public:
    static PrimaryKeyFormatterPtr CreatePKFormatterByLoadMode(const config::PrimaryKeyIndexConfigPtr& indexConfig);

    static PrimaryKeyFormatterPtr CreatePKFormatterByIndexType(const config::PrimaryKeyIndexConfigPtr& indexConfig);

private:
    IE_LOG_DECLARE();
};

///////////////////////////////////////////////////////////////
template <typename Key>
inline typename PrimaryKeyFormatterCreator<Key>::PrimaryKeyFormatterPtr
PrimaryKeyFormatterCreator<Key>::CreatePKFormatterByLoadMode(const config::PrimaryKeyIndexConfigPtr& indexConfig)
{
    // TODO: only for read, move to pk loader
    switch (indexConfig->GetPKLoadStrategyParam()->GetPrimaryKeyLoadMode()) {
    case config::PrimaryKeyLoadStrategyParam::HASH_TABLE:
        return PrimaryKeyFormatterPtr(new HashPrimaryKeyFormatter<Key>());
    case config::PrimaryKeyLoadStrategyParam::SORTED_VECTOR:
        assert(indexConfig->GetPrimaryKeyIndexType() == pk_sort_array);
        return PrimaryKeyFormatterPtr(new SortedPrimaryKeyFormatter<Key>());
    case config::PrimaryKeyLoadStrategyParam::BLOCK_VECTOR:
        assert(indexConfig->GetPrimaryKeyIndexType() == pk_block_array);
        return PrimaryKeyFormatterPtr(new BlockPrimaryKeyFormatter<Key>(indexConfig->GetPrimaryKeyDataBlockSize()));
    default:
        assert(false);
        return PrimaryKeyFormatterPtr();
    }
}

template <typename Key>
typename PrimaryKeyFormatterCreator<Key>::PrimaryKeyFormatterPtr
PrimaryKeyFormatterCreator<Key>::CreatePKFormatterByIndexType(const config::PrimaryKeyIndexConfigPtr& indexConfig)
{
    PrimaryKeyFormatterPtr pkFormatter;
    assert(indexConfig);
    switch (indexConfig->GetPrimaryKeyIndexType()) {
    case pk_hash_table:
        return PrimaryKeyFormatterPtr(new HashPrimaryKeyFormatter<Key>());
        // pkFormatter.reset(new HashPrimaryKeyFormatter<Key>());
        // break;
    case pk_sort_array:
        return PrimaryKeyFormatterPtr(new SortedPrimaryKeyFormatter<Key>());
        // pkFormatter.reset(new SortedPrimaryKeyFormatter<Key>());
        // break;
    case pk_block_array:
        return PrimaryKeyFormatterPtr(new BlockPrimaryKeyFormatter<Key>(indexConfig->GetPrimaryKeyDataBlockSize()));
        // pkFormatter.reset(new BlockPrimaryKeyFormatter<Key>(indexConfig->GetPrimaryKeyDataBlockSize()));
        // break;
    default:
        assert(false);
        return PrimaryKeyFormatterPtr();
    }
    return PrimaryKeyFormatterPtr();
}
}} // namespace indexlib::index

#endif //__INDEXLIB_PRIMARY_KEY_FORMATTER_CREATOR_H
