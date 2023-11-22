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

#include "indexlib/common_define.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/index/normal/primarykey/primary_key_formatter.h"
#include "indexlib/index/normal/primarykey/primary_key_formatter_creator.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename Key>
class PrimaryKeySegmentFormatter
{
public:
    typedef std::shared_ptr<PrimaryKeyFormatter<Key>> PrimaryKeyFormatterPtr;
    typedef index::PKPair<Key> PKPair;
    typedef util::HashMap<Key, docid_t> HashMapType;
    typedef std::shared_ptr<HashMapType> HashMapTypePtr;

public:
    PrimaryKeySegmentFormatter() {}
    ~PrimaryKeySegmentFormatter() {}

public:
    void Init(const config::PrimaryKeyIndexConfigPtr& indexConfig, const file_system::FileReaderPtr& fileReader,
              const file_system::DirectoryPtr& directory)
    {
        mPrimaryKeyFormatter = PrimaryKeyFormatterCreator<Key>::CreatePKFormatterByLoadMode(indexConfig);
        mLoadMode = indexConfig->GetPKLoadStrategyParam()->GetPrimaryKeyLoadMode();
        if (mLoadMode == config::PrimaryKeyLoadStrategyParam::HASH_TABLE) {
            mHashPrimaryKeyFormatter = dynamic_cast<HashPrimaryKeyFormatter<Key>*>(mPrimaryKeyFormatter.get());
        }
        if (mLoadMode == config::PrimaryKeyLoadStrategyParam::BLOCK_VECTOR) {
            mBlockPrimaryKeyFormatter = dynamic_cast<BlockPrimaryKeyFormatter<Key>*>(mPrimaryKeyFormatter.get());
            mBlockPrimaryKeyFormatter->Load(fileReader, directory);
        }
    }

    future_lite::coro::Lazy<docid_t> FindAsync(char* data, size_t count, Key key,
                                               future_lite::Executor* executor) const __ALWAYS_INLINE;

private:
    PrimaryKeyFormatterPtr mPrimaryKeyFormatter;
    HashPrimaryKeyFormatter<Key>* mHashPrimaryKeyFormatter;
    BlockPrimaryKeyFormatter<Key>* mBlockPrimaryKeyFormatter;
    config::PrimaryKeyLoadStrategyParam::PrimaryKeyLoadMode mLoadMode;

private:
    IE_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////
template <typename Key>
inline future_lite::coro::Lazy<docid_t>
PrimaryKeySegmentFormatter<Key>::FindAsync(char* data, size_t count, Key key, future_lite::Executor* executor) const
{
    if (mLoadMode == config::PrimaryKeyLoadStrategyParam::HASH_TABLE) {
        co_return mHashPrimaryKeyFormatter->Find(data, key);
    } else if (mLoadMode == config::PrimaryKeyLoadStrategyParam::SORTED_VECTOR) {
        co_return SortedPrimaryKeyFormatter<Key>::Find((PKPair*)data, count, key);
    } else {
        co_return co_await mBlockPrimaryKeyFormatter->FindAsync(key, executor);
    }
}
}} // namespace indexlib::index
