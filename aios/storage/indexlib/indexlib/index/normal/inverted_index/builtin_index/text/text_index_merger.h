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
#include "indexlib/index/normal/framework/index_merger_creator.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_merger.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_merger.h"
#include "indexlib/index/normal/inverted_index/builtin_index/pack/on_disk_pack_index_iterator.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index { namespace legacy {

class TextIndexMerger : public NormalIndexMerger
{
public:
    typedef OnDiskPackIndexIterator OnDiskTextIndexIterator;

public:
    TextIndexMerger() {}
    ~TextIndexMerger() {}

    DECLARE_INDEX_MERGER_IDENTIFIER(text);
    DECLARE_INDEX_MERGER_CREATOR(TextIndexMerger, it_text);

public:
    OnDiskIndexIteratorCreatorPtr CreateOnDiskIndexIteratorCreator() override
    {
        return OnDiskIndexIteratorCreatorPtr(
            new OnDiskTextIndexIterator::Creator(GetPostingFormatOption(), mIOConfig, mIndexConfig));
    }

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TextIndexMerger);
}}} // namespace indexlib::index::legacy
