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
#ifndef __INDEXLIB_EXPACK_INDEX_MERGER_H
#define __INDEXLIB_EXPACK_INDEX_MERGER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/builtin_index/expack/on_disk_expack_index_iterator.h"
#include "indexlib/index/normal/inverted_index/builtin_index/pack/pack_index_merger.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index { namespace legacy {

class ExpackIndexMerger : public PackIndexMerger
{
public:
    ExpackIndexMerger();
    virtual ~ExpackIndexMerger();

    DECLARE_INDEX_MERGER_IDENTIFIER(expack);
    DECLARE_INDEX_MERGER_CREATOR(ExpackIndexMerger, it_expack);

public:
    OnDiskIndexIteratorCreatorPtr CreateOnDiskIndexIteratorCreator() override;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ExpackIndexMerger);

///////////////////////////////////////////////////////////
ExpackIndexMerger::ExpackIndexMerger() {}

ExpackIndexMerger::~ExpackIndexMerger() {}

OnDiskIndexIteratorCreatorPtr ExpackIndexMerger::CreateOnDiskIndexIteratorCreator()
{
    return OnDiskIndexIteratorCreatorPtr(
        new OnDiskExpackIndexIterator::Creator(GetPostingFormatOption(), mIOConfig, mIndexConfig));
}
}}} // namespace indexlib::index::legacy

#endif //__INDEXLIB_EXPACK_INDEX_MERGER_H
