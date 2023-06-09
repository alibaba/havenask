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
#include "indexlib/index/inverted_index/patch/InvertedIndexPatchIteratorCreator.h"

#include "indexlib/config/ITabletSchema.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/index/inverted_index/patch/MultiFieldInvertedIndexPatchIterator.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, InvertedIndexPatchIteratorCreator);

std::shared_ptr<IInvertedIndexPatchIterator>
InvertedIndexPatchIteratorCreator::Create(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
                                          const std::vector<std::shared_ptr<indexlibv2::framework::Segment>>& segments,
                                          const std::shared_ptr<indexlib::file_system::IDirectory>& patchExtraDir)
{
    // TODO @qingran support sub schema
    std::shared_ptr<MultiFieldInvertedIndexPatchIterator> iterator(
        new MultiFieldInvertedIndexPatchIterator(schema, /*isSub*/ false, patchExtraDir));

    auto status = iterator->Init(segments);
    if (!status.IsOK()) {
        AUTIL_LOG(WARN, "create inverted index patch iterator failed");
        return nullptr;
    }
    return iterator;
}

} // namespace indexlib::index
