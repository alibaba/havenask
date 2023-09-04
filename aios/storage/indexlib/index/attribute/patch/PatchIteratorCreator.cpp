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
#include "indexlib/index/attribute/patch/PatchIteratorCreator.h"

#include "indexlib/config/ITabletSchema.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/index/attribute/patch/MultiFieldPatchIterator.h"
#include "indexlib/index/attribute/patch/PatchIterator.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, PatchIteratorCreator);

std::unique_ptr<PatchIterator>
PatchIteratorCreator::Create(const std::shared_ptr<config::ITabletSchema>& schema,
                             const std::vector<std::pair<docid_t, std::shared_ptr<framework::Segment>>>& segments)
{
    auto iterator = std::make_unique<MultiFieldPatchIterator>(schema);
    iterator->Init(segments, false);
    return iterator;
}

} // namespace indexlibv2::index
