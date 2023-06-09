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

#include "indexlib/index/inverted_index/InvertedIndexMerger.h"
#include "indexlib/index/inverted_index/OnDiskIndexIteratorCreator.h"
#include "indexlib/index/inverted_index/builtin_index/pack/OnDiskPackIndexIterator.h"

namespace indexlib::index {

class TextIndexMerger : public InvertedIndexMerger
{
public:
    using OnDiskTextIndexIterator = OnDiskPackIndexIterator;

    TextIndexMerger() = default;
    ~TextIndexMerger() = default;

    std::string GetIdentifier() const override { return "index.merger.text"; }

    std::shared_ptr<OnDiskIndexIteratorCreator> CreateOnDiskIndexIteratorCreator() override
    {
        return std::shared_ptr<OnDiskIndexIteratorCreator>(
            new OnDiskTextIndexIterator::Creator(_indexFormatOption.GetPostingFormatOption(), _ioConfig, _indexConfig));
    }

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
