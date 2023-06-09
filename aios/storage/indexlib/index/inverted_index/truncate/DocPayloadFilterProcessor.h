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

#include "indexlib/index/inverted_index/truncate/DocInfoAllocator.h"
#include "indexlib/index/inverted_index/truncate/IDocFilterProcessor.h"
#include "indexlib/index/inverted_index/truncate/IEvaluator.h"

namespace indexlib::index {

class DocPayloadFilterProcessor : public IDocFilterProcessor
{
public:
    DocPayloadFilterProcessor(const indexlibv2::config::DiversityConstrain& constrain,
                              const std::shared_ptr<IEvaluator>& evaluator,
                              const std::shared_ptr<DocInfoAllocator>& allocator);
    ~DocPayloadFilterProcessor() = default;

public:
    bool BeginFilter(const DictKeyInfo& key, const std::shared_ptr<PostingIterator>& postingIt) override;
    bool IsFiltered(docid_t docId) override;
    void SetTruncateMetaReader(const std::shared_ptr<TruncateMetaReader>& metaReader) override
    {
        _metaReader = metaReader;
    };

private:
    int64_t _minValue;
    int64_t _maxValue;
    uint64_t _mask;
    std::shared_ptr<PostingIterator> _postingIt;
    std::shared_ptr<TruncateMetaReader> _metaReader;
    std::shared_ptr<IEvaluator> _evaluator;
    DocInfo* _docInfo;
    Reference* _sortFieldRef;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
