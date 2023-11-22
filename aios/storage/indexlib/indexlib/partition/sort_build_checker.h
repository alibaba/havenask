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
#include "indexlib/index/merger_util/truncate/doc_info_allocator.h"
#include "indexlib/index/merger_util/truncate/multi_comparator.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, AttributeSchema);
DECLARE_REFERENCE_CLASS(document, Document);
DECLARE_REFERENCE_CLASS(index, DocInfo);
DECLARE_REFERENCE_CLASS(index, SortValueEvaluator);

namespace indexlib { namespace partition {

class SortBuildChecker
{
public:
    SortBuildChecker();
    ~SortBuildChecker();

public:
    bool Init(const config::AttributeSchemaPtr& attrSchema, const index_base::PartitionMeta& partMeta);

    bool CheckDocument(const document::DocumentPtr& doc);

private:
    void Evaluate(const document::DocumentPtr& document, index::legacy::DocInfo* docInfo);

private:
    uint32_t mCheckCount;
    index::legacy::DocInfo* mLastDocInfo;
    index::legacy::DocInfo* mCurDocInfo;
    std::vector<index::SortValueEvaluatorPtr> mEvaluators;
    index::legacy::MultiComparator mComparator;
    index::legacy::DocInfoAllocator mDocInfoAllocator;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SortBuildChecker);
}} // namespace indexlib::partition
