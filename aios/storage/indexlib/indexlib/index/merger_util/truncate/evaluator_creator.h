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
#include "indexlib/config/diversity_constrain.h"
#include "indexlib/config/truncate_profile.h"
#include "indexlib/index/merger_util/truncate/doc_info_allocator.h"
#include "indexlib/index/merger_util/truncate/evaluator.h"
#include "indexlib/index/merger_util/truncate/truncate_attribute_reader_creator.h"
#include "indexlib/index/merger_util/truncate/truncate_common.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, TruncateIndexProperty);

namespace indexlib::index::legacy {

class EvaluatorCreator
{
public:
    EvaluatorCreator();
    ~EvaluatorCreator();

public:
    static EvaluatorPtr Create(const config::TruncateProfile& truncProfile,
                               const config::DiversityConstrain& distConfig,
                               TruncateAttributeReaderCreator* truncateAttrCreator,
                               const DocInfoAllocatorPtr& allocator);

    static EvaluatorPtr CreateEvaluator(const std::string& fieldName,
                                        TruncateAttributeReaderCreator* truncateAttrCreator,
                                        const DocInfoAllocatorPtr& allocator,
                                        const config::TruncateProfile& truncProfile);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(EvaluatorCreator);
} // namespace indexlib::index::legacy
