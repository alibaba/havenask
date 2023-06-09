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

#include "indexlib/index/inverted_index/config/TruncateIndexProperty.h"
#include "indexlib/index/inverted_index/config/TruncateProfile.h"
#include "indexlib/index/inverted_index/truncate/DocInfoAllocator.h"
#include "indexlib/index/inverted_index/truncate/IEvaluator.h"
#include "indexlib/index/inverted_index/truncate/TruncateAttributeReaderCreator.h"

namespace indexlib::index {

class EvaluatorCreator
{
public:
    EvaluatorCreator() = default;
    ~EvaluatorCreator() = default;

public:
    static std::shared_ptr<IEvaluator>
    Create(const indexlibv2::config::TruncateIndexProperty& truncateIndexProperty,
           const std::shared_ptr<TruncateAttributeReaderCreator>& truncateAttributeCreator,
           const std::shared_ptr<DocInfoAllocator>& allocator);

private:
    static std::shared_ptr<IEvaluator>
    CreateEvaluator(const std::string& fieldName,
                    const std::shared_ptr<TruncateAttributeReaderCreator>& truncateAttributeCreator,
                    const std::shared_ptr<DocInfoAllocator>& allocator,
                    const std::shared_ptr<indexlibv2::config::TruncateProfile>& truncProfile);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
