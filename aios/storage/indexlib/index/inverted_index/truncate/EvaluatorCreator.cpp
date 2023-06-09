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
#include "indexlib/index/inverted_index/truncate/EvaluatorCreator.h"

#include "indexlib/index/inverted_index/truncate/AttributeEvaluator.h"
#include "indexlib/index/inverted_index/truncate/DocPayloadEvaluator.h"
#include "indexlib/index/inverted_index/truncate/MultiAttributeEvaluator.h"
#include "indexlib/index/inverted_index/truncate/TruncateAttributeReader.h"
#include "indexlib/util/ClassTypedFactory.h"

namespace indexlib::index {

AUTIL_LOG_SETUP(index, EvaluatorCreator);

std::shared_ptr<IEvaluator>
EvaluatorCreator::Create(const indexlibv2::config::TruncateIndexProperty& truncateIndexProperty,
                         const std::shared_ptr<TruncateAttributeReaderCreator>& truncateAttrCreator,
                         const std::shared_ptr<DocInfoAllocator>& allocator)
{
    auto multiEvaluator = std::make_shared<MultiAttributeEvaluator>();
    auto distConfig = truncateIndexProperty.truncateStrategy->GetDiversityConstrain();
    if (truncateIndexProperty.HasFilter()) {
        auto evaluator = CreateEvaluator(distConfig.GetFilterField(), truncateAttrCreator, allocator,
                                         truncateIndexProperty.truncateProfile);
        multiEvaluator->AddEvaluator(evaluator);
    }
    if (truncateIndexProperty.HasSort()) {
        for (size_t i = 0; i < truncateIndexProperty.GetSortDimenNum(); ++i) {
            auto evaluator = CreateEvaluator(truncateIndexProperty.truncateProfile->sortParams[i].GetSortField(),
                                             truncateAttrCreator, allocator, truncateIndexProperty.truncateProfile);
            multiEvaluator->AddEvaluator(evaluator);
        }
    }
    if (distConfig.NeedDistinct()) {
        auto evaluator = CreateEvaluator(distConfig.GetDistField(), truncateAttrCreator, allocator,
                                         truncateIndexProperty.truncateProfile);
        multiEvaluator->AddEvaluator(evaluator);
    }
    return multiEvaluator;
}

std::shared_ptr<IEvaluator>
EvaluatorCreator::CreateEvaluator(const std::string& fieldName,
                                  const std::shared_ptr<TruncateAttributeReaderCreator>& truncateAttrCreator,
                                  const std::shared_ptr<DocInfoAllocator>& allocator,
                                  const std::shared_ptr<indexlibv2::config::TruncateProfile>& truncProfile)
{
    std::shared_ptr<IEvaluator> evaluator;
    if (fieldName == DOC_PAYLOAD_FIELD_NAME) {
        auto refer = allocator->GetReference(fieldName);
        assert(refer);
        auto docPayloadEvaluator = new DocPayloadEvaluator(refer);
        const std::string factorField = truncProfile->GetDocPayloadFactorField();
        if (!factorField.empty()) {
            docPayloadEvaluator->SetFactorEvaluator(
                CreateEvaluator(factorField, truncateAttrCreator, allocator, truncProfile));
        }

        docPayloadEvaluator->SetFP16(truncProfile->DocPayloadUseFP16());
        evaluator.reset(docPayloadEvaluator);
    } else {
        auto attrReader = truncateAttrCreator->Create(fieldName);
        assert(attrReader);
        auto attrConfig = truncateAttrCreator->GetAttributeConfig(fieldName);
        auto refer = allocator->GetReference(attrConfig->GetAttrName());
        assert(refer);
        FieldType fieldType = attrConfig->GetFieldType();
        auto attrEvaluator =
            indexlib::util::ClassTypedFactory<IEvaluator, AttributeEvaluator, std::shared_ptr<TruncateAttributeReader>,
                                              Reference*>::GetInstance()
                ->Create(fieldType, attrReader, refer);
        evaluator.reset(attrEvaluator);
    }
    return evaluator;
}

} // namespace indexlib::index
