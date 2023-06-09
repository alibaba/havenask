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
#include "indexlib/index/merger_util/truncate/evaluator_creator.h"

#include "indexlib/config/attribute_config.h"
#include "indexlib/config/truncate_index_property.h"
#include "indexlib/index/merger_util/truncate/attribute_evaluator.h"
#include "indexlib/index/merger_util/truncate/doc_payload_evaluator.h"
#include "indexlib/index/merger_util/truncate/multi_attribute_evaluator.h"
#include "indexlib/index/merger_util/truncate/truncate_attribute_reader.h"
#include "indexlib/util/class_typed_factory.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::util;

namespace indexlib::index::legacy {
IE_LOG_SETUP(index, EvaluatorCreator);

EvaluatorCreator::EvaluatorCreator() {}

EvaluatorCreator::~EvaluatorCreator() {}

EvaluatorPtr EvaluatorCreator::Create(const TruncateProfile& truncProfile, const DiversityConstrain& distConfig,
                                      TruncateAttributeReaderCreator* truncateAttrCreator,
                                      const DocInfoAllocatorPtr& allocator)
{
    MultiAttributeEvaluatorPtr multiEvaluator(new MultiAttributeEvaluator);
    if (distConfig.NeedFilter()) {
        EvaluatorPtr evaluator =
            CreateEvaluator(distConfig.GetFilterField(), truncateAttrCreator, allocator, truncProfile);
        multiEvaluator->AddEvaluator(evaluator);
    }

    if (truncProfile.HasSort()) {
        for (size_t i = 0; i < truncProfile.GetSortDimenNum(); ++i) {
            EvaluatorPtr evaluator = CreateEvaluator(truncProfile.mSortParams[i].GetSortField(), truncateAttrCreator,
                                                     allocator, truncProfile);
            multiEvaluator->AddEvaluator(evaluator);
        }
    }

    if (distConfig.NeedDistinct()) {
        EvaluatorPtr evaluator =
            CreateEvaluator(distConfig.GetDistField(), truncateAttrCreator, allocator, truncProfile);
        multiEvaluator->AddEvaluator(evaluator);
    }

    return multiEvaluator;
}

EvaluatorPtr EvaluatorCreator::CreateEvaluator(const string& fieldName,
                                               TruncateAttributeReaderCreator* truncateAttrCreator,
                                               const DocInfoAllocatorPtr& allocator,
                                               const config::TruncateProfile& truncProfile)
{
    EvaluatorPtr evaluator;
    if (fieldName == DOC_PAYLOAD_FIELD_NAME) {
        DocPayloadEvaluator* docPayloadEvaluator = new DocPayloadEvaluator;
        Reference* refer = allocator->GetReference(fieldName);
        assert(refer);
        docPayloadEvaluator->Init(refer);
        const string factorField = truncProfile.GetDocPayloadFactorField();
        if (!factorField.empty()) {
            docPayloadEvaluator->SetFactorEvaluator(
                CreateEvaluator(factorField, truncateAttrCreator, allocator, truncProfile));
        }
        docPayloadEvaluator->SetFP16(truncProfile.DocPayloadUseFP16());
        evaluator.reset(docPayloadEvaluator);
    } else {
        auto attrReader = truncateAttrCreator->Create(fieldName);
        assert(attrReader);
        const AttributeConfigPtr& attrConfig = attrReader->GetAttributeConfig();
        Reference* refer = allocator->GetReference(attrConfig->GetAttrName());
        assert(refer);
        FieldType fieldType = attrConfig->GetFieldType();
        Evaluator* attrEvaluator =
            ClassTypedFactory<Evaluator, AttributeEvaluator, TruncateAttributeReaderPtr, Reference*>::GetInstance()
                ->Create(fieldType, attrReader, refer);
        evaluator.reset(attrEvaluator);
    }
    return evaluator;
}
} // namespace indexlib::index::legacy
