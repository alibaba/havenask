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
#include "indexlib/partition/sort_build_checker.h"

#include "indexlib/config/attribute_schema.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/index/segment_metrics_updater/sort_value_evaluator.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/util/class_typed_factory.h"

using namespace std;
using namespace autil;

using namespace indexlib::util;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::document;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, SortBuildChecker);

SortBuildChecker::SortBuildChecker() : mCheckCount(0), mLastDocInfo(NULL), mCurDocInfo(NULL) {}

SortBuildChecker::~SortBuildChecker() {}

bool SortBuildChecker::Init(const AttributeSchemaPtr& attrSchema, const PartitionMeta& partMeta)
{
    if (partMeta.Size() == 0) {
        return false;
    }

    for (size_t i = 0; i < partMeta.Size(); i++) {
        const indexlibv2::config::SortDescription& desc = partMeta.GetSortDescription(i);
        const string& fieldName = desc.GetSortFieldName();

        AttributeConfigPtr attributeConfig = attrSchema->GetAttributeConfig(fieldName);
        if (!attributeConfig || attributeConfig->IsMultiValue() || attributeConfig->GetFieldType() == ft_string) {
            return false;
        }

        FieldType fieldType = attributeConfig->GetFieldType();
        mDocInfoAllocator.DeclareReference(fieldName, fieldType,
                                           attributeConfig->GetFieldConfig()->IsEnableNullField());
        index::legacy::Reference* refer = mDocInfoAllocator.GetReference(fieldName);
        index::legacy::Comparator* comp =
            ClassTypedFactory<index::legacy::Comparator, index::legacy::ComparatorTyped, index::legacy::Reference*,
                              bool>::GetInstance()
                ->Create(fieldType, refer, desc.GetSortPattern() == indexlibv2::config::sp_desc);
        if (!comp) {
            return false;
        }
        mComparator.AddComparator(index::legacy::ComparatorPtr(comp));

        SortValueEvaluatorPtr evaluator(
            ClassTypedFactory<SortValueEvaluator, SortValueEvaluatorTyped>::GetInstance()->Create(fieldType));
        evaluator->Init(attributeConfig, refer);
        mEvaluators.push_back(evaluator);
    }
    mCurDocInfo = mDocInfoAllocator.Allocate();
    return true;
}

bool SortBuildChecker::CheckDocument(const DocumentPtr& doc)
{
    ++mCheckCount;
    if (doc->GetDocOperateType() != ADD_DOC) {
        return true;
    }

    if (!mLastDocInfo) {
        // first doc to check
        mLastDocInfo = mDocInfoAllocator.Allocate();
        Evaluate(doc, mLastDocInfo);
        return true;
    }

    assert(mLastDocInfo);
    Evaluate(doc, mCurDocInfo);

    if (mComparator.LessThan(mCurDocInfo, mLastDocInfo)) {
        IE_LOG(ERROR, "[%d]th doc not in sequence with last built doc for sort build", mCheckCount);
        swap(mCurDocInfo, mLastDocInfo);
        return false;
    }
    swap(mCurDocInfo, mLastDocInfo);
    return true;
}

void SortBuildChecker::Evaluate(const DocumentPtr& document, index::legacy::DocInfo* docInfo)
{
    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);
    const AttributeDocumentPtr attrDoc = doc->GetAttributeDocument();
    assert(attrDoc);
    for (size_t i = 0; i < mEvaluators.size(); i++) {
        mEvaluators[i]->Evaluate(attrDoc, docInfo);
    }
}
}} // namespace indexlib::partition
