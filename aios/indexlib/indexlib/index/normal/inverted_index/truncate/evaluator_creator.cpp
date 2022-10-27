#include "indexlib/index/normal/inverted_index/truncate/evaluator_creator.h"
#include "indexlib/index/normal/inverted_index/truncate/attribute_evaluator.h"
#include "indexlib/index/normal/inverted_index/truncate/doc_payload_evaluator.h"
#include "indexlib/index/normal/inverted_index/truncate/multi_attribute_evaluator.h"
#include "indexlib/util/class_typed_factory.h"
#include "indexlib/config/truncate_index_property.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, EvaluatorCreator);

EvaluatorCreator::EvaluatorCreator() 
{
}

EvaluatorCreator::~EvaluatorCreator() 
{
}

EvaluatorPtr EvaluatorCreator::Create(
        const TruncateIndexProperty& truncIndexProperty,
        TruncateAttributeReaderCreator *truncateAttrCreator,
        const DocInfoAllocatorPtr& allocator)
{
    const DiversityConstrain& distConfig = 
        truncIndexProperty.mTruncateStrategy->GetDiversityConstrain();
    MultiAttributeEvaluatorPtr multiEvaluator(new MultiAttributeEvaluator);
    if (truncIndexProperty.HasFilter())
    {
        EvaluatorPtr evaluator = CreateEvaluator(distConfig.GetFilterField(),
                truncateAttrCreator, allocator);
        multiEvaluator->AddEvaluator(evaluator);
    }

    if (truncIndexProperty.HasSort())
    {
        for(size_t i = 0; i < truncIndexProperty.GetSortDimenNum(); ++i)
        {
            EvaluatorPtr evaluator = CreateEvaluator(
                    truncIndexProperty.mTruncateProfile->
                    mSortParams[i].GetSortField(), 
                    truncateAttrCreator, allocator);
            multiEvaluator->AddEvaluator(evaluator);
        }
    }

    if (distConfig.NeedDistinct())
    {
        EvaluatorPtr evaluator = CreateEvaluator(distConfig.GetDistField(), 
                truncateAttrCreator, allocator);
        multiEvaluator->AddEvaluator(evaluator);        
    }

    return multiEvaluator;
}

EvaluatorPtr EvaluatorCreator::CreateEvaluator(
        const string& fieldName,
        TruncateAttributeReaderCreator *truncateAttrCreator,
        const DocInfoAllocatorPtr& allocator)
{
    EvaluatorPtr evaluator;
    if (fieldName == DOC_PAYLOAD_FIELD_NAME)
    {
        DocPayloadEvaluator* docPayloadEvaluator = new DocPayloadEvaluator;
        Reference* refer = allocator->GetReference(fieldName);
        assert(refer);
        docPayloadEvaluator->Init(refer);
        evaluator.reset(docPayloadEvaluator);
    }
    else 
    {
        TruncateAttributeReaderPtr attrReader = 
            truncateAttrCreator->Create(fieldName);
        assert(attrReader);
        const AttributeConfigPtr& attrConfig = 
            attrReader->GetAttributeConfig();
        Reference* refer = allocator->GetReference(attrConfig->GetAttrName());
        assert(refer);
        FieldType fieldType = attrConfig->GetFieldType();
        Evaluator* attrEvaluator = 
            ClassTypedFactory<Evaluator, AttributeEvaluator, 
                              TruncateAttributeReaderPtr, Reference*>
            ::GetInstance()->Create(fieldType, attrReader, refer);
        evaluator.reset(attrEvaluator);
    }
    return evaluator;
}

IE_NAMESPACE_END(index);

