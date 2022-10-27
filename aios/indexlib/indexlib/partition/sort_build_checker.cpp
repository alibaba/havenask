#include "indexlib/partition/sort_build_checker.h"
#include "indexlib/util/class_typed_factory.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/index/sort_value_evaluator.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, SortBuildChecker);

SortBuildChecker::SortBuildChecker() 
    : mCheckCount(0)
    , mLastDocInfo(NULL)
    , mCurDocInfo(NULL)
{
}

SortBuildChecker::~SortBuildChecker() 
{
}

bool SortBuildChecker::Init(
        const AttributeSchemaPtr& attrSchema,
        const PartitionMeta& partMeta)
{
    if (partMeta.Size() == 0)
    {
        return false;
    }

    for (size_t i = 0; i < partMeta.Size(); i++)
    {
        const SortDescription &desc = partMeta.GetSortDescription(i);
        const string &fieldName = desc.sortFieldName;

        AttributeConfigPtr attributeConfig = 
            attrSchema->GetAttributeConfig(fieldName);
        if (!attributeConfig || attributeConfig->IsMultiValue() || 
            attributeConfig->GetFieldType() == ft_string)
        {
            return false;
        }

        FieldType fieldType = attributeConfig->GetFieldType();
        mDocInfoAllocator.DeclareReference(fieldName, fieldType);
        Reference* refer = mDocInfoAllocator.GetReference(fieldName);
        Comparator *comp =
            ClassTypedFactory<Comparator, ComparatorTyped, Reference*, bool>
            ::GetInstance()->Create(fieldType, refer, desc.sortPattern == sp_desc);
        if (!comp)
        {
            return false;
        }
        mComparator.AddComparator(ComparatorPtr(comp));
        
        SortValueEvaluatorPtr evaluator(
                ClassTypedFactory<SortValueEvaluator, SortValueEvaluatorTyped>
                ::GetInstance()->Create(fieldType));
        evaluator->Init(attributeConfig->GetFieldConfig(), refer);
        mEvaluators.push_back(evaluator);
    }
    mCurDocInfo = mDocInfoAllocator.Allocate();
    return true;
}

bool SortBuildChecker::CheckDocument(const DocumentPtr& doc)
{
    ++mCheckCount;
    if (doc->GetDocOperateType() != ADD_DOC)
    {
        return true;
    }

    if (!mLastDocInfo)
    {
        // first doc to check
        mLastDocInfo = mDocInfoAllocator.Allocate();
        Evaluate(doc, mLastDocInfo);
        return true;
    }

    assert(mLastDocInfo);
    Evaluate(doc, mCurDocInfo);
    
    if (mComparator.LessThan(mCurDocInfo, mLastDocInfo))
    {
        IE_LOG(ERROR,
               "[%d]th doc not in sequence with last built doc for sort build",
               mCheckCount);
        swap(mCurDocInfo, mLastDocInfo);
        return false;
    }
    swap(mCurDocInfo, mLastDocInfo);
    return true;
}

void SortBuildChecker::Evaluate(const DocumentPtr& document,
                                DocInfo* docInfo)
{
    NormalDocumentPtr doc =
        DYNAMIC_POINTER_CAST(NormalDocument, document);
    const AttributeDocumentPtr attrDoc =
        doc->GetAttributeDocument();
    assert(attrDoc);
    for (size_t i = 0; i < mEvaluators.size(); i++)
    {
        mEvaluators[i]->Evaluate(attrDoc, docInfo);
    }
}

IE_NAMESPACE_END(partition);
