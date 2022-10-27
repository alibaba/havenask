#include "indexlib/index/max_min_segment_metrics_updater.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/index/normal/inverted_index/truncate/comparator.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/offline_attribute_segment_reader_container.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/util/class_typed_factory.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/field_type_traits.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, MaxMinSegmentMetricsUpdater);

const string MaxMinSegmentMetricsUpdater::UPDATER_NAME = "max_min";

MaxMinSegmentMetricsUpdater::MaxMinSegmentMetricsUpdater()
    : mCheckedDocCount(0)
    , mCurDocInfo(nullptr)
    , mMinDocInfo(nullptr)
    , mMaxDocInfo(nullptr)
    , mFieldType(ft_unknown)
    , mReference(nullptr)
{
}

MaxMinSegmentMetricsUpdater::~MaxMinSegmentMetricsUpdater() {}

bool MaxMinSegmentMetricsUpdater::ValidateFieldType(FieldType ft) const
{
    switch (ft)
    {
    case ft_int8:
    case ft_uint8:
    case ft_int16:
    case ft_uint16:
    case ft_int32:
    case ft_uint32:
    case ft_int64:
    case ft_uint64:
    case ft_float:
    case ft_fp8:
    case ft_fp16:
    case ft_double:
        return true;
    default:
        return false;
    }
    return false;
}

bool MaxMinSegmentMetricsUpdater::Init(const config::IndexPartitionSchemaPtr& schema,
          const config::IndexPartitionOptions& options, const util::KeyValueMap& parameters) {
    auto iterator = parameters.find("attribute_name");
    if (iterator == parameters.end())
    {
        IE_LOG(ERROR, "must set attribute_name");
        return false;
    }
    mAttrName = iterator->second;
    auto attributeConfig = schema->GetAttributeSchema()->GetAttributeConfig(mAttrName);
    if (!attributeConfig)
    {
        IE_LOG(ERROR, "can not find attr [%s]", mAttrName.c_str());
        return false;
    }
    mFieldType = attributeConfig->GetFieldType();
    if (!ValidateFieldType(mFieldType))
    {
        IE_LOG(ERROR, "field type [%d] not valid for this metrics updater", mFieldType);
        return false;
    }
    mDocInfoAllocator.DeclareReference(mAttrName, mFieldType);
    mReference = mDocInfoAllocator.GetReference(mAttrName);
    auto comp
        = ClassTypedFactory<Comparator, ComparatorTyped, Reference*, bool>::GetInstance()->Create(
            mFieldType, mReference, false);
    if (unlikely(!comp))
    {
        IE_LOG(ERROR, "create comparator failed");
        return false;
    }
    mComparator.reset(comp);
    auto evaluator
        = ClassTypedFactory<SortValueEvaluator, SortValueEvaluatorTyped>::GetInstance()->Create(
            mFieldType);
    if (unlikely(!evaluator))
    {
        IE_LOG(ERROR, "create evaluator failed");
        return false;
    }
    evaluator->Init(attributeConfig->GetFieldConfig(), mReference);
    mEvaluator.reset(evaluator);
    mCurDocInfo = mDocInfoAllocator.Allocate();
    return true;
}

bool MaxMinSegmentMetricsUpdater::InitForMerge(const config::IndexPartitionSchemaPtr& schema,
    const config::IndexPartitionOptions& options,
    const index_base::SegmentMergeInfos& segMergeInfos,
    const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders,
    const util::KeyValueMap& parameters)
{
    mReaderContainer = attrReaders;    
    return Init(schema, options, parameters);
}

void MaxMinSegmentMetricsUpdater::Update(const document::DocumentPtr& doc)
{
    if (doc->GetDocOperateType() != ADD_DOC)
    {
        return;
    }
    ++mCheckedDocCount;
    if (!mMaxDocInfo)
    {
        mMaxDocInfo = mDocInfoAllocator.Allocate();
        mMinDocInfo = mDocInfoAllocator.Allocate();
        Evaluate(doc, mMaxDocInfo);
        Evaluate(doc, mMinDocInfo);
        return;
    }
    assert(mMaxDocInfo);
    assert(mMinDocInfo);
    Evaluate(doc, mCurDocInfo);

    DoUpdate();
}

void MaxMinSegmentMetricsUpdater::UpdateForMerge(segmentid_t oldSegId, docid_t oldLocalId)
{
    auto iter = mSegReaderMap.find(oldSegId);
    if (iter == mSegReaderMap.end())
    {
        auto segReader = mReaderContainer->GetAttributeSegmentReader(mAttrName, oldSegId);
        if (!segReader)
        {
            INDEXLIB_FATAL_ERROR(Runtime, "create segment reader for attr[%s] segid[%d] failed",
                mAttrName.c_str(), oldSegId);
        }
        iter = mSegReaderMap.insert(iter, {oldSegId, segReader});
    }
    auto segReader = iter->second;
    ++mCheckedDocCount;

#define FILL_REF(fieldType)                                                                       \
    case fieldType:                                                                               \
        FillReference<FieldTypeTraits<fieldType>::AttrItemType>(segReader, oldLocalId);           \
        break;

    switch (mFieldType)
    {
        NUMBER_FIELD_MACRO_HELPER(FILL_REF);
    default:
        assert(false);
        INDEXLIB_FATAL_ERROR(Runtime, "invalid field type [%d]", mFieldType);
    }
#undef FILL_REF

    DoUpdate();
}

template<typename T>
void MaxMinSegmentMetricsUpdater::FillReference(
    const AttributeSegmentReaderPtr& segReader, docid_t localId)
{
    T value = T();
    if (!segReader->Read(localId, reinterpret_cast<uint8_t*>(&value), sizeof(value)))
    {
        INDEXLIB_FATAL_ERROR(Runtime, "read local docid [%d] failed", localId);
    }
    auto typedRefer = static_cast<ReferenceTyped<T>*>(mReference);
    assert(typedRefer);
    typedRefer->Set(value, mCurDocInfo);    
    if (!mMinDocInfo)
    {
        mMaxDocInfo = mDocInfoAllocator.Allocate();
        mMinDocInfo = mDocInfoAllocator.Allocate();
        typedRefer->Set(value, mMaxDocInfo);
        typedRefer->Set(value, mMinDocInfo);
        return;
    }
    typedRefer->Set(value, mCurDocInfo);
}

void MaxMinSegmentMetricsUpdater::DoUpdate()
{
    if (mComparator->LessThan(mCurDocInfo, mMinDocInfo))
    {
        std::swap(mCurDocInfo, mMinDocInfo);
    }
    else if (mComparator->LessThan(mMaxDocInfo, mCurDocInfo))
    {
        std::swap(mCurDocInfo, mMaxDocInfo);
    }
}


json::JsonMap MaxMinSegmentMetricsUpdater::Dump() const
{
    json::JsonMap jsonMap;
    jsonMap[CHECKED_DOC_COUNT] = mCheckedDocCount;
    json::JsonMap valueMap;
    if (mCheckedDocCount == 0)
    {
        return jsonMap;
    }
#define FILL_JSONMAP(fieldType)                                                                   \
    case fieldType:                                                                               \
        FillJsonMap<FieldTypeTraits<fieldType>::AttrItemType>(valueMap);                          \
        break;

    switch (mFieldType)
    {
        NUMBER_FIELD_MACRO_HELPER(FILL_JSONMAP);
    default:
        assert(false);
        INDEXLIB_FATAL_ERROR(Runtime, "invalid field type [%d]", mFieldType);
    }
#undef FILL_JSONMAP
    string attrKey = GetAttrKey(mAttrName);
    jsonMap[attrKey] = valueMap;
    return jsonMap;
}

template <typename T>
void MaxMinSegmentMetricsUpdater::FillJsonMap(json::JsonMap& jsonMap) const
{
    auto typedRefer = static_cast<ReferenceTyped<T>*>(mReference);
    assert(typedRefer);
    T value = T();
    typedRefer->Get(mMaxDocInfo, value);
    jsonMap["max"] = value;
    typedRefer->Get(mMinDocInfo, value);
    jsonMap["min"] = value;
}

void MaxMinSegmentMetricsUpdater::Evaluate(const DocumentPtr& document, DocInfo* docInfo)
{
    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);
    const AttributeDocumentPtr attrDoc = doc->GetAttributeDocument();
    assert(attrDoc);

    mEvaluator->Evaluate(attrDoc, docInfo);
}

#undef ENUM_SINGLE_VALUE_MACRO

IE_NAMESPACE_END(index);
