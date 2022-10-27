#include "indexlib/merger/split_strategy/time_series_split_strategy.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"
#include "indexlib/config/field_type_traits.h"

using namespace std;

IE_NAMESPACE_BEGIN(merger);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(config);
IE_LOG_SETUP(merger, TimeSeriesSplitStrategy);
IE_LOG_SETUP_TEMPLATE(merger, TimeSeriesSplitProcessorImpl);

const std::string TimeSeriesSplitStrategy::STRATEGY_NAME = "time_series";

TimeSeriesSplitStrategy::TimeSeriesSplitStrategy(index::SegmentDirectoryBasePtr segDir,
        config::IndexPartitionSchemaPtr schema, 
        index::OfflineAttributeSegmentReaderContainerPtr attrReaders,
        const index_base::SegmentMergeInfos& segMergeInfos, const MergePlan& plan)
    : SplitSegmentStrategy(segDir, schema, attrReaders, segMergeInfos, plan)
    , mTimeSeriesSplitProcessor(NULL)
{
}


TimeSeriesSplitStrategy::~TimeSeriesSplitStrategy() 
{
    if (mTimeSeriesSplitProcessor)
    {
        delete mTimeSeriesSplitProcessor;
        mTimeSeriesSplitProcessor = NULL;
    }
}

bool TimeSeriesSplitStrategy::Init(const util::KeyValueMap& parameters)
{
    mAttributeName = GetValueFromKeyValueMap(parameters, "attribute", "");
    string rangesStr = GetValueFromKeyValueMap(parameters, "ranges", "");

    // not success ? 
    auto attributeConfig = mSchema->GetAttributeSchema()->GetAttributeConfig(mAttributeName);

    if (!attributeConfig)
    {
        INDEXLIB_FATAL_ERROR(                                                           
                BadParameter, "Invalid field [%s] for time series split strategy", 
                mAttributeName.c_str()); 
        return false;
    }
    mFieldType = attributeConfig->GetFieldType();

#define CREATE_PROCESSOR(fieldType)                                                               \
    case fieldType:                                                                               \
        mTimeSeriesSplitProcessor                                                                 \
            = new TimeSeriesSplitProcessorImpl<FieldTypeTraits<fieldType>::AttrItemType>();       \
        mTimeSeriesSplitProcessor->Init(mAttributeName, rangesStr, mSegMergeInfos);               \
    break;

    switch (mFieldType)
    {
        NUMBER_FIELD_MACRO_HELPER(CREATE_PROCESSOR);
    default:
        INDEXLIB_FATAL_ERROR(Runtime, "invalid field type [%d]", mFieldType);
        return false;
    }
#undef CREATE_PROCESSOR

    return true;
}

segmentindex_t TimeSeriesSplitStrategy::DoProcess(segmentid_t oldSegId, docid_t oldLocalId)
{
    if (mReaderContainer && mTimeSeriesSplitProcessor)
    {
        auto attributeSegmentReader
            = mReaderContainer->GetAttributeSegmentReader(mAttributeName, oldSegId);
        return mTimeSeriesSplitProcessor->GetDocSegment(attributeSegmentReader, oldLocalId);
    }
    else
    {
        INDEXLIB_FATAL_ERROR(Runtime, "get segment [%d] attribute reader failed", oldSegId);
        return 0;
    }
    return 0;
}

IE_NAMESPACE_END(merger);

