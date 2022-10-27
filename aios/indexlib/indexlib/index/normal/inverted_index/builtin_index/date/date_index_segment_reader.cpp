#include "indexlib/index/normal/inverted_index/builtin_index/date/date_index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_creator.h"
#include "indexlib/index/normal/inverted_index/builtin_index/date/time_range_info.h"
#include "indexlib/config/date_index_config.h"

using namespace std;
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, DateIndexSegmentReader);

DateIndexSegmentReader::DateIndexSegmentReader()
    : mMinTime(1)
    , mMaxTime(0)
    , mIsHashTypeDict(false)
{
}

DateIndexSegmentReader::~DateIndexSegmentReader() 
{
}

void DateIndexSegmentReader::Open(const config::IndexConfigPtr& indexConfig,
                                  const index_base::SegmentData& segmentData)
{
    mIsHashTypeDict = indexConfig->IsHashTypedDictionary();
    NormalIndexSegmentReader::Open(indexConfig, segmentData);
    file_system::DirectoryPtr indexDirectory = segmentData.GetIndexDirectory(
            indexConfig->GetIndexName(), true);
    TimeRangeInfo rangeInfo;
    rangeInfo.Load(indexDirectory);
    mMinTime = rangeInfo.GetMinTime();
    mMaxTime = rangeInfo.GetMaxTime();
    DateIndexConfigPtr dateIndexConfig = DYNAMIC_POINTER_CAST(
            DateIndexConfig, indexConfig);
    mFormat = dateIndexConfig->GetDateLevelFormat();
}

DictionaryReader* DateIndexSegmentReader::CreateDictionaryReader(
        const IndexConfigPtr& indexConfig)
{
    return DictionaryCreator::CreateIntegrateReader(indexConfig);
}

IE_NAMESPACE_END(index);

