#include "indexlib/index/normal/inverted_index/builtin_index/range/range_level_segment_reader.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_creator.h"

using namespace std;

IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, RangeLevelSegmentReader);

void RangeLevelSegmentReader::Open(const config::IndexConfigPtr& indexConfig,
                                   const index_base::SegmentData& segmentData) 
{
    assert(indexConfig);
    mSegmentData = segmentData;
    DirectoryPtr parentDirectory = segmentData.GetIndexDirectory(mParentIndexName, false);
    if (!parentDirectory)
    {
        IE_LOG(WARN, "index dir [%s] not exist in segment [%d]", 
               mParentIndexName.c_str(), segmentData.GetSegmentId());
        return;
    }

    DirectoryPtr indexDirectory =
        parentDirectory->GetDirectory(indexConfig->GetIndexName(), false);
    if (!indexDirectory)
    {
        IE_LOG(WARN, "sub index dir [%s] in dir [%s] not exist in segment [%d]",
               indexConfig->GetIndexName().c_str(), mParentIndexName.c_str(),
               segmentData.GetSegmentId());
        return;
    }
    NormalIndexSegmentReader::Open(indexConfig, indexDirectory);
    mIsHashTypeDict = indexConfig->IsHashTypedDictionary();
}

DictionaryReader* RangeLevelSegmentReader::CreateDictionaryReader(
        const config::IndexConfigPtr& indexConfig)
{
    return DictionaryCreator::CreateIntegrateReader(indexConfig);
}

IE_NAMESPACE_END(index);

