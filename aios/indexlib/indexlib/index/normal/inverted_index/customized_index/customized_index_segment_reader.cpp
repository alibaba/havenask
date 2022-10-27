#include "indexlib/index/normal/inverted_index/customized_index/customized_index_segment_reader.h"
#include "indexlib/index/in_memory_segment_reader.h"

using namespace std;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, CustomizedIndexSegmentReader);

CustomizedIndexSegmentReader::CustomizedIndexSegmentReader() 
{
}

CustomizedIndexSegmentReader::~CustomizedIndexSegmentReader() 
{
}

bool CustomizedIndexSegmentReader::Init(
        const IndexConfigPtr& indexConfig, const IndexSegmentRetrieverPtr& retriever)
{
    if (!indexConfig)
    {
        IE_LOG(ERROR, "indexConfig is NULL");
        return false;
    }
    mRetriever = retriever;
    return true;
}

bool CustomizedIndexSegmentReader::GetSegmentPosting(
        dictkey_t key, docid_t baseDocId,
        SegmentPosting &segPosting, Pool* sessionPool) const
{
    INDEXLIB_FATAL_ERROR(UnSupported, "unsupported interface!");
    return false;
}

IE_NAMESPACE_END(index);

