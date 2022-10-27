#include "indexlib/index/normal/inverted_index/truncate/truncate_attribute_reader.h"

using namespace std;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, TruncateAttributeReader);

TruncateAttributeReader::TruncateAttributeReader() 
{
}

TruncateAttributeReader::~TruncateAttributeReader() 
{
}

void TruncateAttributeReader::Init(const ReclaimMapPtr &relaimMap, 
                                   const AttributeConfigPtr& attrConfig)
{ 
    mReclaimMap = relaimMap; 
    mAttrConfig = attrConfig;
}

void TruncateAttributeReader::AddAttributeSegmentReader(segmentid_t segId, 
        const AttributeSegmentReaderPtr& segmentReader)
{
    if (segId >= (segmentid_t)mReaders.size())
    {
        mReaders.resize(segId + 1, AttributeSegmentReaderPtr());
    }
    assert(mReaders[segId] == NULL);
    mReaders[segId] = segmentReader;
}

bool TruncateAttributeReader::Read(docid_t docId, uint8_t* buf, 
                                   uint32_t bufLen)
{
    segmentid_t segId = INVALID_SEGMENTID;
    docid_t localDocId = mReclaimMap->GetOldDocIdAndSegId(docId, segId);
    assert(segId != INVALID_SEGMENTID && segId < (segmentid_t)mReaders.size());
    assert(mReaders[segId] != NULL);
    return mReaders[segId]->Read(localDocId, buf, bufLen);
}

IE_NAMESPACE_END(index);

