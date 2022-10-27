#include "indexlib/table/segment_meta.h"

using namespace std;

IE_NAMESPACE_BEGIN(table);
IE_LOG_SETUP(table, SegmentMeta);

SegmentMeta::SegmentMeta()
    : type(SegmentSourceType::UNKNOWN)
{
}

SegmentMeta::~SegmentMeta() 
{
}

IE_NAMESPACE_END(table);

