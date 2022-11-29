#ifndef __INDEXLIB_AITHETA_RAW_SEGMENT_BUILDER_H
#define __INDEXLIB_AITHETA_RAW_SEGMENT_BUILDER_H

#include <tuple>
#include <unordered_map>

#include <indexlib/common_define.h>
#include <indexlib/misc/log.h>
#include <indexlib/file_system/directory.h>
#include <indexlib/indexlib.h>

#include "aitheta_indexer/plugins/aitheta/common_define.h"
#include "aitheta_indexer/plugins/aitheta/raw_segment.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);

class RawSegmentBuilder {
 public:
    RawSegmentBuilder(CommonParam cp) : mParam(cp) {
        mRawSegment.reset(new RawSegment);
        mRawSegment->SetDimension(mParam.mDim);
    }
    ~RawSegmentBuilder() {}

 public:
    bool Build(int64_t catId, int64_t pkey, docid_t docId, const EmbeddingPtr &embedding);
    size_t EstimateBuildMemoryUse() const { return mRawSegment->EstimateBuildMemoryUse(); }
    bool Dump(const IE_NAMESPACE(file_system)::DirectoryPtr &indexDir);

 private:
    CommonParam mParam;
    RawSegmentPtr mRawSegment;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RawSegmentBuilder);

IE_NAMESPACE_END(aitheta_plugin);

#endif  //__INDEXLIB_AITHETA_RAW_SEGMENT_BUILDER_H
