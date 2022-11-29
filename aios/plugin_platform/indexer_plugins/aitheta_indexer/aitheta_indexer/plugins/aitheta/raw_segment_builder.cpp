#include "aitheta_indexer/plugins/aitheta/raw_segment_builder.h"

#include <assert.h>

#include <queue>

#include "aitheta_indexer/plugins/aitheta/util/indexlib_io_wrapper.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);
using namespace std;

IE_NAMESPACE_USE(file_system);

IE_LOG_SETUP(aitheta_plugin, RawSegmentBuilder);

bool RawSegmentBuilder::Build(int64_t catId, int64_t pkey, docid_t docId, const EmbeddingPtr &embedding) {
    return mRawSegment->Add(catId, pkey, docId, embedding);
}

bool RawSegmentBuilder::Dump(const DirectoryPtr &dir) { return mRawSegment->Dump(dir); }

IE_NAMESPACE_END(aitheta_plugin);
