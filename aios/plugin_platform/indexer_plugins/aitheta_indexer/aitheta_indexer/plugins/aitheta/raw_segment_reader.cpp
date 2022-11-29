#include "aitheta_indexer/plugins/aitheta/raw_segment_reader.h"

#include <assert.h>

#include <queue>

#include "aitheta_indexer/plugins/aitheta/util/indexlib_io_wrapper.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);
using namespace std;

IE_NAMESPACE_USE(file_system);

IE_LOG_SETUP(aitheta_plugin, RawSegmentReader);

bool RawSegmentReader::Open(const DirectoryPtr &dir) {
    mSegment.reset(new RawSegment(mSegmentMeta));
    return mSegment->Load(dir);
}

IE_NAMESPACE_END(aitheta_plugin);
