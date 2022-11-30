#include "aitheta_indexer/plugins/aitheta/index_segment_merger.h"

#include <aitheta/index_framework.h>
#include <fslib/fs/FileSystem.h>
#include <indexlib/file_system/buffered_file_reader.h>
#include <indexlib/storage/file_system_wrapper.h>

#include "aitheta_indexer/plugins/aitheta/util/flexible_float_index_holder.h"
#include "aitheta_indexer/plugins/aitheta/util/indexlib_io_wrapper.h"
#include "aitheta_indexer/plugins/aitheta/util/input_storage.h"
#include "aitheta_indexer/plugins/aitheta/util/output_storage.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);
using namespace std;
using namespace aitheta;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(common);
IE_LOG_SETUP(aitheta_plugin, IndexSegmentMerger);

bool IndexSegmentMerger::Merge(const IE_NAMESPACE(file_system)::DirectoryPtr &directory, docid_t baseDocId) {
    vector<docid_t> segmentBaseDocIds;
    if (!mIndexSegment->MergeRawSegments(baseDocId, mRawSegmentVec, segmentBaseDocIds)) {
        return false;
    }
    if (!mIndexSegment->Dump(directory)) {
        return false;
    }
    if (mKnnConfig.IsAvailable()) {
        return mKnnConfig.Dump(directory);
    }
    return true;
}

IE_NAMESPACE_END(aitheta_plugin);