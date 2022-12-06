#include "aitheta_indexer/plugins/aitheta/parallel_segment_merger.h"

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
IE_LOG_SETUP(aitheta_plugin, ParallelSegmentMerger);

bool ParallelSegmentMerger::Merge(const IE_NAMESPACE(file_system)::DirectoryPtr &directory,
                                  const CustomReduceTaskPtr &reduceTask, docid_t baseDocId) {
    vector<docid_t> segBaseDocIds;
    int32_t taskId = reduceTask->taskId;
    if (!mParallelSegment->Merge(mRawSegVector, segBaseDocIds, baseDocId)) {
        IE_LOG(ERROR, "failed to merge parallel segment and raw segments, instance Id[%d]", taskId);
        return false;
    }
    mParallelSegment->SetDumpSegId(taskId);
    if (!mParallelSegment->Dump(directory)) {
        IE_LOG(ERROR, "failed to dump merged parallel segment, instance Id[%d]", taskId);
        return false;
    }

    if (mKnnConfig.IsAvailable()) {
        return mKnnConfig.Dump(directory);
    }
    return true;
}

IE_NAMESPACE_END(aitheta_plugin);
