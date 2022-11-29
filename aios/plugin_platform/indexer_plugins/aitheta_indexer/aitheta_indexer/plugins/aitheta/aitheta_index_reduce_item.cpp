
#include "aitheta_indexer/plugins/aitheta/aitheta_index_reduce_item.h"

#include <autil/legacy/exception.h>
#include <autil/legacy/jsonizable.h>
#include <indexlib/misc/exception.h>
#include <indexlib/storage/file_system_wrapper.h>

#include "aitheta_indexer/plugins/aitheta/aitheta_indexer.h"
#include "aitheta_indexer/plugins/aitheta/common_define.h"
#include "aitheta_indexer/plugins/aitheta/util/indexlib_io_wrapper.h"

using namespace std;

IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(aitheta_plugin);
IE_LOG_SETUP(aitheta_plugin, AithetaIndexReduceItem);

bool AithetaIndexReduceItem::UpdateDocId(const DocIdMap &docIdMap) {
    if (unlikely(nullptr == mSegment)) {
        IE_LOG(ERROR, "mSegment not inited");
        return false;
    }
    return mSegment->UpdateDocId(docIdMap);
}

bool AithetaIndexReduceItem::LoadIndex(const DirectoryPtr &indexDir) {
    SegmentMeta meta;
    if (!meta.Load(indexDir)) {
        IE_LOG(WARN, "get meta failed in [%s]", indexDir->GetPath().data());
        return false;
    }
    switch (meta.getType()) {
        case SegmentType::kRaw: {
            mSegment.reset(new RawSegment(meta));
        } break;
        case SegmentType::kIndex: {
            mSegment.reset(new IndexSegment(meta));
        } break;
        case SegmentType::kMultiIndex: {
            mSegment.reset(new ParallelSegment(meta));
        } break;
        default:
            IE_LOG(ERROR, "unexpected segment type [%d]", (int)meta.getType());
            return false;
    }
    return mSegment->Load(indexDir);
}

IE_NAMESPACE_END(aitheta_plugin);
