#include "aitheta_indexer/plugins/aitheta/aitheta_index_segment_retriever.h"

#include <algorithm>

#include <autil/StringTokenizer.h>
#include <autil/StringUtil.h>
#include <autil/mem_pool/Pool.h>
#include <fslib/fslib.h>
#include <indexlib/file_system/buffered_file_reader.h>
#include <indexlib/file_system/directory.h>
#include <indexlib/storage/file_system_wrapper.h>
#include <proxima/common/params_define.h>

#include "indexlib/storage/file_system_wrapper.h"
#include "aitheta_indexer/plugins/aitheta/common_define.h"
#include "aitheta_indexer/plugins/aitheta/raw_segment_reader.h"
#include "aitheta_indexer/plugins/aitheta/index_segment_reader.h"
#include "aitheta_indexer/plugins/aitheta/util/indexlib_io_wrapper.h"
#include "aitheta_indexer/plugins/aitheta/util/input_storage.h"

using namespace std;
using namespace aitheta;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(aitheta_plugin);
IE_LOG_SETUP(aitheta_plugin, AithetaIndexSegmentRetriever);

size_t AithetaIndexSegmentRetriever::EstimateLoadMemoryUse(const IE_NAMESPACE(file_system)::DirectoryPtr &dir) const {
    SegmentMeta meta;
    if (!meta.Load(dir)) {
        IE_LOG(WARN, "get meta failed in [%s]", dir->GetPath().data());
        return 0l;
    }

    SegmentPtr segment;
    switch (meta.getType()) {
        case SegmentType::kRaw: {
            segment.reset(new RawSegment(meta));
        } break;
        case SegmentType::kIndex: {
            segment.reset(new IndexSegment(meta));
        } break;
        case SegmentType::kMultiIndex: {
            segment.reset(new ParallelSegment(meta));
        } break;
        default: {
            IE_LOG(ERROR, "unknown segment type:[%d]", (int)meta.getType());
            return 0l;
        }
    }
    size_t size = 0u;
    if (!segment->EstimateLoad4RetrieveMemoryUse(dir, size)) {
        IE_LOG(ERROR, "failed to estimate online load memory in path[%s]", dir->GetPath().c_str());
        return 0u;
    }
    return size;
}

AithetaIndexSegmentRetriever::AithetaIndexSegmentRetriever(const util::KeyValueMap &parameters)
    : IndexSegmentRetriever(parameters) {}

bool AithetaIndexSegmentRetriever::Open(const file_system::DirectoryPtr &dir) {
    if (nullptr == dir) {
        IE_LOG(ERROR, "dir is nullptr");
        return false;
    }
    if (!mSegmentMeta.Load(dir)) {
        IE_LOG(WARN, "get meta failed in [%s]", dir->GetPath().data());
        return false;
    }

    switch (mSegmentMeta.getType()) {
        case SegmentType::kRaw:
            mSegmentReader.reset(new RawSegmentReader(mSegmentMeta));
            break;
        case SegmentType::kIndex: {
            InitKnnConfig(dir);
            mSegmentReader.reset(new IndexSegmentReader(mSegmentMeta, mParameters, mKnnConfig));
        } break;
        case SegmentType::kMultiIndex: {
            DirectoryPtr subDir;
            if (!ParallelSegment::GetSubDir(dir, 0u, subDir)) {
                return false;
            }
            InitKnnConfig(subDir);
            mSegmentReader.reset(new IndexSegmentReader(mSegmentMeta, mParameters, mKnnConfig));
        } break;
        default:
            IE_LOG(ERROR, "segment type err [%d]", (int)mSegmentMeta.getType());
            return false;
    }

    return mSegmentReader->Open(dir);
}

IE_NAMESPACE_END(aitheta_plugin);
