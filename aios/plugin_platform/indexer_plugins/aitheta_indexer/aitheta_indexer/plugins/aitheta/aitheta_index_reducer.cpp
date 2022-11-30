#include "aitheta_indexer/plugins/aitheta/aitheta_index_reducer.h"

#include <time.h>

#include <algorithm>
#include <map>
#include <queue>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include <fslib/fslib.h>
#include <aitheta/index_framework.h>
#include <autil/TimeUtility.h>
#include <indexlib/file_system/buffered_file_writer.h>
#include <indexlib/storage/file_system_wrapper.h>
#include <proxima/common/params_define.h>
#include <indexlib/misc/exception.h>

#include "aitheta_indexer/plugins/aitheta/util/custom_logger.h"
#include "aitheta_indexer/plugins/aitheta/aitheta_index_reduce_item.h"
#include "aitheta_indexer/plugins/aitheta/raw_segment.h"
#include "aitheta_indexer/plugins/aitheta/index_segment.h"
#include "aitheta_indexer/plugins/aitheta/parallel_segment.h"
#include "aitheta_indexer/plugins/aitheta/index_segment_builder.h"
#include "aitheta_indexer/plugins/aitheta/index_segment_merger.h"
#include "aitheta_indexer/plugins/aitheta/raw_segment_merger.h"
#include "aitheta_indexer/plugins/aitheta/parallel_segment_merger.h"
#include "aitheta_indexer/plugins/aitheta/util/indexlib_io_wrapper.h"

using namespace std;
using namespace aitheta;
using namespace fslib;
using namespace fslib::fs;
using namespace autil::legacy;

IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_BEGIN(aitheta_plugin);

IE_LOG_SETUP(aitheta_plugin, AithetaIndexReducer);

AithetaIndexReducer::~AithetaIndexReducer() {}

bool AithetaIndexReducer::DoInit(const IndexerResourcePtr &resource) {
    RegisterGlobalIndexLogger();
    mKnnConfig.Load(resource->pluginPath);
    return true;
}

int64_t AithetaIndexReducer::EstimateMemoryUse(const vector<DirectoryPtr> &indexDirs,
                                               const SegmentMergeInfos &segMergeInfos,
                                               const index_base::OutputSegmentMergeInfos &outputSegMergeInfos,
                                               bool isSortMerge) const {
    size_t totalMemoryUse = 0u;
    bool hasIndexSeg = false;

    size_t rawSegMetaSize = 0u;
    for (const auto &dir : indexDirs) {
        SegmentMeta meta;
        if (meta.Load(dir)) {
            IE_LOG(INFO, "segment meta load in path[%s] success", dir->GetPath().c_str());
        } else {
            IE_LOG(WARN, "failed to load segment meta in path[%s] ", dir->GetPath().c_str());
            continue;
        }

        SegmentPtr segment;
        switch (meta.getType()) {
            case SegmentType::kRaw: {
                RawSegmentPtr rawSegment(new RawSegment(meta));
                rawSegMetaSize += rawSegment->GetMetaSize();
                segment = rawSegment;
            } break;
            case SegmentType::kIndex: {
                segment.reset(new IndexSegment(meta));
                hasIndexSeg = true;
            } break;
            case SegmentType::kMultiIndex: {
                segment.reset(new ParallelSegment(meta));
                hasIndexSeg = true;
            } break;
            default: {
                IE_LOG(ERROR, "unknown segment type:[%d]", (int)meta.getType());
                return 0l;
            }
        }
        size_t oneSegSize = 0u;
        if (!segment->EstimateLoad4ReduceMemoryUse(dir, oneSegSize)) {
            return false;
        }
        totalMemoryUse += oneSegSize;
    }

    if (mMergeHint.GetId() == ParallelMergeItem::INVALID_PARALLEL_MERGE_ITEM_ID) {
        IE_LOG(INFO, "no task resources, disable parallel index build");
    } else if (!hasIndexSeg) {
        totalMemoryUse *= mMergeHint.GetDataRatio();
        totalMemoryUse += rawSegMetaSize;
    }
    IE_LOG(INFO, "Estimate memory use[%lu] in reduce", totalMemoryUse);
    return totalMemoryUse;
}

// build index by ascending order of category id
bool AithetaIndexReducer::Reduce(const vector<IndexReduceItemPtr> &reduceItems,
                                 const index_base::OutputSegmentMergeInfos &outputSegMergeInfos, bool isSortMerge,
                                 const ReduceResource &resource) {
    if (outputSegMergeInfos.empty()) {
        IE_LOG(ERROR, "empty output segment merge info");
        return false;
    }

    IndexSegmentPtr indexSegment;
    ParallelSegmentPtr parallelSegment;
    vector<RawSegmentPtr> rawSegmentVec;
    auto directory = outputSegMergeInfos[0].directory;

    for (size_t i = 0U; i < reduceItems.size(); ++i) {
        auto &item = reduceItems[i];
        auto aReduceItem = DYNAMIC_POINTER_CAST(AithetaIndexReduceItem, item);
        if (nullptr == aReduceItem) {
            IE_LOG(ERROR, "cast to AithetaIndexReduceItem failed");
            return false;
        }
        SegmentPtr segment = aReduceItem->GetSegment();
        switch (segment->GetSegmentType()) {
            case SegmentType::kRaw: {
                RawSegmentPtr rawSegment = DYNAMIC_POINTER_CAST(RawSegment, segment);
                rawSegment->SetDimension(mComParam.mDim);
                rawSegmentVec.push_back(rawSegment);
                break;
            }
            case SegmentType::kIndex: {
                indexSegment = DYNAMIC_POINTER_CAST(IndexSegment, segment);
                break;
            }
            case SegmentType::kMultiIndex: {
                parallelSegment = DYNAMIC_POINTER_CAST(ParallelSegment, segment);
                break;
            }
            default: {
                IE_LOG(ERROR, "unknown segment type:[%d]", (int)segment->GetSegmentType());
                return false;
            }
        }
    }

    /* Retrieve Task */
    CustomReduceTaskPtr reduceTask;
    if (!ReduceTaskCreater::Retrieve(mMergeHint, mTaskResources, reduceTask)) {
        IE_LOG(ERROR, "failed to retrieve reduce task");
        return false;
    }

    /* Build Index or Merge */
    if (parallelSegment) {
        ParallelSegmentMerger merger(parallelSegment, rawSegmentVec, mKnnConfig);
        return merger.Merge(directory, reduceTask);
    }
    if (indexSegment) {
        IndexSegmentMerger merger(indexSegment, rawSegmentVec, mKnnConfig);
        return merger.Merge(directory);
    }
    if (resource.isEntireDataSet) {
        vector<RawSegmentPtr> fullRawSegVector;
        vector<RawSegmentPtr> incRawSegVector;
        for (auto segment : rawSegmentVec) {
            if (segment->HasEmbedding()) {
                fullRawSegVector.push_back(segment);
            } else {
                incRawSegVector.push_back(segment);
            }
        }
        IE_LOG(INFO, "fullRawSegVector size [%lu], incRawSegVector size [%lu]", fullRawSegVector.size(),
               incRawSegVector.size());
        IndexSegmentBuilder builder(fullRawSegVector, mComParam, mKvParam, mKnnConfig);
        return builder.BuildAndDump(directory, reduceTask, incRawSegVector);
    } else {
        RawSegmentMerger merger(rawSegmentVec);
        return merger.Merge(directory);
    }
}

vector<ReduceTask> AithetaIndexReducer::CreateReduceTasks(const vector<DirectoryPtr> &indexDirs,
                                                          const SegmentMergeInfos &segmentInfos, uint32_t instanceCount,
                                                          bool isEntireDataSet, MergeTaskResourceManagerPtr &resMgr) {
    bool hasIndexSeg = false;
    ReduceTaskInput input;

    for (auto dir : indexDirs) {
        SegmentMeta meta;
        if (!meta.Load(dir)) {
            throw ExceptionBase("load meta failure");
        }
        switch (meta.getType()) {
            case SegmentType::kRaw: {
                RawSegment segment(meta);
                segment.PrepareReduceTaskInput(dir, input);
                break;
            }
            case SegmentType::kIndex: {
                IndexSegment segment(meta);
                segment.PrepareReduceTaskInput(dir, input);
                hasIndexSeg = true;
                break;
            }
            case SegmentType::kMultiIndex: {
                ParallelSegment segment(meta);
                segment.PrepareReduceTaskInput(dir, input);
                hasIndexSeg = true;
                break;
            }
            default: {
                throw ExceptionBase("unknown meta type");
            }
        }
    }

    return ReduceTaskCreater::Create(instanceCount, isEntireDataSet, resMgr, hasIndexSeg, input);
}

void AithetaIndexReducer::EndParallelReduce(const OutputSegmentMergeInfos &outputSegMergeInfos, int32_t parallelCount,
                                            const vector<MergeTaskResourceVector> &instResourceVec) {
    IE_LOG(INFO, "start end parallel reduce");

    for (const auto &info : outputSegMergeInfos) {
        ParallelReduceMeta reduceMeta(parallelCount);
        reduceMeta.Store(info.directory);

        if (info.directory->IsExist(SEGMENT_META_FILE_NAME)) {
            SegmentMeta segMeta;
            if (segMeta.Load(info.directory)) {
                IE_LOG(INFO, "parallel reduce may be disabled or inc merge happens");
                continue;
            } else {
                // reentrance logic for parallel merge
                info.directory->RemoveFile(SEGMENT_META_FILE_NAME);
                IE_LOG(WARN, "content of segment meta is incomplete, retry end parallel reduce...");
            }
        }

        size_t docIdRemapFileSize = 0u;
        if (!ParallelSegment::MergeDocIdRemap(info.directory, reduceMeta, docIdRemapFileSize)) {
            throw misc::ExceptionBase("merge docid remap file failure");
        }

        SegmentMeta segMeta(SegmentType::kMultiIndex, 0u, 0u);
        segMeta.setFileSize(docIdRemapFileSize);
        if (!ParallelSegment::MergeSegMeta(info.directory, reduceMeta, segMeta)) {
            throw misc::ExceptionBase("merge segment meta failure");
        }
    }

    IE_LOG(INFO, "finish end parallel reduce");
}

IE_NAMESPACE_END(aitheta_plugin);
