#include "aitheta_indexer/plugins/aitheta/parallel_segment.h"

#include <aitheta/index_framework.h>
#include <indexlib/file_system/buffered_file_reader.h>
#include <indexlib/storage/file_system_wrapper.h>

#include "aitheta_indexer/plugins/aitheta/util/flexible_float_index_holder.h"
#include "aitheta_indexer/plugins/aitheta/util/indexlib_io_wrapper.h"
#include "aitheta_indexer/plugins/aitheta/util/input_storage.h"
#include "aitheta_indexer/plugins/aitheta/util/output_storage.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);
using namespace std;
using namespace aitheta;

IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index);
IE_LOG_SETUP(aitheta_plugin, ParallelSegment);

bool ParallelSegment::PrepareReduceTaskInput(const DirectoryPtr &dir, ReduceTaskInput &input) {
    IE_LOG(INFO, "return nothing, dummy function");
    return true;
}

bool ParallelSegment::Load(const DirectoryPtr &dir) {
    mDirectory = dir;

    if (!mReduceMeta.Load(dir)) {
        IE_LOG(ERROR, "failed to load parallel reduce segment meta in path[%s]", dir->GetPath().c_str());
        return false;
    }

    shared_ptr<vector<docid_t>> sharedDocIdArray(new vector<docid_t>());
    shared_ptr<vector<int64_t>> sharedPkeyArray(new vector<int64_t>());
    shared_ptr<unordered_map<int64_t, docid_t>> sharedPkey2DocIdIdx(new unordered_map<int64_t, docid_t>());

    bool loadDocIdRemapSucess = false;
    if (dir->IsExist(DOCID_REMAP_FILE_NAME)) {
        if (!IndexSegment::LoadDocIdRemap(dir, FSOT_MMAP, *sharedDocIdArray, *sharedPkeyArray)) {
            return false;
        }
        loadDocIdRemapSucess = true;
    }

    for (size_t taskId = 0; taskId < mReduceMeta.parallelCount; ++taskId) {
        DirectoryPtr subDir;
        if (!GetSubDir(dir, taskId, mReduceMeta, subDir)) {
            return false;
        }

        SegmentMeta meta;
        if (!meta.Load(subDir)) {
            IE_LOG(ERROR, "failed to get meta in [%s]", subDir->GetPath().c_str());
            return false;
        }
        if (meta.getType() != SegmentType::kIndex) {
            IE_LOG(ERROR, "Index Segment is Expected");
            return false;
        }

        IndexSegmentPtr segment(new IndexSegment(meta));
        segment->SetDocIdArray(sharedDocIdArray);
        segment->SetPkeyArray(sharedPkeyArray);
        segment->SetPkey2DocIdIdx(sharedPkey2DocIdIdx);

        if (loadDocIdRemapSucess) {
            segment->DisableLoadDocIdRemap();
        }

        bool isNotLastSegment = (taskId + 1u < mReduceMeta.parallelCount) ? true : false;
        if (isNotLastSegment) {
            segment->DisableConstructPkeyMap();
        }

        if (!segment->Load(subDir)) {
            IE_LOG(ERROR, "failed to load segment with path[%s]", subDir->GetPath().c_str());
            return false;
        }

        mSubSegVector.push_back(segment);
    }

    IE_LOG(INFO, "successfully load multi-index segment in path[%s]", dir->GetPath().c_str());
    return true;
}

bool ParallelSegment::UpdateDocId(const DocIdMap &docIdMap) {
    if (mSubSegVector.empty()) {
        IE_LOG(ERROR, "mSubSegVector is empty");
        return false;
    }

    if (!mSubSegVector[0]->UpdateDocId(docIdMap)) {
        IE_LOG(ERROR, "failed to update docid in instance");
        return false;
    }

    IE_LOG(INFO, "successfully update docId");
    return true;
}

bool ParallelSegment::Dump(const IE_NAMESPACE(file_system)::DirectoryPtr &directory) {
    if (mDumpSegId >= mSubSegVector.size()) {
        IE_LOG(ERROR, "error instance Id[%u], total segment num[%lu]", mDumpSegId, mSubSegVector.size());
        return false;
    }

    // dump docid remap file in ONLY one sub segment to save space
    if (mDumpSegId != 0u) {
        mSubSegVector[mDumpSegId]->DisableDumpDocIdRemap();
    }

    if (!mSubSegVector[mDumpSegId]->Dump(directory)) {
        IE_LOG(ERROR, "failed to dump segment, instance Id[%u]", mDumpSegId);
        return false;
    }
    IE_LOG(INFO, "successfully dump segment with id[%u] with path[%s]", mDumpSegId, directory->GetPath().c_str());
    return true;
}

bool ParallelSegment::Merge(const vector<RawSegmentPtr> &segVector, const vector<docid_t> &segBaseDocIds,
                            const docid_t baseDocId) {
    if (mSubSegVector.empty()) {
        IE_LOG(ERROR, "mSubSegVector is empty");
        return false;
    }

    if (!mSubSegVector[0]->MergeRawSegments(baseDocId, segVector, segBaseDocIds)) {
        return false;
    }

    IE_LOG(INFO, "successfully merge segment");
    return true;
}

IndexSegmentPtr ParallelSegment::GetSegment(const uint32_t segId) {
    if (segId >= mReduceMeta.parallelCount) {
        IE_LOG(ERROR, "error instance Id[%u], total segment num[%u]", segId, mReduceMeta.parallelCount);
        return IndexSegmentPtr();
    }

    return mSubSegVector[segId];
}

bool ParallelSegment::GetSubDir(const IE_NAMESPACE(file_system)::DirectoryPtr &dir, uint32_t taskId,
                                IE_NAMESPACE(index)::ParallelReduceMeta &reduceMeta,
                                IE_NAMESPACE(file_system)::DirectoryPtr &subDir) {
    if (taskId >= reduceMeta.parallelCount) {
        IE_LOG(ERROR, "error sub dir id[%u], total segment num[%u]", taskId, reduceMeta.parallelCount);
        return false;
    }

    subDir = dir->GetDirectory(reduceMeta.GetInsDirName(taskId), false);
    if (nullptr == subDir) {
        IE_LOG(ERROR, "failed to get sub-directory, task id[%u] in  directory[%s]", taskId, dir->GetPath().c_str());
        return false;
    }
    return true;
}

bool ParallelSegment::GetSubDir(const IE_NAMESPACE(file_system)::DirectoryPtr &dir, uint32_t taskId,
                                IE_NAMESPACE(file_system)::DirectoryPtr &subDir) {
    ParallelReduceMeta reduceMeta;
    if (!reduceMeta.Load(dir)) {
        IE_LOG(ERROR, "failed to load parallel reduce meta file in[%s]", dir->GetPath().c_str());
        return false;
    }
    return GetSubDir(dir, taskId, reduceMeta, subDir);
}

bool ParallelSegment::EstimateLoad4ReduceMemoryUse(const IE_NAMESPACE(file_system)::DirectoryPtr &dir,
                                                   size_t &totalSize) const {
    ParallelReduceMeta reduceMeta;
    if (!reduceMeta.Load(dir)) {
        IE_LOG(ERROR, "failed to load parallel reduce meta file");
        return false;
    }

    totalSize = dir->GetFileLength(DOCID_REMAP_FILE_NAME);

    SegmentMeta meta;
    if (!meta.Load(dir)) {
        return false;
    }
    totalSize += meta.getTotalDocNum() * (sizeof(docid_t) + sizeof(int64_t));

    for (size_t taskId = 0; taskId < reduceMeta.parallelCount; ++taskId) {
        DirectoryPtr subDir;
        if (!GetSubDir(dir, taskId, reduceMeta, subDir)) {
            return false;
        }
        totalSize += subDir->GetFileLength(INDEX_INFO_FILE_NAME);
        totalSize += subDir->GetFileLength(JSONIZABLE_INDEX_INFO_FILE_NAME);
    }

    return true;
}

bool ParallelSegment::EstimateLoad4RetrieveMemoryUse(const IE_NAMESPACE(file_system)::DirectoryPtr &dir,
                                                     size_t &size) const {
    size = mMeta.getFileSize() * IDX_SEG_LOAD_4_RETRIEVE_MEM_USE_MAGIC_NUM;
    size += mMeta.getTotalDocNum() * (sizeof(docid_t) + sizeof(int64_t)) * PKEY_2_DOCID_MAP_MAGIC_NUM;
    return true;
}

bool ParallelSegment::MergeDocIdRemap(const IE_NAMESPACE(file_system)::DirectoryPtr &directory,
                                      IE_NAMESPACE(index)::ParallelReduceMeta &reduceMeta, size_t &docIdRemapFileSize) {
    IE_LOG(INFO, "start to merge docid remap file");
    vector<docid_t> mergedDocIdArray;
    vector<int64_t> mergedPkeyArray;

    bool mergeSuccess = false;

    for (size_t taskId = 0u; taskId < reduceMeta.parallelCount; ++taskId) {
        DirectoryPtr subDir;
        if (!ParallelSegment::GetSubDir(directory, taskId, reduceMeta, subDir)) {
            return false;
        }

        SegmentMeta segmentMeta;
        if (!segmentMeta.Load(subDir)) {
            return false;
        }

        if (segmentMeta.isOptimizeMergeIndex()) {
            if (!IndexSegment::LoadDocIdRemap(subDir, FSOT_BUFFERED, mergedDocIdArray, mergedPkeyArray)) {
                IE_LOG(INFO, "failed to find docid remap file, try next one");
                continue;
            }
            IE_LOG(INFO, "No need to reconstruct docid remap file, just copy it from [%s]", subDir->GetPath().c_str());
            mergeSuccess = true;
            break;
        }
    }

    if (!mergeSuccess) {
        for (size_t taskId = 0u; taskId < reduceMeta.parallelCount; ++taskId) {
            DirectoryPtr subDir;
            if (!ParallelSegment::GetSubDir(directory, taskId, reduceMeta, subDir)) {
                return false;
            }

            vector<docid_t> docIdArray;
            vector<int64_t> pkeyArray;
            if (!IndexSegment::LoadDocIdRemap(subDir, FSOT_BUFFERED, docIdArray, pkeyArray)) {
                return false;
            }
            for (size_t idx = 0u; idx < docIdArray.size(); ++idx) {
                docid_t docId = docIdArray[idx];
                int64_t pkey = pkeyArray[idx];

                if (unlikely(INVALID_DOCID == docId)) {
                    IE_LOG(WARN, "invalid docid is not expected");
                    continue;
                }

                if ((size_t)docId >= mergedDocIdArray.size()) {
                    mergedDocIdArray.resize(docId + 1, INVALID_DOCID);
                    mergedPkeyArray.resize(docId + 1, INVALID_CATEGORY_ID);
                }
                mergedDocIdArray[docId] = docId;
                mergedPkeyArray[docId] = pkey;
            }
        }
    }

    if (!IndexSegment::DumpDocIdRemap(directory, mergedDocIdArray, mergedPkeyArray, docIdRemapFileSize)) {
        return false;
    }
    IE_LOG(INFO, "finish merging docid remap file, total doc number[%lu]", mergedDocIdArray.size());
    return true;
}

bool ParallelSegment::MergeSegMeta(const DirectoryPtr &directory, ParallelReduceMeta &reduceMeta,
                                   SegmentMeta &mergedSegMeta) {
    if (mergedSegMeta.getType() != SegmentType::kMultiIndex) {
        IE_LOG(ERROR, "SegmentMeta should be kMultiIndex");
        return false;
    }

    for (size_t taskId = 0u; taskId < reduceMeta.parallelCount; ++taskId) {
        DirectoryPtr subDir;
        if (!ParallelSegment::GetSubDir(directory, taskId, reduceMeta, subDir)) {
            return false;
        }
        SegmentMeta segMeta;
        if (!segMeta.Load(subDir)) {
            IE_LOG(ERROR, "failed to load segment meta, sub directory no[%lu], directory[%s]", taskId,
                   subDir->GetPath().c_str());
            return false;
        }
        mergedSegMeta.Merge(segMeta);
        if (0u == taskId) {
            mergedSegMeta.isOptimizeMergeIndex(segMeta.isOptimizeMergeIndex());
        }
    }

    if (!mergedSegMeta.Dump(directory)) {
        return false;
    }
    IE_LOG(INFO, "finish merging segment meta");
    return true;
}

IE_NAMESPACE_END(aitheta_plugin);
