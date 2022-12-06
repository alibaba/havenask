#include "aitheta_indexer/plugins/aitheta/index_segment.h"

#include <aitheta/index_framework.h>
#include <autil/TimeUtility.h>
#include <indexlib/file_system/buffered_file_reader.h>
#include <indexlib/storage/file_system_wrapper.h>

#include "aitheta_indexer/plugins/aitheta/util/flexible_float_index_holder.h"
#include "aitheta_indexer/plugins/aitheta/util/indexlib_io_wrapper.h"
#include "aitheta_indexer/plugins/aitheta/util/input_storage.h"
#include "aitheta_indexer/plugins/aitheta/util/output_storage.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);
using namespace std;
using namespace aitheta;
using namespace autil;

IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(common);
IE_LOG_SETUP(aitheta_plugin, IndexSegment);

IndexSegment::IndexSegment(const SegmentMeta &meta)
    : Segment(meta), mEnableConstructPkeyMap(true), mEnableDumpDocIdRemap(true), mEnabelLoadDocIdRemap(true) {
    mDocIdArray.reset(new std::vector<docid_t>());
    mPkeyArray.reset(new std::vector<int64_t>());
    mPkey2DocIdIdx.reset(new std::unordered_map<int64_t, docid_t>());
}

void IndexSegment::BuildPkMap4IndexSegment() {
    int64_t startTime = TimeUtility::currentTimeInMicroSeconds();
    mPkey2DocIdIdx->reserve(mPkeyArray->size() * PKEY_2_DOCID_MAP_MAGIC_NUM);
    for (size_t i = 0u; i < mPkeyArray->size(); ++i) {
        (*mPkey2DocIdIdx)[(*mPkeyArray)[i]] = (docid_t)i;
    }
    IE_LOG(INFO, "cost [%ld] ms to construct pkey map", (TimeUtility::currentTimeInMicroSeconds() - startTime) / 1000);
}

bool IndexSegment::PrepareReduceTaskInput(const DirectoryPtr &dir, ReduceTaskInput &input) {
    IE_LOG(INFO, "return nothing, dummy function");
    return true;
}

bool IndexSegment::Load(const DirectoryPtr &dir) {
    IE_LOG(INFO, "start loading index in path[%s]", dir->GetPath().c_str());
    mLoadDir = dir;

    if (mEnabelLoadDocIdRemap) {
        if (!LoadDocIdRemapInOldVersion()) {
            return false;
        }
        // if (!LoadDocIdRemap(dir, FSOT_MMAP, *mDocIdArray, *mPkeyArray)) {
        //     return false;
        // }
    }

    if (!LoadIndexInfo(dir, FSOT_MMAP, mIndexInfos, mJIndexInfos)) {
        return false;
    }

    if (mEnableConstructPkeyMap) {
        BuildPkMap4IndexSegment();
    }

    IE_LOG(INFO, "finish loading index in path[%s]", dir->GetPath().c_str());
    return true;
}

bool IndexSegment::UpdateDocId(const DocIdMap &docIdMap) {
    for (size_t i = 0; i < mDocIdArray->size(); ++i) {
        if (INVALID_DOCID != (*mDocIdArray)[i]) {
            (*mDocIdArray)[i] = docIdMap.GetNewDocId((*mDocIdArray)[i]);
        }
    }
    return true;
}

bool IndexSegment::Dump(const IE_NAMESPACE(file_system)::DirectoryPtr &directory) {
    IE_LOG(INFO, "start dumping index in path[%s]", directory->GetPath().c_str());
    size_t totalIndexSize = 0u;

    bool isOptimizeMergeIndex = (mLoadDir != nullptr ? true : false);
    if (isOptimizeMergeIndex) {
        size_t fileSize = 0u;
        if (!CopyIndex(mLoadDir, directory, fileSize)) {
            return false;
        }
        totalIndexSize += fileSize;
    } else {
        totalIndexSize += mIndexBuiltSize;
    }

    if (mEnableDumpDocIdRemap) {
        size_t fileSize = 0u;
        if (!DumpDocIdRemap(directory, *mDocIdArray, *mPkeyArray, fileSize)) {
            return false;
        }
        totalIndexSize += fileSize;
    }

    {
        size_t fileSize = 0u;
        if (!DumpIndexInfo(directory, mIndexInfos, mJIndexInfos, fileSize)) {
            return false;
        }
        totalIndexSize += fileSize;
    }

    mMeta.isOptimizeMergeIndex(isOptimizeMergeIndex);
    if (!DumpSegmentMeta(directory, totalIndexSize, mIndexInfos, mMeta)) {
        return false;
    }

    IE_LOG(INFO, "finsih dumping index in path[%s]", directory->GetPath().c_str());
    return true;
}

bool IndexSegment::MergeRawSegments(docid_t baseDocId, const std::vector<RawSegmentPtr> &rawSegmentVec,
                                    const std::vector<docid_t> &segmentBaseDocIds) {
    int64_t startTime = autil::TimeUtility::currentTimeInMicroSeconds();

    if (mPkey2DocIdIdx->empty()) {
        BuildPkMap4IndexSegment();
    }

    if (mPkey2DocIdIdx->empty()) {
        IE_LOG(INFO, "index segment is empty, no need to merge inc raw segment");
        return true;
    }

    if (!segmentBaseDocIds.empty() && segmentBaseDocIds.size() != rawSegmentVec.size()) {
        IE_LOG(ERROR, "baseBaseDocIds not empty and size [%lu] != rawSegmentVec size [%lu]", segmentBaseDocIds.size(),
               rawSegmentVec.size());
        return false;
    }

    std::unordered_map<int64_t, std::pair<docid_t, docid_t>> rawSegmentPkeyMap;
    BuildPkMap4RawSegments(rawSegmentVec, segmentBaseDocIds, rawSegmentPkeyMap);

    for (const auto &keyValue : rawSegmentPkeyMap) {
        int64_t pkey = keyValue.first;
        auto globalDocId = keyValue.second;
        auto it = mPkey2DocIdIdx->find(pkey);
        if (it != mPkey2DocIdIdx->end()) {
            docid_t oldDocId = (*mDocIdArray)[it->second];
            docid_t newDocId = globalDocId.second;
            docid_t rawSegBaseDocId = globalDocId.first;
            if (oldDocId == INVALID_DOCID || rawSegBaseDocId > baseDocId) {
                if (newDocId == INVALID_DOCID) {
                    (*mDocIdArray)[it->second] = INVALID_DOCID;
                } else {
                    (*mDocIdArray)[it->second] = newDocId + rawSegBaseDocId - baseDocId;
                }
            }
        }
    }

    IE_LOG(INFO, "finish merge raw segment, use time [%ld] ms",
           (autil::TimeUtility::currentTimeInMicroSeconds() - startTime) / 1000);
    return true;
}

bool IndexSegment::GetIndexBaseAddress(int8_t *&address) {
    if (nullptr == mLoadDir) {
        IE_LOG(ERROR, "mLoadDir is not initialized");
        return false;
    }
    if (nullptr == mIndexProvider) {
        mIndexProvider = mLoadDir->CreateFileReader(INDEX_FILE_NAME, file_system::FSOT_MMAP);
        if (nullptr == mIndexProvider) {
            IE_LOG(ERROR, "failed to create reader[%s] in dir[%s]", INDEX_FILE_NAME.c_str(),
                   mLoadDir->GetPath().c_str());
            return false;
        }
        if (0u == mIndexProvider->GetLength()) {
            IE_LOG(WARN, "index in file[%s] is empty in dir[%s]", INDEX_FILE_NAME.c_str(), mLoadDir->GetPath().c_str());
            return true;
        }
        mIndexProvider->Lock(0, mIndexProvider->GetLength());
    }

    address = static_cast<int8_t *>(mIndexProvider->GetBaseAddress());
    if (!address) {
        IE_LOG(ERROR, "failed to mmap file [%s]", mIndexProvider->GetPath().c_str());
        return false;
    }
    return true;
}

bool IndexSegment::EstimateLoad4ReduceMemoryUse(const IE_NAMESPACE(file_system)::DirectoryPtr &dir,
                                                size_t &size) const {
    size_t pkey2docIdIdxMapSize = mMeta.getTotalDocNum() * (sizeof(int64_t) + sizeof(docid_t));
    size = mMeta.getFileSize() - dir->GetFileLength(INDEX_FILE_NAME) + pkey2docIdIdxMapSize;
    return true;
}

bool IndexSegment::EstimateLoad4RetrieveMemoryUse(const IE_NAMESPACE(file_system)::DirectoryPtr &dir,
                                                  size_t &size) const {
    size = mMeta.getFileSize() * IDX_SEG_LOAD_4_RETRIEVE_MEM_USE_MAGIC_NUM;
    size += mMeta.getTotalDocNum() * (sizeof(int64_t) + sizeof(docid_t)) * PKEY_2_DOCID_MAP_MAGIC_NUM;
    return true;
}

bool IndexSegment::LoadDocIdRemap(const IE_NAMESPACE(file_system)::DirectoryPtr &directory, FSOpenType type,
                                  std::vector<docid_t> &docIdArray, std::vector<int64_t> &pkeyArray) {
    auto reader = IndexlibIoWrapper::CreateReader(directory, DOCID_REMAP_FILE_NAME, type);
    if (nullptr == reader) {
        IE_LOG(ERROR, "failed to create reader for file[%s] in path[%s]", DOCID_REMAP_FILE_NAME.c_str(),
               directory->GetPath().c_str());
        return false;
    }
    size_t docIdNum = reader->GetLength() / (sizeof(docid_t) + sizeof(int64_t));
    if (0u != docIdNum) {
        docIdArray.resize(docIdNum);
        pkeyArray.resize(docIdNum);
        {
            int64_t startTime = TimeUtility::currentTimeInMicroSeconds();
            reader->Read(docIdArray.data(), sizeof(docid_t) * docIdArray.size());
            reader->Read(pkeyArray.data(), sizeof(int64_t) * pkeyArray.size());
            IE_LOG(INFO, "cost [%ld] ms to read file[%s]",
                   (TimeUtility::currentTimeInMicroSeconds() - startTime) / 1000, DOCID_REMAP_FILE_NAME.c_str());
        }
    }
    return true;
}

bool IndexSegment::DumpDocIdRemap(const IE_NAMESPACE(file_system)::DirectoryPtr &directory,
                                  const std::vector<docid_t> &docIdArray, const std::vector<int64_t> &pkeyArray,
                                  size_t &fileSize) {
    if (directory->IsExist(DOCID_REMAP_FILE_NAME)) {
        IE_LOG(WARN, "file[%s] exists in directory[%s], remove it", DOCID_REMAP_FILE_NAME.c_str(),
               directory->GetPath().c_str());
        directory->RemoveFile(DOCID_REMAP_FILE_NAME);
    }

    auto writer = IndexlibIoWrapper::CreateWriter(directory, DOCID_REMAP_FILE_NAME);
    if (nullptr == writer) {
        IE_LOG(ERROR, "failed to create writer for file[%s] in path[%s]", DOCID_REMAP_FILE_NAME.c_str(),
               directory->GetPath().c_str());
        return false;
    }
    writer->Write(docIdArray.data(), sizeof(docid_t) * docIdArray.size());
    writer->Write(pkeyArray.data(), sizeof(int64_t) * pkeyArray.size());
    fileSize = writer->GetLength();
    writer->Close();
    return true;
}

bool IndexSegment::LoadIndexInfo(const IE_NAMESPACE(file_system)::DirectoryPtr &directory,
                                 IE_NAMESPACE(file_system)::FSOpenType type, std::vector<IndexInfo> &indexInfos,
                                 std::vector<JsonizableIndexInfo> &jIndexInfos) {
    {
        // load index info
        auto reader = IndexlibIoWrapper::CreateReader(directory, INDEX_INFO_FILE_NAME, type);
        if (nullptr == reader) {
            return false;
        }
        size_t infoNum = 0U;
        reader->Read(&infoNum, sizeof(infoNum));
        if (0u == infoNum) {
            IE_LOG(WARN, "file[%s] is empty in dir[%s]", INDEX_INFO_FILE_NAME.c_str(), directory->GetPath().c_str());
            return true;
        }

        indexInfos.resize(infoNum);
        reader->Read(indexInfos.data(), sizeof(IndexInfo) * infoNum);
    }
    {
        // load jsonizabel index info
        auto reader = IndexlibIoWrapper::CreateReader(directory, JSONIZABLE_INDEX_INFO_FILE_NAME, type);
        if (nullptr == reader) {
            // for compatibility
            IE_LOG(INFO, "file[%s] does not exist in directory[%s]", JSONIZABLE_INDEX_INFO_FILE_NAME.c_str(),
                   directory->GetPath().c_str());
            return true;
        }
        string content(static_cast<const char *>(reader->GetBaseAddress()), reader->GetLength());
        try {
            autil::legacy::FromJsonString(jIndexInfos, content);
        } catch (const exception &e) {
            IE_LOG(WARN, "failed to deserialize content[%s]  with error[%s]", content.c_str(), e.what());
        }
    }
    return true;
}

bool IndexSegment::DumpIndexInfo(const IE_NAMESPACE(file_system)::DirectoryPtr &directory,
                                 const std::vector<IndexInfo> &indexInfos,
                                 const std::vector<JsonizableIndexInfo> &jIndexInfos, size_t &fileSize) {
    {
        // dump index meta
        auto writer = IndexlibIoWrapper::CreateWriter(directory, INDEX_INFO_FILE_NAME);
        if (writer == nullptr) {
            return false;
        }

        size_t size = indexInfos.size();
        writer->Write(&size, sizeof(size));
        writer->Write(indexInfos.data(), size * sizeof(IndexInfo));
        fileSize = writer->GetLength();
        IE_LOG(INFO, "dump file[%s] sucess", writer->GetPath().c_str());
        writer->Close();
    }
    {
        // dump jsonizable index meta
        auto writer = IndexlibIoWrapper::CreateWriter(directory, JSONIZABLE_INDEX_INFO_FILE_NAME);
        if (writer == nullptr) {
            return false;
        }

        try {
            string content = autil::legacy::ToJsonString(jIndexInfos);
            writer->Write(content.data(), content.size());
        } catch (const exception &e) {
            IE_LOG(ERROR, "failed to jsonize mJsonIndexInfos with err[%s]", e.what());
            writer->Close();
            return false;
        }
        fileSize += writer->GetLength();
        IE_LOG(INFO, "dump file[%s] sucess", writer->GetPath().c_str());
        writer->Close();
    }
    return true;
}

bool IndexSegment::DumpSegmentMeta(const IE_NAMESPACE(file_system)::DirectoryPtr &directory, size_t totalIndexSize,
                                   const std::vector<IndexInfo> &indexInfos, SegmentMeta &meta) {
    meta.setFileSize(totalIndexSize);
    meta.setCategoryNum(indexInfos.size());
    for (const auto &info : indexInfos) {
        if (info.mDocNum > meta.getMaxCategoryDocNum()) {
            meta.setMaxCategoryDocNum(info.mDocNum);
        }
    }
    meta.Dump(directory);
    return true;
}

bool IndexSegment::CopyIndex(const IE_NAMESPACE(file_system)::DirectoryPtr &loadDir,
                             const IE_NAMESPACE(file_system)::DirectoryPtr &dumpDir, size_t &fileSize) {
    auto reader = IndexlibIoWrapper::CreateReader(loadDir, INDEX_FILE_NAME);
    if (nullptr == reader) {
        return false;
    }
    fileSize = reader->GetLength();
    auto writer = IndexlibIoWrapper::CreateWriter(dumpDir, INDEX_FILE_NAME);
    if (nullptr == writer) {
        return false;
    }

    size_t bufSize = 16 * 1024 * 1024u;  // 16MB;
    vector<int8_t> buf(bufSize);
    for (size_t offset = 0u; offset < fileSize; offset += bufSize) {
        size_t readLen = std::min(fileSize - offset, bufSize);
        reader->Read(static_cast<void *>(buf.data()), readLen, offset);
        writer->Write(buf.data(), readLen);
    }
    writer->Close();
    return true;
}

bool IndexSegment::LoadDocIdRemapInOldVersion() {
    if (nullptr == mLoadDir) {
        IE_LOG(ERROR, "load directory is not initialized");
        return false;
    }
    auto reader = IndexlibIoWrapper::CreateReader(mLoadDir, DOCID_REMAP_FILE_NAME, FSOT_IN_MEM);
    if (nullptr == reader) {
        IE_LOG(ERROR, "failed to create reader for file[%s]", DOCID_REMAP_FILE_NAME.c_str());
        return false;
    }
    size_t docIdNum = reader->GetLength() / (sizeof(docid_t) + sizeof(int64_t));
    if (0 != docIdNum) {
        std::vector<docid_t> docIdArray;
        std::vector<int64_t> pkeyArray;
        docIdArray.resize(docIdNum);
        pkeyArray.resize(docIdNum);
        {
            int64_t startTime = TimeUtility::currentTimeInMicroSeconds();
            reader->Read(docIdArray.data(), sizeof(docid_t) * docIdArray.size());
            reader->Read(pkeyArray.data(), sizeof(int64_t) * pkeyArray.size());
            IE_LOG(INFO, "cost [%ld] ms to read file[%s]",
                   (TimeUtility::currentTimeInMicroSeconds() - startTime) / 1000, DOCID_REMAP_FILE_NAME.c_str());
        }
        if (mDocIdArray->empty()) {
            mDocIdArray->swap(docIdArray);
            mPkeyArray->swap(pkeyArray);
        } else {
            for (size_t idx = 0; idx < docIdArray.size(); ++idx) {
                if (docIdArray[idx] != INVALID_DOCID) {
                    if (mDocIdArray->size() <= idx) {
                        mDocIdArray->resize(idx + 1, INVALID_DOCID);
                        mPkeyArray->resize(idx + 1, 0);
                    }
                    (*mDocIdArray)[idx] = docIdArray[idx];
                    (*mPkeyArray)[idx] = pkeyArray[idx];
                }
            }
        }
    }
    reader->Close();
    return true;
}

void IndexSegment::BuildPkMap4RawSegments(const std::vector<RawSegmentPtr> &rawSegments,
                                          const std::vector<docid_t> &baseIds,
                                          std::unordered_map<int64_t, std::pair<docid_t, docid_t>> &pkeyMap) {
    size_t docNum = 0u;
    for (size_t i = 0; i < rawSegments.size(); ++i) {
        for (const auto &records : rawSegments[i]->GetCatId2Records()) {
            const auto &pks = get<0>(records.second);
            const auto &ids = get<1>(records.second);
            assert(pks.size() == ids.size());
            docNum += pks.size();
        }
    }
    pkeyMap.reserve(docNum * PKEY_2_DOCID_MAP_MAGIC_NUM);

    for (size_t i = 0; i < rawSegments.size(); ++i) {
        docid_t baseId = baseIds.empty() ? 0 : baseIds[i];
        for (const auto &records : rawSegments[i]->GetCatId2Records()) {
            const auto &pks = get<0>(records.second);
            const auto &ids = get<1>(records.second);
            for (size_t j = 0u; j < ids.size(); ++j) {
                pkeyMap[pks[j]] = make_pair(baseId, ids[j]);
            }
        }
    }
    IE_LOG(INFO, "build pk map for raw segments, map size[%lu]", docNum);
}

IE_NAMESPACE_END(aitheta_plugin);
