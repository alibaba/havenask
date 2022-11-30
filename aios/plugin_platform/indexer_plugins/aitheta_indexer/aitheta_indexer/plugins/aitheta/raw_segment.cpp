#include "aitheta_indexer/plugins/aitheta/raw_segment.h"

#include <assert.h>

#include <queue>

#include "aitheta_indexer/plugins/aitheta/util/indexlib_io_wrapper.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);
using namespace std;

IE_NAMESPACE_USE(file_system);

IE_LOG_SETUP(aitheta_plugin, RawSegment);

bool RawSegment::Add(int64_t catId, int64_t pkey, docid_t docId, const EmbeddingPtr &embedding) {
    CategoryRecords *catRecords = nullptr;
    auto it = mCatId2Records.find(catId);
    if (it != mCatId2Records.end()) {
        catRecords = &(it->second);
    } else {
        auto newIt = mCatId2Records.emplace(catId, CategoryRecords()).first;
        catRecords = &(newIt->second);
        mBuildMemoryUse += (sizeof(catId) + sizeof(int64_t));
    }
    get<0>(*catRecords).push_back(pkey);
    get<1>(*catRecords).push_back(docId);
    mBuildMemoryUse += (sizeof(docid_t) + sizeof(int64_t));

    if (embedding) {
        get<2>(*catRecords).push_back(embedding);
        mBuildMemoryUse += sizeof(float) * mDimension;
    }
    return true;
}

bool RawSegment::PrepareReduceTaskInput(const DirectoryPtr &dir, ReduceTaskInput &input) {
    auto catIdReader = IndexlibIoWrapper::CreateReader(dir, CATEGORYID_FILE_NAME);
    if (nullptr == catIdReader) {
        IE_LOG(ERROR, "failed to create reader[%s] in path[%s]", CATEGORYID_FILE_NAME.c_str(), dir->GetPath().c_str());
        return false;
    }

    size_t categoryNum = catIdReader->GetLength() / (sizeof(int64_t) + sizeof(size_t));
    for (size_t idx = 0U; idx < categoryNum; ++idx) {
        int64_t catId = -1L;
        size_t docNum = 0u;
        catIdReader->Read(&catId, sizeof(catId));
        catIdReader->Read(&docNum, sizeof(docNum));
        input[catId] += docNum;
    }
    return true;
}

bool RawSegment::Load(const DirectoryPtr &dir) {
    // load category id
    auto catIdReader = IndexlibIoWrapper::CreateReader(dir, CATEGORYID_FILE_NAME);
    if (catIdReader == nullptr) {
        IE_LOG(ERROR, "create reader failed [%s]", CATEGORYID_FILE_NAME.c_str());
        return false;
    }
    size_t categoryNum = catIdReader->GetLength() / (sizeof(int64_t) + sizeof(size_t));
    if (0U == categoryNum) {
        IE_LOG(WARN, "category number is ZERO, categery file len[%lu]", catIdReader->GetLength());
        return true;
    } else {
        IE_LOG(DEBUG, "category number [%lu]", categoryNum);
    }
    // load pkey & doc_ids
    auto pkeyReader = IndexlibIoWrapper::CreateReader(dir, PKEY_FILE_NAME);
    auto docIdReader = IndexlibIoWrapper::CreateReader(dir, DOCID_FILE_NAME);
    if (nullptr == pkeyReader || nullptr == docIdReader) {
        return false;
    }
    int64_t catId = -1L;
    size_t totalDocNum = 0u;
    size_t docNum = 0u;
    for (size_t num = 0U; num < categoryNum; ++num) {
        catIdReader->Read(&catId, sizeof(catId));
        catIdReader->Read(&docNum, sizeof(docNum));
        IE_LOG(DEBUG, "category id [%ld], doc num [%lu]", catId, docNum);
        if (0u == docNum) {
            continue;
        }
        auto &catRecords = mCatId2Records[catId];
        get<0>(catRecords).resize(docNum);
        get<1>(catRecords).resize(docNum);
        pkeyReader->Read(get<0>(catRecords).data(), docNum * sizeof(int64_t));
        docIdReader->Read(get<1>(catRecords).data(), docNum * sizeof(docid_t));
        totalDocNum += docNum;
    }

    catIdReader->Close();
    docIdReader->Close();
    pkeyReader->Close();

    mEmbeddingReader = IndexlibIoWrapper::CreateReader(dir, EMBEDDING_FILE_NAME);
    if (mEmbeddingReader == nullptr) {
        IE_LOG(ERROR, "create reader failed [%s]", EMBEDDING_FILE_NAME.c_str());
        return false;
    }
    IE_LOG(INFO, "raw segment loaded in path[%s], category num[%lu], total doc num[%lu]", dir->GetPath().c_str(),
           categoryNum, totalDocNum);
    return true;
}

bool RawSegment::UpdateDocId(const DocIdMap &docIdMap) {
    size_t validDocNum = 0;
    size_t totalDocNum = 0;
    for (auto &catRecords : mCatId2Records) {
        auto &docIds = get<1>(catRecords.second);
        for (auto &docId : docIds) {
            ++totalDocNum;
            if (INVALID_DOCID == docId) {
                continue;
            }
            docId = docIdMap.GetNewDocId(docId);
            ++validDocNum;
        }
    }
    return true;
}

bool RawSegment::Dump(const DirectoryPtr &dir) {
    auto catIdWriter = IndexlibIoWrapper::CreateWriter(dir, CATEGORYID_FILE_NAME);
    if (nullptr == catIdWriter) {
        return false;
    }
    auto docIdWriter = IndexlibIoWrapper::CreateWriter(dir, DOCID_FILE_NAME);
    if (nullptr == docIdWriter) {
        return false;
    }
    auto pkeyWriter = IndexlibIoWrapper::CreateWriter(dir, PKEY_FILE_NAME);
    if (nullptr == pkeyWriter) {
        return false;
    }
    auto embeddingWriter = IndexlibIoWrapper::CreateWriter(dir, EMBEDDING_FILE_NAME);
    if (nullptr == embeddingWriter) {
        return false;
    }

    size_t totalDocNum = 0u;
    for (const auto &kv : mCatId2Records) {
        int64_t catId = kv.first;
        catIdWriter->Write(&catId, sizeof(catId));
        size_t docNum = get<1>(kv.second).size();
        catIdWriter->Write(&docNum, sizeof(docNum));
        totalDocNum += docNum;
        IE_LOG(DEBUG, "category id [%ld], doc num [%lu]", catId, docNum);
        if (0u == docNum) {
            continue;
        }
        // dump pkey
        auto &pkeys = get<0>(kv.second);
        pkeyWriter->Write(pkeys.data(), sizeof(int64_t) * pkeys.size());
        // dump docId
        auto &docIds = get<1>(kv.second);
        docIdWriter->Write(docIds.data(), sizeof(docid_t) * docIds.size());
        for (const auto &embedding : get<2>(kv.second)) {
            embeddingWriter->Write(embedding.get(), sizeof(float) * mDimension);
        }
        if (docNum > mMeta.getMaxCategoryDocNum()) {
            mMeta.setMaxCategoryDocNum(docNum);
        }
    }

    mMeta.setCategoryNum(mCatId2Records.size());
    mMeta.setValidDocNum(totalDocNum);
    mMeta.setTotalDocNum(totalDocNum);
    mMeta.setFileSize(catIdWriter->GetLength() + pkeyWriter->GetLength() + docIdWriter->GetLength() +
                      embeddingWriter->GetLength());

    catIdWriter->Close();
    docIdWriter->Close();
    pkeyWriter->Close();
    embeddingWriter->Close();

    if (!mMeta.Dump(dir)) {
        return false;
    }
    IE_LOG(INFO, "finish dumping raw segment in path[%s] with doc number[%lu]", dir->GetPath().c_str(), totalDocNum);
    return true;
}

bool RawSegment::EstimateLoad4ReduceMemoryUse(const IE_NAMESPACE(file_system)::DirectoryPtr &dir, size_t &size) const {
    size = mMeta.getFileSize() * RAW_SEG_FULL_REDUCE_MEM_USE_MAGIC_NUM;
    return true;
}

bool RawSegment::EstimateLoad4RetrieveMemoryUse(const IE_NAMESPACE(file_system)::DirectoryPtr &dir,
                                                size_t &size) const {
    size = GetMetaSize();
    return true;
}

size_t RawSegment::GetMetaSize() const {
    size_t size = 0u;
    size += (sizeof(docid_t) + sizeof(int64_t)) * mMeta.getTotalDocNum();
    size += (sizeof(int64_t) + sizeof(int64_t)) * mMeta.getCategoryNum();
    return size;
}

IE_NAMESPACE_END(aitheta_plugin);
