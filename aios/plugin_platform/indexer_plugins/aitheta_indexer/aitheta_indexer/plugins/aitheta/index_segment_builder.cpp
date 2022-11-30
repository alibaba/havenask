#include "aitheta_indexer/plugins/aitheta/index_segment_builder.h"
#include <autil/TimeUtility.h>
#include <aitheta/index_framework.h>
#include <autil/StringUtil.h>
#include <fslib/fs/FileSystem.h>
#include <indexlib/file_system/buffered_file_reader.h>
#include <indexlib/storage/file_system_wrapper.h>
#include "aitheta_indexer/plugins/aitheta/util/flexible_float_index_holder.h"
#include "aitheta_indexer/plugins/aitheta/util/indexlib_io_wrapper.h"
#include "aitheta_indexer/plugins/aitheta/util/input_storage.h"
#include "aitheta_indexer/plugins/aitheta/util/output_storage.h"
#include "aitheta_indexer/plugins/aitheta/util/knn_params_selector/knn_params_selector_factory.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);
using namespace std;
using namespace aitheta;
using namespace fslib;
using namespace fslib::fs;
using namespace autil;

IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(common);
IE_LOG_SETUP(aitheta_plugin, IndexSegmentBuilder);

bool IndexSegmentBuilder::BuildAndDump(const IE_NAMESPACE(file_system)::DirectoryPtr &directory,
                                       const CustomReduceTaskPtr &reduceTask,
                                       const std::vector<RawSegmentPtr> &incRawSegVector) {
    /* Check Parameters */
    if (mRawSegVector.empty()) {
        IE_LOG(WARN, "raw segment count is Zero");
    }

    /* Prepare heap */
    auto compare = [](const RawSegmentPtr lhs, const RawSegmentPtr rhs) {
        return lhs->GetCatId2Records().begin()->first > rhs->GetCatId2Records().begin()->first;
    };
    priority_queue<RawSegmentPtr, vector<RawSegmentPtr>, decltype(compare)> heap(compare);
    for (auto &segment : mRawSegVector) {
        heap.push(segment);
    }

    bool enableParallelMerge = (reduceTask != nullptr ? true : false);

    // build index
    size_t categoryNum = 0;
    size_t validDocNum = 0;
    size_t totalDocNum = 0;
    IndexSegment indexSegment;
    bool buildSuccess = true;
    auto indexWriter = IndexlibIoWrapper::CreateWriter(directory, INDEX_FILE_NAME);
    if (nullptr == indexWriter) {
        return false;
    }

    std::unordered_map<int64_t, std::pair<docid_t, docid_t>> incRawSegPkMap;
    IndexSegment::BuildPkMap4RawSegments(incRawSegVector, {}, incRawSegPkMap);

    // hold all data in one category
    shared_ptr<FlexibleFloatIndexHolder> data(new FlexibleFloatIndexHolder(mComParam.mDim));
    while (!heap.empty()) {
        auto segment = heap.top();
        heap.pop();

        auto &catId2Records = segment->GetCatId2Records();
        if (catId2Records.empty()) {
            continue;
        }
        // category should not use reference
        int64_t category = catId2Records.begin()->first;

        bool enableBuildIndex = true;
        if (enableParallelMerge) {
            const auto &categorySet = reduceTask->categorySet;
            if (categorySet.find(category) == categorySet.end()) {
                enableBuildIndex = false;
            }
        }

        auto embeddingReader = segment->GetEmbeddingReader();
        const CategoryRecords &categoryRecords = catId2Records.begin()->second;
        const auto &pkeys = get<0>(categoryRecords);
        const auto &docIds = get<1>(categoryRecords);
        assert(pkeys.size() == docIds.size());

        for (size_t i = 0; i < docIds.size(); ++i) {
            EmbeddingPtr embedding;
            SHARED_PTR_MALLOC(embedding, mComParam.mDim, float);
            embeddingReader->Read(embedding.get(), sizeof(float) * mComParam.mDim);
            if (!enableBuildIndex) {
                continue;
            }
            int64_t pkey = pkeys[i];
            docid_t docId = docIds[i];
            if (INVALID_DOCID == docId) {
                auto iterator = incRawSegPkMap.find(pkey);
                if (iterator != incRawSegPkMap.end()) {
                    auto globalDocId = iterator->second;
                    docId = globalDocId.first + globalDocId.second;
                }
            }
            if (INVALID_DOCID != docId) {
                size_t pos = validDocNum;
                if (enableParallelMerge) {
                    data->emplace_back(docId, embedding);
                } else {
                    data->emplace_back(pos, embedding);
                }
                indexSegment.AddDocId(pos, docId);
                indexSegment.AddPkey(pos, pkey);
                ++validDocNum;
            }
            ++totalDocNum;
        }
        catId2Records.erase(catId2Records.begin());
        if (!catId2Records.empty()) {
            heap.push(segment);
        }

        bool buildNow = false;
        if (heap.empty()) {
            buildNow = true;
        } else {
            auto nextSegment = heap.top();
            int64_t nextCategory = (nextSegment->GetCatId2Records()).begin()->first;
            if (category != nextCategory) {
                buildNow = true;
                IE_LOG(INFO, "try to build index for category[%ld]", category);
            }
        }

        if (buildNow) {
            if (0 == data->count()) {
                IE_LOG(INFO, "no embeddings OR no need to build with category[%ld]", category);
            } else {
                MipsParams mipsParams;
                if (!Build(data, indexWriter, mipsParams)) {
                    IE_LOG(ERROR, "failed to build index in category[%ld]", category);
                    buildSuccess = false;
                    break;
                }
                indexSegment.addCatIndexInfo(IndexInfo(category, data->count()));
                indexSegment.addJsonizableIndexInfo(JsonizableIndexInfo(category, data->count(), mipsParams));
                data->clear();
                ++categoryNum;
            }
        }
    }
    indexSegment.setIndexBuiltSize(indexWriter->GetLength());
    indexWriter->Close();

    if (!buildSuccess) {
        return false;
    }

    indexSegment.setValidDocNum(validDocNum);
    indexSegment.setTotalDocNum(totalDocNum);
    if (!indexSegment.Dump(directory)) {
        IE_LOG(ERROR, "index segment dump failed, dir[%s]", directory->GetPath().c_str());
        return false;
    }
    IE_LOG(INFO, "build indexes, total category num[%lu], index len[%lu]", categoryNum, indexWriter->GetLength());

    // 将knn config 写到索引中
    if (mKnnConfig.IsAvailable()) {
        mKnnConfig.Dump(directory);
    }
    return true;
}

const std::string IndexSegmentBuilder::CreateLocalDumpPath() {
    const static string kPrefix = "./local_dump_path_";
    pthread_t pid = pthread_self();
    int64_t timestamp = TimeUtility::currentTimeInNanoSeconds();
    string path = kPrefix + StringUtil::toString(pid) + "_" + StringUtil::toString(timestamp);
    FileSystemWrapper::MkDirIfNotExist(path);
    return path;
}

bool IndexSegmentBuilder::Build(FlexibleFloatIndexHolderPtr &data, IE_NAMESPACE(file_system)::FileWriterPtr &writer,
                                MipsParams &mipsParams) {
    auto kvParam = mKvParam;
    kvParam[DOCUMENT_NUM] = StringUtil::toString(data->count());
    string localPath = CreateLocalDumpPath();
    kvParam[BUILD_TEMP_DUMP_DIR] = localPath;

    IndexMeta meta;
    KnnParamsSelectorFactory paramsSelectorFactory;
    IKnnParamsSelectorPtr paramsSelectorPtr =
        paramsSelectorFactory.CreateSelector(mComParam.mIndexType, mComParam, mKnnConfig, data->count());
    if (!paramsSelectorPtr) {
        IE_LOG(ERROR, "get [%s]'s params selector failed", mComParam.mIndexType.c_str());
        return false;
    }

    if (!paramsSelectorPtr->InitMeta(kvParam, meta)) {
        IE_LOG(ERROR, "init knn meta failed");
        return false;
    }

    IndexParams params;
    if (!paramsSelectorPtr->InitKnnBuilderParams(kvParam, params)) {
        IE_LOG(ERROR, "init knn builder params failed");
        return false;
    }

    MipsParams orinialParams;
    if (paramsSelectorPtr->EnableMipsParams(kvParam)) {
        if (!paramsSelectorPtr->InitMipsParams(kvParam, orinialParams)) {
            IE_LOG(ERROR, "init MipsParams failed");
            return false;
        }
        if (!data->mipsReform(orinialParams, mipsParams)) {
            IE_LOG(ERROR, "failed to execute mipsReform, with param:[enable:[%d], mval:[%u], uval:[%f], norm:[%f]]",
                   mipsParams.enable, mipsParams.mval, mipsParams.uval, mipsParams.norm);
            return false;
        }
    }

    string name = paramsSelectorPtr->GetBuilderName();
    auto builder = IndexFactory::CreateBuilder(name);
    if (nullptr == builder) {
        IE_LOG(ERROR, "create builder[%s] failed", name.data());
        return false;
    }
    IE_LOG(INFO, "create builder[%s] success", name.data());

    int err = builder->init(meta, params);
    if (err < 0) {
        IE_LOG(ERROR, "init index[%s] failed, err[%d]", name.data(), err);
        return false;
    }

    err = builder->trainIndex(data);
    if (err < 0) {
        IE_LOG(ERROR, "train index[%s] failed, err[%d]", name.data(), err);
        return false;
    }
    IE_LOG(INFO, "train[%s]", name.data());

    err = builder->buildIndex(data);
    if (err < 0) {
        IE_LOG(ERROR, "build index[%s] failed, err[%d]", name.data(), err);
        return false;
    }
    IE_LOG(INFO, "build index[%s] success", name.data());

    IndexStoragePtr storage(new OutputStorage(writer));
    err = builder->dumpIndex(".", storage);
    if (err < 0) {
        IE_LOG(ERROR, "dump index[%s] failed, err[%d]", name.data(), err);
        return false;
    }
    IE_LOG(INFO, "dump index[%s] success in file[%s]", name.data(), writer->GetPath().c_str());

    err = builder->cleanup();
    if (err < 0) {
        IE_LOG(ERROR, "cleanup builder[%s] failed, err[%d]", name.data(), err);
        return false;
    }
    IE_LOG(INFO, "clean up index[%s] success", name.data());

    FileSystemWrapper::DeleteDir(localPath);
    return true;
}

IE_NAMESPACE_END(aitheta_plugin);
