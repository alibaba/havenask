#include "aitheta_indexer/plugins/aitheta/test/aitheta_test_base.h"

#include <time.h>
#include <fstream>

#include <aitheta/index_distance.h>
#include <autil/ConstString.h>
#include <autil/mem_pool/Pool.h>
#include <autil/StringUtil.h>
#include <indexlib/config/customized_index_config.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_module_factory.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_reduce_item.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_reducer.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_retriever.h>
#include <indexlib/index/normal/inverted_index/customized_index/indexer.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_plugin_util.h>
#include <indexlib/index_base/schema_adapter.h>
#include <indexlib/plugin/Module.h>
#include <indexlib/config/module_info.h>
#include <indexlib/test/schema_maker.h>
#include <indexlib/file_system/directory_creator.h>
#include "aitheta_indexer/plugins/aitheta/util/indexlib_io_wrapper.h"
#include "aitheta_indexer/plugins/aitheta/aitheta_index_module_factory.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_BEGIN(aitheta_plugin);
IE_NAMESPACE_USE(util);

IE_LOG_SETUP(aitheta_plugin, AithetaTestBase);

AithetaTestBase::~AithetaTestBase() {
    for (auto ptr : mReclaimMaps) {
        delete ptr;
    }
}

void AithetaTestBase::CaseSetUp() {
    mRootDir = TEST_DATA_PATH;
    mOptions = IndexPartitionOptions();
    mPluginRoot = mRootDir + "aitheta_indexer_plugins";
    mSchema = SchemaMaker::MakeSchema("pk:string", "ipk:primarykey64:pk", "", "");
    mFactory = AithetaIndexModuleFactoryPtr(new AithetaIndexModuleFactory());

    mResMgr.reset(new MergeTaskResourceManager);
    DirectoryPtr resourceDir =
        GET_PARTITION_DIRECTORY()->MakeDirectory("resource_" + StringUtil::toString(randomNum()));
    mResMgr->Init(resourceDir->GetPath());

    IE_LOG(INFO, "CaseSetUp: RootDir %s, PluginRoot %s", mRootDir.c_str(), mPluginRoot.c_str());
}

void AithetaTestBase::CaseTearDown() { IE_LOG(INFO, "CaseTearDown"); }

bool AithetaTestBase::Build(const util::KeyValueMap &params, const std::string &inputPath, std::string &outputPath,
                            size_t &successCnt, const vector<docid_t> &docIds, bool isFullBuildPhase) {
    fstream input(inputPath);
    if (!input.is_open()) {
        IE_LOG(ERROR, "failed to open file with path[%s]", inputPath.c_str());
        return false;
    }

    vector<string> data;
    for (docid_t idx = 0u; !input.eof(); ++idx) {
        string doc;
        std::getline(input, doc);
        data.push_back(doc);
    }
    outputPath = CreateOutputPath(params, {inputPath});
    return Build(params, data, outputPath, successCnt, docIds);
}

bool AithetaTestBase::Build(const util::KeyValueMap &params, const std::vector<std::string> &data,
                            std::string &outputPath, size_t &successCnt, const std::vector<docid_t> &docIds,
                            bool isFullBuildPhase) {
    if (outputPath.empty()) {
        outputPath = CreateOutputPath(params, data);
    }

    if (mFileSystem->IsExist(outputPath)) {
        IE_LOG(INFO, "RawSegment already exists in [%s]. skip building...", outputPath.c_str());
        return true;
    }
    FileSystemWrapper::MkDirIfNotExist(outputPath);
    auto dir = DirectoryCreator::Create(mFileSystem, outputPath);
    if (!dir) {
        IE_LOG(INFO, "failed to create dir [%s]", outputPath.c_str());
        return false;
    }

    IndexerPtr indexer(mFactory->createIndexer(params));

    auto resource = CreateIndexerResource(isFullBuildPhase);
    if (!indexer->DoInit(resource)) {
        return false;
    }

    for (docid_t idx = 0u; idx < (docid_t)data.size(); ++idx) {
        const string &doc = data[idx];
        vector<string> strs(StringUtil::split(doc, ";"));
        if (2u != strs.size() && 3u != strs.size()) {
            IE_LOG(ERROR, "invalid input data. Valid format: cat;itemid;embedding OR itemid;embedding");
            return false;
        }

        vector<const Field *> fields;
        vector<IndexRawField> rawFields(strs.size());
        for (size_t i = 0; i < strs.size(); ++i) {
            rawFields[i].SetData(ConstString(strs[i]));
            fields.push_back(&rawFields[i]);
        }

        docid_t docId = idx;
        if (!docIds.empty()) {
            if ((docid_t)docIds.size() <= idx) {
                IE_LOG(ERROR, "unexpected docIds size");
                return false;
            }
            docId = docIds[idx];
        }
        if (!indexer->Build(fields, docId)) {
            IE_LOG(WARN, "failed to build doc[%s], ignore it", doc.c_str());
            continue;
        }
        ++successCnt;
    }
    IE_LOG(INFO, "finish building one RawSegment in [%s]", outputPath.c_str());
    if (!indexer->Dump(dir)) {
        return false;
    }
    IE_LOG(INFO, "dump one RawSegment in [%s]", outputPath.c_str());
    return true;
}

bool AithetaTestBase::Load(const util::KeyValueMap &params, const std::string &path, docid_t base,
                           const vector<docid_t> &newDocIds, IE_NAMESPACE(index)::IndexReduceItemPtr &segment) {
    IndexReducerPtr reducer(mFactory->createIndexReducer(params));
    segment = reducer->CreateReduceItem();

    DirectoryPtr dir = DirectoryCreator::Create(mFileSystem, path);
    if (!segment->LoadIndex(dir)) {
        return false;
    }
    auto docIdMap = CreateDocIdMap(base, newDocIds);
    if (!segment->UpdateDocId(docIdMap)) {
        return false;
    }
    IE_LOG(INFO, "Load Segment in [%s]", path.c_str());
    return true;
}

bool AithetaTestBase::Reduce(const util::KeyValueMap &params, const vector<std::string> &segmentPaths,
                             bool isEntireDataSet, const vector<docid_t> &baseDocIds, const vector<docid_t> &docIdMap,
                             std::string &outputPath, uint32_t parallelCount) {
    if (baseDocIds.size() != segmentPaths.size()) {
        IE_LOG(ERROR, "baseDocIds size does not match segmentPaths");
        return false;
    }
    outputPath = CreateOutputPath(params, segmentPaths);
    if (mFileSystem->IsExist(outputPath)) {
        IE_LOG(INFO, "Reduce is already done in [%s]. skip Reducing...", outputPath.c_str());
        return true;
    }
    FileSystemWrapper::MkDirIfNotExist(outputPath);

    {
        std::vector<DirectoryPtr> dirVector;
        for (const auto &path : segmentPaths) {
            dirVector.push_back(DirectoryCreator::Create(mFileSystem, path));
            IE_LOG(INFO, "segment path[%s]\n", path.c_str());
        }

        util::KeyValueMap params;
        config::IndexPartitionSchemaPtr schema(new config::IndexPartitionSchema);
        config::IndexPartitionOptions options;
        util::CounterMapPtr counterMap;
        index_base::PartitionMeta partitionMeta;
        IndexerResourcePtr resource(new IndexerResource(schema, options, counterMap, partitionMeta, "", ""));
        MergeItemHint mergeHint;
        mergeHint.SetDataRatio(1.0f / parallelCount);
        mergeHint.SetId(parallelCount > 1 ? 0 : ParallelMergeItem::INVALID_PARALLEL_MERGE_ITEM_ID);
        MergeTaskResourceVector taskResources;
        AithetaIndexReducer reducer(params);
        reducer.Init(resource, mergeHint, taskResources);
        SegmentMergeInfos segMergeInfos;
        index_base::OutputSegmentMergeInfos outputSegMergeInfos;
        size_t expectedEstimateMemoryUse = InnerGetIndexSegmentLoad4ReduceMemoryUse(segmentPaths, parallelCount);
        size_t actualEstimateMemoryUse =
            reducer.EstimateMemoryUse(dirVector, segMergeInfos, outputSegMergeInfos, false);
        if (expectedEstimateMemoryUse != actualEstimateMemoryUse) {
            IE_LOG(ERROR, "expected size[%lu], actual[%lu]", expectedEstimateMemoryUse, actualEstimateMemoryUse);
            return false;
        }
    }

    if (parallelCount <= 1u) {
        vector<IndexReduceItemPtr> reduceItems(segmentPaths.size());
        for (size_t i = 0; i < segmentPaths.size(); ++i) {
            if (!Load(params, segmentPaths[i], baseDocIds[i], docIdMap, reduceItems[i])) {
                IE_LOG(ERROR, "failed to load segment in path[%s]", segmentPaths[i].c_str());
                return false;
            }
        }
        return SequentialReduce(params, reduceItems, isEntireDataSet, outputPath);
    } else {
        return ParallelReduce(params, segmentPaths, isEntireDataSet, baseDocIds, docIdMap, outputPath, parallelCount);
    }
}

bool AithetaTestBase::CreateSearcher(const util::KeyValueMap &params, const vector<string> &segmentPaths,
                                     const vector<docid_t> &baseDocIds, IndexRetrieverPtr &searcher) {
    vector<DirectoryPtr> dirs;
    for (const std::string &path : segmentPaths) {
        if (!mFileSystem->IsExist(path)) {
            IE_LOG(INFO, "index path[%s] does not exit", path.c_str());
            return false;
        }
        auto dir = DirectoryCreator::Create(mFileSystem, path);
        if (!dir) {
            IE_LOG(INFO, "failed to create dir [%s]", path.c_str());
            return false;
        }
        dirs.push_back(dir);
    }

    vector<IndexSegmentRetrieverPtr> retrievers;
    for (auto dir : dirs) {
        IndexSegmentRetrieverPtr retriever(mFactory->createIndexSegmentRetriever(params));
        if (!retriever->Open(dir)) {
            IE_LOG(ERROR, "failed to open dir[%s]", dir->GetPath().c_str());
            return false;
        }
        retrievers.push_back(retriever);
    }
    DeletionMapReaderPtr deletionMapReader(new DeletionMapReader);
    searcher.reset(mFactory->createIndexRetriever(params));
    if (!searcher->Init(deletionMapReader, retrievers, baseDocIds)) {
        return false;
    }
    IE_LOG(INFO, "create searcher success");
    return true;
}

bool AithetaTestBase::CreateSearcher(const util::KeyValueMap &params, const vector<std::string> &dataPaths,
                                     string &outputPath, IE_NAMESPACE(index)::IndexRetrieverPtr &searcher,
                                     uint32_t parallelCount) {
    size_t totalDocIdCnt = 0u;
    vector<string> buildDirs;
    vector<size_t> docCnts;
    for (auto &dataPath : dataPaths) {
        string buildDir;
        size_t successCnt = 0;
        if (!Build(params, dataPath, buildDir, successCnt)) {
            return false;
        }
        buildDirs.push_back(buildDir);
        docCnts.push_back(successCnt);
        totalDocIdCnt += successCnt;
    }

    vector<docid_t> docIdMap(totalDocIdCnt);
    for (docid_t i = 0; i < (docid_t)totalDocIdCnt; ++i) {
        docIdMap[i] = i;
    }

    vector<docid_t> baseDocIdVector;
    for (size_t i = 0; i < buildDirs.size(); ++i) {
        docid_t base = baseDocIdVector.empty() ? 0 : (baseDocIdVector.back() + docCnts[i]);
        baseDocIdVector.push_back(base);
    }

    if (!Reduce(params, buildDirs, true, baseDocIdVector, docIdMap, outputPath, parallelCount)) {
        return false;
    }

    if (!CreateSearcher(params, {outputPath}, {0}, searcher)) {
        return false;
    }
    return true;
}

bool AithetaTestBase::Search(IE_NAMESPACE(index)::IndexRetrieverPtr &searcher, const std::string &query,
                             vector<std::pair<docid_t, float>> &results) {
    autil::mem_pool::Pool pool;
    common::Term term;
    term.SetWord(query);
    vector<SegmentMatchInfo> segMatchInfos = searcher->Search(term, &pool);
    if (segMatchInfos.empty()) {
        IE_LOG(ERROR, "Search result is empty");
        return false;
    }
    const auto &matchInfo = segMatchInfos[0].matchInfo;
    results.clear();
    for (size_t i = 0; i < matchInfo->matchCount; ++i) {
        float score = 0.0f;
        memcpy(&score, matchInfo->matchValues + i, sizeof(score));
        docid_t docId = matchInfo->docIds[i];
        results.emplace_back(docId, score);
    }
    return true;
}

DocIdMap AithetaTestBase::CreateDocIdMap(docid_t base, const vector<docid_t> &globalDocIds) {
    // look at DocIdMap, u see why i write the stupid code
    auto reclaimMap = new ReclaimMapPtr;
    reclaimMap->reset(new ReclaimMap);
    (*reclaimMap)->SetSliceArray(globalDocIds);
    mReclaimMaps.push_back(reclaimMap);
    return DocIdMap(base, *reclaimMap);
}

string AithetaTestBase::CreateOutputPath(const util::KeyValueMap &params, const std::vector<std::string> &inputPaths,
                                         const std::string &suffix) {
    string signature;
    if (!inputPaths.empty()) {
        signature = inputPaths[0];
    }
    for (auto &kv : params) {
        signature.append(kv.first).append(kv.second);
    }

    auto dir = StringUtil::toString(HashAlgorithm::hashString(signature.c_str(), signature.size(), 0));
    if (!suffix.empty()) {
        dir.append("_").append(suffix);
    }

    return FileSystemWrapper::JoinPath(GET_PARTITION_DIRECTORY()->GetPath(), dir);
}

bool AithetaTestBase::SequentialReduce(const util::KeyValueMap &params, const vector<IndexReduceItemPtr> &reduceItems,
                                       bool isEntireDataSet, const std::string &outputPath) {
    auto outputDir = DirectoryCreator::Create(mFileSystem, outputPath);

    IndexPartitionSchemaPtr schema(new IndexPartitionSchema());
    IndexPartitionOptions options = IndexPartitionOptions();
    CounterMapPtr counterMap(new CounterMap());
    IndexerResourcePtr indexResource(
        new IndexerResource(schema, options, counterMap, PartitionMeta(), "index", mPluginRoot));
    indexResource->pluginPath = mPluginRoot;

    ReduceResource reduceResource(SegmentMergeInfos(), ReclaimMapPtr(new ReclaimMap), isEntireDataSet);

    IndexReducerPtr reducer(mFactory->createIndexReducer(params));
    if (!reducer->DoInit(indexResource)) {
        return false;
    }
    OutputSegmentMergeInfo outputSegMergeInfo;
    outputSegMergeInfo.directory = outputDir;
    if (!reducer->Reduce(reduceItems, {outputSegMergeInfo}, false, reduceResource)) {
        return false;
    }
    IE_LOG(INFO, "Reduce Segment to [%s]", outputPath.c_str());
    return true;
}

bool AithetaTestBase::ParallelReduce(const util::KeyValueMap &params, const vector<string> &segmentPaths,
                                     bool isEntireDataSet, const vector<docid_t> &baseDocIds,
                                     const vector<docid_t> &docIdMap, const string &outputPath,
                                     uint32_t parallelCount) {
    auto outputDir = DirectoryCreator::Create(mFileSystem, outputPath);

    IndexPartitionSchemaPtr schema(new IndexPartitionSchema());
    IndexPartitionOptions options = IndexPartitionOptions();
    CounterMapPtr counterMap(new CounterMap());
    IndexerResourcePtr indexResource(
        new IndexerResource(schema, options, counterMap, PartitionMeta(), "index", mPluginRoot));
    indexResource->pluginPath = mPluginRoot;

    ReduceResource reduceResource(SegmentMergeInfos(), ReclaimMapPtr(new ReclaimMap), isEntireDataSet);

    std::vector<DirectoryPtr> dirVector;
    for (const auto &path : segmentPaths) {
        dirVector.push_back(DirectoryCreator::Create(mFileSystem, path));
    }

    MergeTaskResourceManagerPtr resMgr(new MergeTaskResourceManager);
    const auto resourceDir = GET_PARTITION_DIRECTORY()->MakeDirectory("resource");
    resMgr->Init(resourceDir->GetPath());

    vector<ReduceTask> reduceTasks;
    {
        IndexReducerPtr reducer(mFactory->createIndexReducer(params));
        reduceTasks =
            reducer->CreateReduceTasks(dirVector, SegmentMergeInfos(), parallelCount, isEntireDataSet, resMgr);
    }

    ParallelReduceMeta meta(reduceTasks.size());
    vector<MergeTaskResourceVector> instRscVector;
    for (size_t i = 0; i < reduceTasks.size(); ++i) {
        MergeItemHint hint(i, reduceTasks[i].dataRatio, 1, reduceTasks.size());
        MergeTaskResourceVector taskResources;
        for (resourceid_t id : reduceTasks[i].resourceIds) {
            MergeTaskResourcePtr mergeTaskResource = resMgr->GetResource(id);
            taskResources.push_back(mergeTaskResource);
        }
        IndexReducerPtr reducer(mFactory->createIndexReducer(params));
        reducer->Init(indexResource, hint, taskResources);
        reducer->DoInit(indexResource);
        OutputSegmentMergeInfo outputSegMergeInfo;
        outputSegMergeInfo.directory = outputDir->MakeDirectory(meta.GetInsDirName(i));

        vector<IndexReduceItemPtr> reduceItems(segmentPaths.size());
        for (size_t j = 0; j < segmentPaths.size(); ++j) {
            if (!Load(params, segmentPaths[j], baseDocIds[j], docIdMap, reduceItems[j])) {
                IE_LOG(ERROR, "failed to load segment in path[%s]", segmentPaths[j].c_str());
                return false;
            }
        }

        if (!reducer->Reduce(reduceItems, {outputSegMergeInfo}, false, reduceResource)) {
            return false;
        }
        instRscVector.push_back(taskResources);
    }
    {
        /* End Parallel Reduce */
        OutputSegmentMergeInfo info;
        info.directory = outputDir;
        IndexReducerPtr reducer(mFactory->createIndexReducer(params));
        reducer->EndParallelReduce({info}, reduceTasks.size(), instRscVector);
    }
    return true;
}

IndexerResourcePtr AithetaTestBase::CreateIndexerResource(bool isFullBuildPhase) {
    IndexPartitionSchemaPtr schema;
    IndexPartitionOptions options;
    if (isFullBuildPhase) {
        options.GetBuildConfig().buildPhase = BuildConfigBase::BuildPhase::BP_FULL;
    } else {
        options.GetBuildConfig().buildPhase = BuildConfigBase::BuildPhase::BP_INC;
    }
    util::CounterMapPtr counterMap;
    PartitionMeta pMeta;
    return IndexerResourcePtr(new IndexerResource(schema, options, counterMap, pMeta, "", ""));
}

bool AithetaTestBase::LoadSegment(const util::KeyValueMap &params, const std::string &path, const DocIdMap &docIdMap,
                                  SegmentPtr &segment) {
    IE_NAMESPACE(index)::IndexReducerPtr reducer(mFactory->createIndexReducer(params));
    auto reduceItem = DYNAMIC_POINTER_CAST(AithetaIndexReduceItem, reducer->CreateReduceItem());
    auto dir = IE_NAMESPACE(file_system)::DirectoryCreator::Create(mFileSystem, path);
    if (!reduceItem->LoadIndex(dir)) {
        return false;
    }
    if (!reduceItem->UpdateDocId(docIdMap)) {
        return false;
    }
    segment = reduceItem->GetSegment();
    if (!segment) {
        return false;
    }
    return true;
}

size_t AithetaTestBase::InnerGetIndexSegmentLoad4ReduceMemoryUse(const std::vector<std::string> &pathVector,
                                                                 size_t parallelCount) {
    bool hasIndexSeg = false;
    for (auto &path : pathVector) {
        auto dir = IndexlibIoWrapper::CreateDirectory(path);
        SegmentMeta meta;
        meta.Load(dir);
        if (meta.getType() == SegmentType::kUnknown) {
            IE_LOG(ERROR, "kUnknown segment type");
            return 0l;
        } else if (meta.getType() != SegmentType::kRaw) {
            hasIndexSeg = true;
            break;
        }
    }

    size_t expectedMemSize = 0u;

    if (parallelCount > 1u && !hasIndexSeg) {
        size_t metaSize = 0u;
        for (auto &path : pathVector) {
            auto dir = IndexlibIoWrapper::CreateDirectory(path);
            SegmentMeta meta;
            meta.Load(dir);
            RawSegment segment(meta);
            size_t size = 0u;
            expectedMemSize +=
                FileSystemWrapper::GetFileLength(FileSystemWrapper::JoinPath(path, CATEGORYID_FILE_NAME)) +
                FileSystemWrapper::GetFileLength(FileSystemWrapper::JoinPath(path, DOCID_FILE_NAME)) +
                FileSystemWrapper::GetFileLength(FileSystemWrapper::JoinPath(path, PKEY_FILE_NAME)) +
                FileSystemWrapper::GetFileLength(FileSystemWrapper::JoinPath(path, EMBEDDING_FILE_NAME));
            metaSize += segment.GetMetaSize();
        }
        return (expectedMemSize * 1.0 / parallelCount) * RAW_SEG_FULL_REDUCE_MEM_USE_MAGIC_NUM + metaSize;
    }

    for (auto &path : pathVector) {
        auto dir = IndexlibIoWrapper::CreateDirectory(path);
        SegmentMeta meta;
        meta.Load(dir);

        switch (meta.getType()) {
            case SegmentType::kRaw: {
                size_t size =
                    FileSystemWrapper::GetFileLength(FileSystemWrapper::JoinPath(path, CATEGORYID_FILE_NAME)) +
                    FileSystemWrapper::GetFileLength(FileSystemWrapper::JoinPath(path, DOCID_FILE_NAME)) +
                    FileSystemWrapper::GetFileLength(FileSystemWrapper::JoinPath(path, PKEY_FILE_NAME)) +
                    FileSystemWrapper::GetFileLength(FileSystemWrapper::JoinPath(path, EMBEDDING_FILE_NAME));
                size *= RAW_SEG_FULL_REDUCE_MEM_USE_MAGIC_NUM;
                expectedMemSize += size;
            } break;
            case SegmentType::kIndex: {
                size_t pkey2docIdIdxMapSize = meta.getTotalDocNum() * (sizeof(int64_t) + sizeof(docid_t));
                expectedMemSize += dir->GetFileLength(INDEX_INFO_FILE_NAME) +
                                   dir->GetFileLength(JSONIZABLE_INDEX_INFO_FILE_NAME) +
                                   dir->GetFileLength(DOCID_REMAP_FILE_NAME) + pkey2docIdIdxMapSize;
            } break;
            case SegmentType::kMultiIndex: {
                ParallelReduceMeta reduceMeta;
                reduceMeta.Load(dir->GetPath());
                size_t pkey2docIdIdxMapSize = meta.getTotalDocNum() * (sizeof(int64_t) + sizeof(docid_t));
                expectedMemSize += pkey2docIdIdxMapSize;
                expectedMemSize += dir->GetFileLength(DOCID_REMAP_FILE_NAME);
                for (size_t i = 0; i < reduceMeta.parallelCount; ++i) {
                    string subPath = dir->GetPath() + "/inst_" + StringUtil::toString(reduceMeta.parallelCount) + "_" +
                                     StringUtil::toString(i);
                    auto subDir = IndexlibIoWrapper::CreateDirectory(subPath);
                    expectedMemSize += subDir->GetFileLength(INDEX_INFO_FILE_NAME) +
                                       subDir->GetFileLength(JSONIZABLE_INDEX_INFO_FILE_NAME);
                }
            } break;
            default: {
                IE_LOG(ERROR, "unexpect segment type");
                return 0l;
            }
        }
    }
    return expectedMemSize;
}

size_t AithetaTestBase::InnerGetIndexSegmentLoad4RetrieveMemoryUse(const std::string &dirPath) {
    auto dir = IndexlibIoWrapper::CreateDirectory(dirPath);
    assert(nullptr != dir);
    SegmentMeta meta;
    meta.Load(dir);
    if (dir->IsExist("parallel_meta")) {
        if (meta.getType() != SegmentType::kMultiIndex) {
            IE_LOG(ERROR, "unexpected segment type[%d]", (int)meta.getType());
            return 0u;
        }
        ParallelReduceMeta reduceMeta;
        reduceMeta.Load(dirPath);
        assert(reduceMeta.parallelCount > 1u);
        size_t expectLoad4RetrieveSize = 0u;
        size_t maxRemapFileSize = 0u;

        expectLoad4RetrieveSize +=
            FileSystemWrapper::GetFileLength(FileSystemWrapper::JoinPath(dirPath, DOCID_REMAP_FILE_NAME));

        for (size_t i = 0; i < reduceMeta.parallelCount; ++i) {
            string subPath =
                dirPath + "/inst_" + StringUtil::toString(reduceMeta.parallelCount) + "_" + StringUtil::toString(i);
            expectLoad4RetrieveSize +=
                FileSystemWrapper::GetFileLength(
                    FileSystemWrapper::JoinPath(subPath, JSONIZABLE_INDEX_INFO_FILE_NAME)) +
                FileSystemWrapper::GetFileLength(FileSystemWrapper::JoinPath(subPath, INDEX_INFO_FILE_NAME)) +
                FileSystemWrapper::GetFileLength(FileSystemWrapper::JoinPath(subPath, INDEX_FILE_NAME));
            auto subDir = IndexlibIoWrapper::CreateDirectory(subPath);
            if (subDir->IsExist(DOCID_REMAP_FILE_NAME)) {
                expectLoad4RetrieveSize +=
                    FileSystemWrapper::GetFileLength(FileSystemWrapper::JoinPath(subPath, DOCID_REMAP_FILE_NAME));
            }
        }
        expectLoad4RetrieveSize *= IDX_SEG_LOAD_4_RETRIEVE_MEM_USE_MAGIC_NUM;
        expectLoad4RetrieveSize +=
            meta.getTotalDocNum() * (sizeof(docid_t) + sizeof(int64_t)) * PKEY_2_DOCID_MAP_MAGIC_NUM;
        return expectLoad4RetrieveSize;
    } else {
        size_t expectLoad4RetrieveSize =
            FileSystemWrapper::GetFileLength(FileSystemWrapper::JoinPath(dirPath, INDEX_FILE_NAME)) +
            FileSystemWrapper::GetFileLength(FileSystemWrapper::JoinPath(dirPath, DOCID_REMAP_FILE_NAME)) +
            FileSystemWrapper::GetFileLength(FileSystemWrapper::JoinPath(dirPath, INDEX_INFO_FILE_NAME)) +
            FileSystemWrapper::GetFileLength(FileSystemWrapper::JoinPath(dirPath, JSONIZABLE_INDEX_INFO_FILE_NAME));
        expectLoad4RetrieveSize *= IDX_SEG_LOAD_4_RETRIEVE_MEM_USE_MAGIC_NUM;

        expectLoad4RetrieveSize +=
            meta.getTotalDocNum() * (sizeof(docid_t) + sizeof(int64_t)) * PKEY_2_DOCID_MAP_MAGIC_NUM;
        return expectLoad4RetrieveSize;
    }
}

IE_NAMESPACE_END(aitheta_plugin);
