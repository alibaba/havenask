#include "aitheta_indexer/plugins/aitheta/index_segment_reader.h"

#include <functional>
#include <sstream>
#include <memory>

#include <autil/Lock.h>
#include <aitheta/index_framework.h>
#include <aitheta/utility/vector.h>
#include <indexlib/file_system/buffered_file_reader.h>
#include <indexlib/storage/file_system_wrapper.h>

#include "aitheta_indexer/plugins/aitheta/util/flexible_float_index_holder.h"
#include "aitheta_indexer/plugins/aitheta/util/indexlib_io_wrapper.h"
#include "aitheta_indexer/plugins/aitheta/util/input_storage.h"
#include "aitheta_indexer/plugins/aitheta/util/knn_params_selector/knn_params_selector_factory.h"
#include "aitheta_indexer/plugins/aitheta/util/output_storage.h"
#include "aitheta_indexer/plugins/aitheta/util/search_work_item.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);
using namespace std;
using namespace aitheta;
using namespace autil;
using namespace kmonitor;

IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(common);
IE_LOG_SETUP(aitheta_plugin, IndexSegmentReader);

ContextHolder::~ContextHolder() {
    mLock.wrlock();
    for (auto &outerKv : mContext) {
        auto &signatureContext = outerKv.second;
        for (auto &innerKv : signatureContext) {
            auto &context = innerKv.second;
            delete context;
            context = nullptr;
        }
    }
    mLock.unlock();
}

bool ContextHolder::getContext(const SearchInfo &info, IndexSearcher::Context *&ret) {
    pthread_t id = pthread_self();
    mLock.rdlock();
    auto outItr = mContext.find(id);
    if (outItr != mContext.end()) {
        auto &sigContext = outItr->second;
        auto inItr = sigContext.find(info.mSignature);
        if (inItr != sigContext.end()) {
            ret = inItr->second;
            mLock.unlock();
            return true;
        }
    }
    mLock.unlock();
    auto context = info.mSearcher->createContext(info.mIndexParams);
    if (nullptr != context) {
        mLock.wrlock();
        ret = context.release();
        mContext[id][info.mSignature] = ret;
        mLock.unlock();
    } else {
        return false;
    }
    return true;
}

IndexSegmentReader::~IndexSegmentReader() {
    if (mThreadPool) {
        mThreadPool->stop();
        mThreadPool.reset();
        IE_LOG(INFO, "thread pool is released successfully");
    }
}

bool IndexSegmentReader::InitDistType() {
    auto distType = GetValueFromKeyValueMap(mKvParam, SEARCH_METRIC_TYPE, "");
    if (distType.empty()) {
        distType = GetValueFromKeyValueMap(mKvParam, BUILD_METRIC_TYPE, "");
    }
    if (distType.empty()) {
        IE_LOG(ERROR, "failed to get distance type");
        return false;
    }

    if (L2 == distType) {
        mDistType = DistanceType::kL2;
        return true;
    }

    if (INNER_PRODUCT == distType) {
        mDistType = DistanceType::kInnerProduct;
        return true;
    }

    IE_LOG(ERROR, " unknown distance type: [%s]", distType.c_str());
    return false;
}

bool IndexSegmentReader::LoadSegment(const IE_NAMESPACE(file_system)::DirectoryPtr &dir) {
    if (mSegmentMeta.getType() == SegmentType::kIndex) {
        IndexSegmentPtr segment(new IndexSegment(mSegmentMeta));
        if (!segment->Load(dir)) {
            return false;
        }
        mSegVector.push_back(segment);
    } else if (mSegmentMeta.getType() == SegmentType::kMultiIndex) {
        ParallelSegmentPtr pSegment(new ParallelSegment(mSegmentMeta));
        if (!pSegment->Load(dir)) {
            return false;
        }
        mSegVector = pSegment->GetSegmentVector();
    } else {
        IE_LOG(ERROR, "unexpected index type[%d]", (int)mSegmentMeta.getType());
        return false;
    }
    IE_LOG(INFO, "finish loading all the index segments in path[%s]", dir->GetPath().c_str());
    return true;
}

bool IndexSegmentReader::InitSearchInfo() {
    IE_LOG(INFO, "starting init search info");
    for (auto &segment : mSegVector) {
        int8_t *address = (int8_t *)0;
        if (!segment->GetIndexBaseAddress(address)) {
            return false;
        }
        if (address == (int8_t *)0) {
            continue;
        }

        const auto &indexInfos = segment->getCatIndexInfos();
        const auto &jIndexInfos = segment->getJsonizableIndexInfos();

        for (size_t i = 0; i < indexInfos.size(); ++i) {
            auto &info = indexInfos[i];
            KnnParamsSelectorFactory paramsSelectorFactory;
            IKnnParamsSelectorPtr paramsSelectorPtr =
                paramsSelectorFactory.CreateSelector(mComParam.mIndexType, mComParam, mKnnConfig, info.mDocNum);
            if (!paramsSelectorPtr) {
                IE_LOG(ERROR, "failed to get [%s]'s params selector", mComParam.mIndexType.c_str());
                return false;
            }
            auto name = paramsSelectorPtr->GetSearcherName();
            auto searcher = IndexFactory::CreateSearcher(name);
            if (nullptr == searcher) {
                IE_LOG(ERROR, "failed to create searcher[%s]", name.data());
                return false;
            }

            mKvParam[DOCUMENT_NUM] = to_string(info.mDocNum);
            IndexParams iParams;
            if (!paramsSelectorPtr->InitKnnSearcherParams(mKvParam, iParams, true)) {
                IE_LOG(ERROR, "failed to init searcher params");
                return false;
            }
            int ret = searcher->init(iParams);
            if (ret < 0) {
                IE_LOG(ERROR, "failed to init searcher, error[%s]", IndexError::What(ret));
                return false;
            }

            IndexStoragePtr storage(new InputStorage(address));
            ret = searcher->loadIndex(".", storage);

            if (ret < 0) {
                IE_LOG(ERROR, "failed to load index, error[%s]", IndexError::What(ret));
                return false;
            }

            MipsParams mipsParams(false, 0U, 0.0F, 0.0F);
            aitheta::MipsReformer reformer;
            if (i < jIndexInfos.size()) {
                mipsParams = jIndexInfos[i].mMipsParams;
                if (mipsParams.enable) {
                    reformer = aitheta::MipsReformer(mipsParams.mval, mipsParams.uval, mipsParams.norm);
                }
            }
            int64_t signature = paramsSelectorPtr->GetParamsSignature(iParams);
            SearchInfo searchInfo(signature, info.mDocNum, searcher, iParams, mipsParams.enable, reformer);
            if (!mCatId2Searcher.emplace(info.mCatId, searchInfo).second) {
                IE_LOG(ERROR, "category[%ld] already exists", info.mCatId)
                return false;
            }
        }
    }
    IE_LOG(INFO, "finish init search info");
    return true;
}

void IndexSegmentReader::InitThreadPool() {
    size_t threadNum = 0u;
    size_t queueSize = 0u;

    auto val = GetValueFromKeyValueMap(mKvParam, ENABLE_PARALLEL_SEARCH, "false");
    if (!autil::StringUtil::fromString(val, mEnableParallelSearch)) {
        IE_LOG(INFO, "failed to parse key[%s] val[%s] to bool, disable parallel search", ENABLE_PARALLEL_SEARCH.c_str(),
               val.c_str());
        mEnableParallelSearch = false;
        return;
    }
    if (mEnableParallelSearch == false) {
        IE_LOG(INFO, "parallel search is disabled");
        return;
    }

    val = GetValueFromKeyValueMap(mKvParam, PARALLEL_SEARCH_THREAD_NUM, "");
    if (!autil::StringUtil::fromString(val, threadNum) || threadNum == 0u) {
        threadNum = 64u;
        IE_LOG(INFO, "failed to parse: key[%s] val[%s], use default val [%lu]", PARALLEL_SEARCH_THREAD_NUM.c_str(),
               val.c_str(), threadNum);
    }

    val = GetValueFromKeyValueMap(mKvParam, PARALLEL_SEARCH_QUEUE_SIZE, "");
    if (!autil::StringUtil::fromString(val, queueSize) || queueSize == 0u) {
        queueSize = 256u;
        IE_LOG(INFO, "failed to parse: key[%s] val[%s], use default val [%lu]", PARALLEL_SEARCH_QUEUE_SIZE.c_str(),
               val.c_str(), queueSize);
    }

    mThreadPool.reset(new autil::ThreadPool(threadNum, queueSize));
    const static string kThreadName = "aitheta";
    if (!mThreadPool->start(kThreadName)) {
        IE_LOG(ERROR, "failed to start parallel search pool");
        mThreadPool.reset();
        return;
    }
    IE_LOG(INFO, "start parallel search pool success with params: %s=%lu, %s=%lu", PARALLEL_SEARCH_THREAD_NUM.c_str(),
           threadNum, PARALLEL_SEARCH_QUEUE_SIZE.c_str(), queueSize);
}

bool IndexSegmentReader::InitContextHolder() {
    if (nullptr == mContextHolder) {
        mContextHolder.reset(new ContextHolder);
    }
    return true;
}

bool IndexSegmentReader::Open(const DirectoryPtr &dir) {
    mIndexName = GetValueFromKeyValueMap(mKvParam, INDEX_NAME, "default_index");

    if (!LoadSegment(dir)) {
        return false;
    }

    if (!InitDistType()) {
        return false;
    }

    if (!InitSearchInfo()) {
        return false;
    }

    if (!InitContextHolder()) {
        return false;
    }

    InitThreadPool();

    IE_LOG(INFO, "load indices in path[%s]", dir->GetPath().data());
    return true;
}

bool IndexSegmentReader::UpdatePkey2DocId(docid_t baseDocId, const std::vector<SegmentReaderPtr> &segReaderVector,
                                          const std::vector<docid_t> &segBaseDocIdVector) {
    if (segReaderVector.empty()) {
        return true;
    }

    RawSegmentReaderPtr rawSegReader;
    vector<RawSegmentPtr> rawSegVector;
    for (auto &reader : segReaderVector) {
        rawSegReader = DYNAMIC_POINTER_CAST(RawSegmentReader, reader);
        rawSegVector.push_back(rawSegReader->getSegment());
    }

    if (mSegVector.empty()) {
        IE_LOG(ERROR, "mSegVector is empty");
        return false;
    }

    if (!mSegVector[0]->MergeRawSegments(baseDocId, rawSegVector, segBaseDocIdVector)) {
        return false;
    }

    return true;
}

std::vector<MatchInfo> IndexSegmentReader::Search(const std::vector<QueryInfo> &queryInfos,
                                                  autil::mem_pool::Pool *sessionPool, int32_t topK, bool enableFilter,
                                                  float threshold) {
    int64_t startTime = TimeUtility::currentTime();
    vector<vector<Document>> totalDocs(queryInfos.size());

    if (mEnableParallelSearch) {
        ParallelSearch(queryInfos, topK, totalDocs);
    } else {
        SequentialSearch(queryInfos, topK, totalDocs);
    }

    std::map<docid_t, float> docIds2score;
    Merge(totalDocs, queryInfos, docIds2score);
    if (enableFilter) {
        Filter(threshold, docIds2score);
    }

    MatchInfo result;
    Format(docIds2score, sessionPool, result);

    std::vector<MatchInfo> matchInfos;
    matchInfos.push_back(move(result));
    return matchInfos;
}

void IndexSegmentReader::ParallelSearch(const vector<QueryInfo> &queryInfos, int32_t topK,
                                        vector<vector<Document>> &totalDocs) {
    if (queryInfos.empty()) {
        return;
    }
    size_t locQueryId = 0u;
    {
        // pick a query which is the most time-consuming, run it in the local thread
        size_t maxDocNum = 0u;
        for (size_t i = 0; i < queryInfos.size(); ++i) {
            int64_t catId = queryInfos[i].mCatId;
            auto iter = mCatId2Searcher.find(catId);
            if (iter == mCatId2Searcher.end()) {
                continue;
            }
            size_t docNum = iter->second.mDocNum;
            if (docNum > maxDocNum) {
                maxDocNum = docNum;
                locQueryId = i;
            }
        }
    }

    size_t taskNumInPool = queryInfos.size() - 1;
    shared_ptr<TerminateNotifier> notifier(new TerminateNotifier());
    for (size_t i = 0; i < taskNumInPool; ++i) {
        notifier->inc();
    }
    for (size_t pos = 0; pos < queryInfos.size(); ++pos) {
        if (pos == locQueryId) {
            continue;
        }
        auto closure = [&, this, pos]() { this->SearchOne(queryInfos[pos], topK, totalDocs[pos]); };
        SearchWorkItem *workItem = new SearchWorkItem(closure, notifier);
        auto ret = mThreadPool->pushWorkItem(workItem, true);
        if (ret != autil::ThreadPool::ERROR_TYPE::ERROR_NONE) {
            delete workItem;
            IE_LOG(ERROR, "failed to push work item, error no [%d]", ret);
            continue;
        }
    }

    SearchOne(queryInfos[locQueryId], topK, totalDocs[locQueryId]);
    // wait for the unfinished tasks
    notifier->wait();
}

void IndexSegmentReader::SequentialSearch(const vector<QueryInfo> &queryInfos, int32_t topK,
                                          vector<vector<Document>> &totalDocs) {
    for (size_t pos = 0; pos < queryInfos.size(); ++pos) {
        SearchOne(queryInfos[pos], topK, totalDocs[pos]);
    }
}

void IndexSegmentReader::SearchOne(const QueryInfo &queryInfo, int32_t topK, std::vector<Document> &docs) {
    int64_t catId = queryInfo.mCatId;
    auto itr = mCatId2Searcher.find(catId);
    if (itr == mCatId2Searcher.end()) {
        return;
    }
    const auto &searchInfo = itr->second;
    IndexSearcher::Context *ctx = nullptr;
    if (nullptr == mContextHolder) {
        IE_LOG(ERROR, "context holder is nullptr");
        return;
    }
    if (!mContextHolder->getContext(searchInfo, ctx)) {
        IE_LOG(WARN, "failed to get context for query[%s]", queryInfo.toString().c_str());
        return;
    }

    if (ctx == nullptr) {
        IE_LOG(ERROR, "failed to find ctx, return docs is null");
        return;
    }

    ContextPtr context(ctx);
    int ret = -1;
    int dim = queryInfo.mDim;
    if (searchInfo.mMipsEnanble) {
        aitheta::Vector<float> aithetaVec;
        searchInfo.mReformer.transQuery1(queryInfo.mEmbedding.get(), dim, &aithetaVec);
        int32_t adjustDim = dim + searchInfo.mReformer.m();
        ret = searchInfo.mSearcher->knnSearch(topK, aithetaVec.data(), adjustDim, context);
    } else {
        ret = searchInfo.mSearcher->knnSearch(topK, queryInfo.mEmbedding.get(), dim, context);
    }
    if (ret < 0) {
        IE_LOG(WARN, "failed to search query [%s], topK[%d], err msg[%s]", queryInfo.toString().c_str(), topK,
               IndexError::What(ret));
        context.release();
        return;
    }

    docs = context->result();
    context.release();
}

void IndexSegmentReader::Merge(const std::vector<std::vector<Document>> &docsCollector,
                               const std::vector<QueryInfo> &queryInfos, std::map<docid_t, float> &docIds2score) const {
    if (unlikely(mSegVector.empty())) {
        IE_LOG(ERROR, "mSegVector is empty");
        return;
    }

    const auto &docIdArray = mSegVector.front()->getDocIdArray();
    for (size_t pos = 0; pos < docsCollector.size(); ++pos) {
        const auto iter = mCatId2Searcher.find(queryInfos[pos].mCatId);
        if (iter == mCatId2Searcher.end()) {
            continue;
        }
        const auto &searchInfo = iter->second;
        float sqnorm = 0.0f;
        if (searchInfo.mMipsEnanble) {
            float *data = queryInfos[pos].mEmbedding.get();
            for (int d = 0; d < mComParam.mDim; ++d) {
                sqnorm += data[d] * data[d];
            }
        }
        for (const auto &doc : docsCollector[pos]) {
            if (unlikely(INVALID_DOCID == (docid_t)doc.key)) {
                continue;
            }
            auto newDocId = docIdArray[doc.key];
            if (unlikely(INVALID_DOCID == newDocId)) {
                continue;
            }
            auto itr = docIds2score.find(newDocId);
            float score = doc.score;
            if (searchInfo.mMipsEnanble) {
                score = searchInfo.mReformer.normalize1(sqnorm, doc.score);
            }
            if (itr == docIds2score.end()) {
                docIds2score.emplace(newDocId, score);
            } else {
                auto &bestScore = itr->second;
                if (DistanceType::kL2 == mDistType) {
                    bestScore = bestScore < score ? bestScore : score;
                } else {
                    bestScore = bestScore > score ? bestScore : score;
                }
            }
        }
    }
}

void IndexSegmentReader::Filter(float threshold, std::map<docid_t, float> &docIds2score) const {
    for (auto iter = docIds2score.begin(); iter != docIds2score.end();) {
        if (iter->second > threshold && DistanceType::kL2 == mDistType) {
            iter = docIds2score.erase(iter);
            continue;
        }
        if (iter->second < threshold && DistanceType::kInnerProduct == mDistType) {
            iter = docIds2score.erase(iter);
            continue;
        }
        ++iter;
    }
}

void IndexSegmentReader::Format(const std::map<docid_t, float> &docIds2score, autil::mem_pool::Pool *sessionPool,
                                MatchInfo &result) const {
    result.pool = sessionPool;
    result.matchCount = docIds2score.size();
    result.docIds = IE_POOL_COMPATIBLE_NEW_VECTOR(sessionPool, docid_t, result.matchCount);
    if (nullptr == result.docIds) {
        IE_LOG(ERROR, "alloc mem for docIds failed");
    }

    result.matchValues = IE_POOL_COMPATIBLE_NEW_VECTOR(sessionPool, matchvalue_t, result.matchCount);
    if (nullptr == result.matchValues) {
        IE_LOG(ERROR, "alloc mem for matchValue failed");
    }

    auto itr = docIds2score.begin();
    for (size_t i = 0; i < result.matchCount; ++i) {
        result.docIds[i] = itr->first;
        result.matchValues[i].SetFloat(itr->second);
        ++itr;
    }
}

IE_NAMESPACE_END(aitheta_plugin);
