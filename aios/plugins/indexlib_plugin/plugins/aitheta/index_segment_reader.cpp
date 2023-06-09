/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "indexlib_plugin/plugins/aitheta/index_segment_reader.h"
#include "indexlib_plugin/plugins/aitheta/util/input_storage.h"
#include "indexlib_plugin/plugins/aitheta/util/knn_params_selector/knn_searcher_factory.h"
#include "indexlib_plugin/plugins/aitheta/util/metric_reporter.h"

using namespace std;
using namespace aitheta;
using namespace autil;
using namespace indexlib::index;
namespace indexlib {
namespace aitheta_plugin {
IE_LOG_SETUP(aitheta_plugin, IndexSegmentReader);

bool IndexSegmentReader::Init(const ContextHolderPtr &holder, docid_t sgtBase,
                              const DeletionMapReaderPtr &deletionMapRr) {
    mSgtBase = sgtBase;
    if (deletionMapRr && (mSchemaParam.rtOption.enable || mSchemaParam.incOption.enable)) {
        mDocidFilter = [=](uint64_t docid) { return deletionMapRr->IsDeleted((docid_t)(docid + mSgtBase)); };
    }

    if (mSegment->GetType() != SegmentType::kIndex && mSegment->GetType() != SegmentType::kPMIndex) {
        IE_LOG(ERROR, "unexpected type[%s], failed to init", Type2Str(mSegment->GetType()).c_str());
        return false;
    }

    auto indexSgt = DYNAMIC_POINTER_CAST(IndexSegment, mSegment);
    if (!indexSgt) {
        IE_LOG(ERROR, "failed to dynamical-cast to IndexSegment");
        return false;
    }

    vector<IndexSegmentPtr> segments;
    if (indexSgt->GetType() == SegmentType::kPMIndex) {
        segments = indexSgt->GetSubSegments();
    } else {
        segments = {indexSgt};
    }

    for (auto &segment : segments) {
        auto indexProvider = segment->GetIndexProvider();
        if (!indexProvider->GetLength()) {
            IE_LOG(WARN, "finish init one sgt, but index is empty");
            continue;
        }

        int8_t *indexAddr = static_cast<int8_t *>(indexProvider->GetBaseAddress());
        if (!indexAddr) {
            IE_LOG(ERROR, "failed to get address of index file");
            return false;
        }

        KnnConfig knnCfg = segment->GetOfflineKnnConfig();
        const auto &offlineIndexAttrHolder = segment->GetOfflineIndexAttrHolder();
        for (size_t idx = 0u; idx < offlineIndexAttrHolder.Size(); ++idx) {
            auto &indexAttr = offlineIndexAttrHolder.Get(idx);
            if (!InitBuiltIndex(indexAttr, knnCfg, holder, indexAddr)) {
                IE_LOG(ERROR, "failed to init normal index ,catid[%ld]", indexAttr.catid);
                return false;
            }
        }
    }

    IE_LOG(INFO, "finish initializing index segment reader");
    return true;
}

bool IndexSegmentReader::InitBuiltIndex(const OfflineIndexAttr &indexAttr, const KnnConfig &knnCfg,
                                        const ContextHolderPtr &holder, const int8_t *baseIndexAddr) {
    IndexParams indexParams;
    int64_t signature = 0u;
    bool enableGpu = false;

    bool isBuiltOnline = mSegment->GetMeta().IsBuiltOnline();
    IndexSearcherPtr searcher =
        KnnSearcherFactory::Create(mSchemaParam, knnCfg, indexAttr, isBuiltOnline, indexParams, signature, enableGpu);
    if (!searcher) {
        IE_LOG(ERROR, "failed to create searcher, index type[%s]", mSchemaParam.indexType.c_str());
        return false;
    }

    IndexStoragePtr storage(new InputStorage(baseIndexAddr + indexAttr.offset));
    int32_t ret = searcher->loadIndex(".", storage);
    if (ret < 0) {
        IE_LOG(ERROR, "failed to load index, error[%s]", IndexError::What(ret));
        return false;
    }

    OnlineIndexPtr builtIndex(new BuiltIndex(searcher, indexAttr, signature, indexParams, enableGpu, holder));
    if (!mOnlineIndexHolder->Add(indexAttr.catid, builtIndex)) {
        IE_LOG(ERROR, "failed to add index, catid[%ld] ", indexAttr.catid);
        return false;
    }

    IE_LOG(DEBUG, "init built index: catid[%ld], index size[%lu]", indexAttr.catid, indexAttr.size);
    return true;
}

bool IndexSegmentReader::Search(const vector<Query> &queries, SearchResult &result) {
    vector<Query> modifiedQs;
    if (!ModifyQuery(queries, modifiedQs)) {
        return false;
    }
    // TODO(richard.sy): support concurrent search
    // ConcurrentSearch(modifiedQs, result);
    return SequentialSearch(modifiedQs, result);
}

bool IndexSegmentReader::SequentialSearch(const vector<Query> &queries, SearchResult &result) {
    for (const auto &query : queries) {
        Search(query, result);
    }

    return true;
}

bool IndexSegmentReader::ModifyQuery(const vector<Query> &srcs, vector<Query> &dsts) {
    vector<Query> ques;
    for (const auto &query : srcs) {
        if (query.topk && mOnlineIndexHolder->Has(query.catid)) {
            uint32_t modifiedTopk = 0u;
            ModifyTopk(query.catid, query.topk, modifiedTopk);
            if (modifiedTopk) {
                ques.push_back(query);
                ques.back().topk = modifiedTopk;
            }
        } else {
            IE_LOG(DEBUG, "topk[%u], has index[%ld]:[%d]", query.topk, query.catid,
                   mOnlineIndexHolder->Has(query.catid));
        }
    }
    IE_LOG(DEBUG, "query number before[%lu], after[%lu]", srcs.size(), ques.size());

    auto cmpFunc = [](const Query &l, const Query &r) { return l.catid < r.catid; };
    sort(ques.begin(), ques.end(), cmpFunc);

    uint32_t originDim = mSchemaParam.dimension;
    for (size_t idx = 0u; idx < ques.size();) {
        int64_t catid = ques[idx].catid;
        OnlineIndexPtr index;
        mOnlineIndexHolder->Get(catid, index);
        assert(index);

        size_t first = idx++;
        if (index->enableGpu) {
            while (idx < ques.size() && catid == ques[idx].catid) {
                ++idx;
            }
        }

        size_t queryNum = idx - first;
        bool enableMips = index->reformer.m() > 0u;
        if (1u == queryNum && !enableMips) {
            dsts.push_back(ques[first]);
            continue;
        }

        int32_t dim = originDim;
        shared_ptr<float> squareNorms;
        if (enableMips) {
            dim += index->reformer.m();
            MALLOC_CHECK(squareNorms, queryNum, float);
        }

        EmbeddingPtr modifiedE;
        MALLOC_CHECK(modifiedE, dim * queryNum, float);

        for (size_t qNo = first; qNo < idx; ++qNo) {
            float *src = ques[qNo].embedding.get();
            float *dst = modifiedE.get() + (qNo - first) * dim;
            aitheta::Vector<float> mipsE;
            if (enableMips) {
                auto &originE = ques[qNo].embedding;
                float &sqn = squareNorms.get()[qNo - first];
                sqn = 0.0f;
                for (uint32_t d = 0u; d < originDim; ++d) {
                    sqn += pow(originE.get()[d], 2);
                }
                index->reformer.transQuery1(originE.get(), originDim, &mipsE);
                src = mipsE.data();
            }
            memcpy((void *)dst, (void *)src, dim * sizeof(float));
        }

        uint32_t topk = ques[first].topk;
        bool enableFilter = ques[first].enableScoreFilter;
        float threshold = ques[first].threshold;
        dsts.emplace_back(catid, topk, enableFilter, threshold, dim, modifiedE, queryNum, squareNorms);
    }
    return true;
}

bool IndexSegmentReader::Search(const Query &query, SearchResult &result) {
    OnlineIndexPtr index;
    if (!mOnlineIndexHolder->Get(query.catid, index)) {
        IE_LOG(DEBUG, "failed to find catid[%ld]", query.catid);
        return false;
    }

    IndexSearcher::Context *ctx = nullptr;
    if (!index->Search(query, mDocidFilter, ctx)) {
        return false;
    }

    if (!ctx) {
        IE_LOG(ERROR, "failed to get proxima search result");
        return false;
    }

    SearchResult subResult;
    if (!result.Alloc(query.topk * query.queryNum, subResult)) {
        return false;
    }

    for (uint32_t idx = 0u; idx < query.queryNum; ++idx) {
        auto &docs = ctx->result(idx);
        for (const auto &doc : docs) {
            if (unlikely((docid_t)doc.key == INVALID_DOCID)) {
                continue;
            }
            docid_t key = mSgtBase + doc.key;
            float score = doc.score;
            if (query.enableScoreFilter) {
                if ((mSchemaParam.searchDistType == DistType::kIP && score <= query.threshold) ||
                    (mSchemaParam.searchDistType == DistType::kL2 && score >= query.threshold)) {
                    continue;
                }
            }
            bool enableMips = index->reformer.m() > 0 ? true : false;
            if (enableMips) {
                assert(query.originSqNorms);
                float sqn = query.originSqNorms.get()[idx];
                score = index->reformer.normalize1(sqn, score);
            }
            subResult.AddUnsafe(key, score);
        }
    }

    IE_LOG(DEBUG, "query search with catid[%ld], SgtId[%u]", query.catid, mSgtBase);
    return true;
}

const OfflineIndexAttrHolder &IndexSegmentReader::GetOfflineIndexAttrHolder() const {
    auto indexSegment = DYNAMIC_POINTER_CAST(IndexSegment, mSegment);

    if (!indexSegment) {
        static const OfflineIndexAttrHolder holder;
        return holder;
    }
    return indexSegment->GetOfflineIndexAttrHolder();
}

}
}
