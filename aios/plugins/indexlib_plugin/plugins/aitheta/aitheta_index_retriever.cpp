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
#include "indexlib_plugin/plugins/aitheta/aitheta_index_retriever.h"

#include <unordered_set>

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/URLUtil.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib_plugin/plugins/aitheta/aitheta_rt_segment_retriever.h"
#include "indexlib_plugin/plugins/aitheta/common_define.h"
#include "indexlib_plugin/plugins/aitheta/rt_segment_reader.h"
#include "indexlib_plugin/plugins/aitheta/util/custom_logger.h"
#include "indexlib_plugin/plugins/aitheta/util/string_parser.h"

using namespace std;
using namespace autil;
using namespace aitheta;

using namespace indexlib::file_system;
using namespace indexlib::index;
using namespace indexlib::file_system;
using namespace indexlib::common;
namespace indexlib {
namespace aitheta_plugin {

IE_LOG_SETUP(aitheta_plugin, AithetaIndexRetriever);

void AithetaIndexRetriever::InitQueryTopkRatio() {
    size_t sgtNum = mSgtReaders.size();
    vector<unordered_map<int64_t, float>> topkRatios(sgtNum);
    if (TopkOption::Plan::kHighRec == mSchemaParam.topkOption.plan) {
        static const float kTopkAdjustRatio = 1.0f;
        for (size_t sgtNo = 0u; sgtNo < sgtNum; ++sgtNo) {
            auto &holder = mSgtReaders[sgtNo]->GetOfflineIndexAttrHolder();
            for (size_t attrNo = 0u; attrNo < holder.Size(); ++attrNo) {
                auto catid = holder.Get(attrNo).catid;
                topkRatios[sgtNo].emplace(catid, kTopkAdjustRatio);
                if (mRtSgtNo != INVALID_SGT_NO) {
                    topkRatios[mRtSgtNo].emplace(catid, kTopkAdjustRatio);
                }
            }
        }
    } else {
        vector<unordered_map<catid_t, size_t>> catid2DocNums(sgtNum);
        unordered_set<catid_t> catids;
        for (size_t sgtNo = 0u; sgtNo < sgtNum; ++sgtNo) {
            auto &holder = mSgtReaders[sgtNo]->GetOfflineIndexAttrHolder();
            for (size_t attrNo = 0u; attrNo < holder.Size(); ++attrNo) {
                auto &indexAttr = holder.Get(attrNo);
                catid2DocNums[sgtNo].emplace(indexAttr.catid, indexAttr.docNum);
                catids.emplace(indexAttr.catid);
            }
        }
        for (auto catid : catids) {
            size_t total = 0u;
            for (size_t sgtNo = 0u; sgtNo < sgtNum; ++sgtNo) {
                total += catid2DocNums[sgtNo][catid];
            }
            if (!total) {
                continue;
            }
            for (int32_t sgtNo = 0u; sgtNo < (int32_t)sgtNum; ++sgtNo) {
                if (sgtNo == mRtSgtNo) {
                    topkRatios[sgtNo][catid] = 1.0f;
                } else {
                    float ratio = catid2DocNums[sgtNo][catid] * mSchemaParam.topkOption.zoomout / total;
                    topkRatios[sgtNo][catid] = std::min(1.0f, ratio);
                }
            }
        }
    }
    for (size_t sgtNo = 0u; sgtNo < sgtNum; ++sgtNo) {
        mSgtReaders[sgtNo]->InitQueryTopkRatio(topkRatios[sgtNo]);
        IE_LOG(DEBUG, "segment no[%lu], topk ratios map[%s]", sgtNo, HashMap2Str(topkRatios[sgtNo]).c_str());
    }
}

bool AithetaIndexRetriever::InitRecallReporter() {
    const auto &metricOption = mSchemaParam.metricOption;
    if (!metricOption.enableRecReport) {
        return false;
    }
    uint32_t interval = metricOption.recReportSampleInterval;
    mRecallReporter.reset(new RecallReporter(mSgtReaders, _metricReporter, mRtSgtNo, interval));
    return mRecallReporter->Init();
}

bool AithetaIndexRetriever::Init(const DeletionMapReaderPtr &deletionMapRr,
                                 const std::vector<IndexSegmentRetrieverPtr> &retrievers,
                                 const IndexerResourcePtr &resource) {
    assert(resource);
    IndexRetriever::Init(deletionMapRr, retrievers, resource);
    auto schema = mIndexerResource->schema;
    string tableName = schema != nullptr ? schema->GetSchemaName() : "DEFAULT";
    _metricReporter.reset(new MetricReporter(resource->metricProvider, tableName, resource->indexName));
    RegisterGlobalIndexLogger();

    docid_t rtSgtBaseDocId = INVALID_DOCID;
    auto ctxHolder = ContextHolderPtr(new ContextHolder);
    for (size_t sgtNo = 0u; sgtNo < retrievers.size(); ++sgtNo) {
        auto &indexerSegData = retrievers[sgtNo]->GetIndexerSegmentData();
        docid_t baseDocId = indexerSegData.baseDocId;
        IndexSegmentReaderPtr sgtRr;
        auto indexSgtRetriever = DYNAMIC_POINTER_CAST(AithetaIndexSegmentRetriever, retrievers[sgtNo]);
        if (indexSgtRetriever) {
            sgtRr = indexSgtRetriever->GetSegmentReader();
            if (!sgtRr) {
                IE_LOG(INFO, "segment reader not exist in segment id[%lu],ignore it", sgtNo);
                continue;
            }
            sgtRr->SetMetricReporter(_metricReporter);
            if (!sgtRr->Init(ctxHolder, baseDocId, deletionMapRr)) {
                return false;
            }
        } else {
            auto rtSgtRetriever = DYNAMIC_POINTER_CAST(AithetaRtSegmentRetriever, retrievers[sgtNo]);
            if (!rtSgtRetriever) {
                continue;
            }
            sgtRr = rtSgtRetriever->GetSegmentReader();
            sgtRr->SetMetricReporter(_metricReporter);
            mRtSgtNo = sgtNo;
            rtSgtBaseDocId = baseDocId;
        }
        mSgtReaders.push_back(sgtRr);
    }
    if (mRtSgtNo != INVALID_SGT_NO && !InitRtSgtReader(ctxHolder, rtSgtBaseDocId, deletionMapRr)) {
        return false;
    }
    InitQueryTopkRatio();
    InitRecallReporter();

    METRIC_SETUP(mTotalSeekCount, "TotalSeekCount", kmonitor::GAUGE);
    METRIC_SETUP(mTotalSeekLatency, "TotalSeekLatency", kmonitor::GAUGE);
    METRIC_SETUP(mRtSgtSeekLatency, "RealTimeSegmentSeekLatency", kmonitor::GAUGE);
    METRIC_SETUP(mFullSgtSeekLatency, "FullSegmentSeekLatency", kmonitor::GAUGE);
    IE_LOG(INFO, "init sgt retrievers, sgt number[%lu], RtSgtNo[%d]", mSgtReaders.size(), mRtSgtNo);
    return true;
}

indexlib::index::Result<vector<SegmentMatchInfo>> AithetaIndexRetriever::Search(const Term &term,
                                                                                 mem_pool::Pool *pool) {
    if (unlikely(mSgtReaders.empty())) {
        IE_LOG(ERROR,
               "no segment reader for index[%s], table[%s]",
               mIndexerResource->indexName.c_str(),
               mIndexerResource->schema->GetSchemaName().c_str());
        return indexlib::index::ErrorCode::SegmentFile;
    }

    ScopedLatencyReporter reporter(mTotalSeekLatency);
    vector<Query> queries;
    const std::string &query = term.GetWord();
    if (!ParseQuery(query, queries) || queries.empty()) {
        IE_LOG(ERROR,
               "parse query[%s] failed in index[%s]",
               query.c_str(),
               mIndexerResource->indexName.c_str());
        return indexlib::index::ErrorCode::BadParameter;
    }

    SearchResult result(mSchemaParam.searchDistType);
    for (int32_t sgtNo = 0; sgtNo < (int32_t)mSgtReaders.size(); ++sgtNo) {
        bool status = true;
        if (sgtNo == 0) {
            ScopedLatencyReporter reporter(mFullSgtSeekLatency);
            status = mSgtReaders[sgtNo]->Search(queries, result);
        } else if (sgtNo == mRtSgtNo) {
            ScopedLatencyReporter reporter(mRtSgtSeekLatency);
            status = mSgtReaders[sgtNo]->Search(queries, result);
        } else {
            status = mSgtReaders[sgtNo]->Search(queries, result);
        }
        if (!status) {
            IE_LOG(ERROR, "failed to search in SgtNo[%d]", sgtNo);
        }
    }

    SegmentMatchInfo output;
    result.SelectTopk(queries.size() * queries[0].topk, output.matchInfo, pool);

    if (mRecallReporter && mRecallReporter->CanReport()) {
        mRecallReporter->Report(queries);
    }

    size_t resultCount = output.matchInfo ? output.matchInfo->matchCount : 0u;
    METRIC_REPORT(mTotalSeekCount, resultCount);
    return (vector<SegmentMatchInfo>){output};
}

bool AithetaIndexRetriever::InitRtSgtReader(const ContextHolderPtr &ctxHolder,
                                            docid_t rtSgtBaseDocId,
                                            const indexlib::index::DeletionMapReaderPtr &deletionMapRr) {
    auto rtSgtReader = DYNAMIC_POINTER_CAST(RtSegmentReader, mSgtReaders[mRtSgtNo]);
    if (!rtSgtReader) {
        IE_LOG(ERROR, "failed to dynamically cast to RtIndexSegmentReader");
        return false;
    }
    std::unordered_map<catid_t, IndexAttr> catid2MaxIndexAttr;
    for (size_t sgtNo = 0u; sgtNo < mSgtReaders.size(); ++sgtNo) {
        auto &holder = mSgtReaders[sgtNo]->GetOfflineIndexAttrHolder();
        for (size_t attrNo = 0u; attrNo < holder.Size(); ++attrNo) {
            auto &indexAttr = holder.Get(attrNo);
            auto iterator = catid2MaxIndexAttr.find(indexAttr.catid);
            if (iterator == catid2MaxIndexAttr.end()) {
                iterator = catid2MaxIndexAttr.emplace(indexAttr.catid, indexAttr).first;
                if (iterator->second.docNum < mSchemaParam.rtOption.docNumPredict) {
                    iterator->second.docNum = mSchemaParam.rtOption.docNumPredict;
                }
                float norm = mSchemaParam.rtOption.mipsNorm;
                if (iterator->second.reformer.norm() < norm) {
                    iterator->second.reformer = MipsReformer(norm);
                }
            } else {
                if (iterator->second.docNum < indexAttr.docNum) {
                    iterator->second.docNum = indexAttr.docNum;
                }
                if (iterator->second.reformer.norm() < indexAttr.reformer.norm()) {
                    iterator->second.reformer = indexAttr.reformer;
                }
            }
        }
    }
    rtSgtReader->SetOfflineMaxIndexAttrs(catid2MaxIndexAttr);
    return rtSgtReader->Init(ctxHolder, rtSgtBaseDocId, deletionMapRr);
}

// it is optimized, query is copied once and only once
bool AithetaIndexRetriever::ParseQuery(const string &src, vector<Query> &dst) {
    string qsStr = URLUtil::decode(src);

    bool enableSf = false;
    float score = 0.0f;
    {
        const auto &key = mSchemaParam.sfKey;
        size_t pos = qsStr.rfind(key);
        if (pos != string::npos) {
            string scoreStr = qsStr.substr(pos + key.size());
            size_t fpos = pos + key.size();
            size_t tpos = qsStr.size();
            if (StringParser::Parse(qsStr, fpos, tpos, score)) {
                enableSf = true;
            }
            qsStr.resize(pos);
        }
    }

    uint32_t topk = 100u;
    {
        const auto &key = mSchemaParam.topkKey;
        size_t pos = qsStr.rfind(key);
        if (pos != string::npos) {
            size_t fpos = pos + key.size();
            size_t tpos = qsStr.size();
            StringParser::Parse(qsStr, fpos, tpos, topk);
            qsStr.resize(pos);
        }
    }

    // query format: cat0,cat1#embedding0;cat0,cat1#embedding1
    for (; !qsStr.empty();) {
        vector<catid_t> catids;
        EmbeddingPtr emb;

        // find the first query from tail-to-front
        size_t qFront = 0u;
        {
            const auto &sep = mSchemaParam.querySeparator;
            size_t pos = qsStr.rfind(sep);
            if (pos != string::npos) {
                qFront = pos + sep.size();
            }
        }
        // parse the query
        {
            size_t embFront = qFront;

            // parse catid
            const auto &sep = mSchemaParam.catidEmbedSeparator;
            size_t pos = qsStr.find(sep, qFront);
            if (pos != string::npos) {
                size_t fpos = qFront;
                size_t tpos = pos;
                char catidSep = mSchemaParam.catidSeparator[0];
                if (!StringParser::Parse(qsStr, fpos, tpos, catidSep, catids)) {
                    IE_LOG(WARN, "failed to parse catid in [%s]", qsStr.c_str());
                    return false;
                }
                embFront = pos + sep.size();
            } else {
                catids.push_back(DEFAULT_CATEGORY_ID);
            }
            // parse embedding
            {
                size_t fpos = embFront;
                size_t tpos = qsStr.size();
                uint32_t dim = mSchemaParam.dimension;
                char embedSep = mSchemaParam.embedSeparator[0];
                if (!StringParser::Parse(qsStr, fpos, tpos, embedSep, dim, emb)) {
                    IE_LOG(WARN, "failed to parse embed in [%s]", qsStr.c_str());
                    return false;
                }
            }
        }

        for (const catid_t catid : catids) {
            dst.emplace_back(catid, topk, enableSf, score, mSchemaParam.dimension, emb);
        }

        if (qFront > 0u) {
            size_t sizeLeft = qFront - mSchemaParam.querySeparator.size();
            qsStr.resize(sizeLeft);
        } else {
            qsStr.clear();
        }
    }

    return true;
}

} // namespace aitheta_plugin
} // namespace indexlib
