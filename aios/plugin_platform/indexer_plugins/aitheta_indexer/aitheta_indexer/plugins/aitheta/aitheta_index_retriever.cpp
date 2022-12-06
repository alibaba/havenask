#include "aitheta_indexer/plugins/aitheta/aitheta_index_retriever.h"

#include <autil/URLUtil.h>
#include <autil/StringTokenizer.h>
#include <autil/StringUtil.h>
#include <autil/mem_pool/Pool.h>
#include <bitset>
#include <indexlib/file_system/directory.h>
#include <indexlib/storage/file_system_wrapper.h>
#include "aitheta_indexer/plugins/aitheta/common_define.h"
#include "aitheta_indexer/plugins/aitheta/util/custom_logger.h"

using namespace std;

IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_BEGIN(aitheta_plugin);

IE_LOG_SETUP(aitheta_plugin, AithetaIndexRetriever);

bool AithetaIndexRetriever::Init(const DeletionMapReaderPtr &deletionMapReader,
                                 const vector<IndexSegmentRetrieverPtr> &retrievers, const vector<docid_t> &baseDocIds,
                                 const IndexerResourcePtr &resource) {
    IndexRetriever::Init(deletionMapReader, retrievers, baseDocIds);

    RegisterGlobalIndexLogger();

    vector<SegmentReaderPtr> segReaderVector;
    vector<docid_t> segBaseDocIdVector;
    size_t index = 0;
    for (auto &retriever : retrievers) {
        auto segRetriever = DYNAMIC_POINTER_CAST(AithetaIndexSegmentRetriever, retriever);
        if (nullptr == segRetriever) {
            IE_LOG(ERROR, "cast to AithetaIndexSegmentRetriever failed");
            return false;
        }
        auto segReader = segRetriever->GetSegmentReader();
        switch (segReader->GetSegmentType()) {
            case SegmentType::kIndex: {
                mSegReader = segReader;
                mSegBaseDocId = baseDocIds[index];
                break;
            }
            case SegmentType::kRaw: {
                segReaderVector.push_back(segReader);
                segBaseDocIdVector.push_back(baseDocIds[index]);
                break;
            }
            case SegmentType::kMultiIndex: {
                mSegReader = segReader;
                mSegBaseDocId = baseDocIds[index];
                break;
            }
            default: {
                break;
            }
        }
        ++index;
    }

    if (!mSegReader) {
        IE_LOG(WARN, "not found index or parallel segment, but expected on current indexlib version");
        return true;
    }
    return mSegReader->UpdatePkey2DocId(mSegBaseDocId, segReaderVector, segBaseDocIdVector);
}

vector<SegmentMatchInfo> AithetaIndexRetriever::Search(const Term &term, autil::mem_pool::Pool *pool) {
    if (!mSegReader) {
        return vector<SegmentMatchInfo>();
    }

    int topK = 0;
    bool needScoreFilter = false;
    float score = 0.0;
    vector<QueryInfo> queryInfos;
    auto queryStr = const_cast<string &>(term.GetWord());
    string decodeQueryStr = autil::URLUtil::decode(queryStr);
    if (!ParseQuery(decodeQueryStr, queryInfos, topK, needScoreFilter, score) || 0 == topK) {
        return std::vector<SegmentMatchInfo>();
    }

    std::vector<MatchInfo> matchInfos;
    std::vector<SegmentMatchInfo> segMatchInfos;
    matchInfos = mSegReader->Search(queryInfos, pool, topK, needScoreFilter, score);

    SegmentMatchInfo segMatchInfo;
    segMatchInfo.baseDocId = mSegBaseDocId;
    for (size_t i = 0; i < matchInfos.size(); ++i) {
        segMatchInfo.matchInfo.reset(new MatchInfo(move(matchInfos[i])));
        segMatchInfos.push_back(segMatchInfo);
    }

    return segMatchInfos;
}

#define CHECK_VALUE(ch) \
    ((ch >= '0' && ch <= '9') || ch == '.' || ch == '+' || ch == '-' || ch == 'e' || ch == 'E' || ch == ' ')

bool AithetaIndexRetriever::ParseQuery(string &query, vector<QueryInfo> &parseResults, int &topK, bool &needScoreFilter,
                                       float &score) {
    const auto &kTopKFlag = mComParam.mTopKFlag;
    const auto &kScoreFilterFlag = mComParam.mScoreFilterFlag;
    const auto &kDim = mComParam.mDim;
    const auto &kQuerySep = mComParam.mQuerySeparator;
    const auto &kCategorySep = mComParam.mCategorySeparator;
    const auto &kEmbeddingSep = mComParam.mEmbeddingSeparator;
    const auto &kCategoryEmbeddingSep = mComParam.mCategoryEmbeddingSeparator;

    // parse score
    score = 0.0;
    size_t pos = query.rfind(kScoreFilterFlag);
    if (pos != std::string::npos) {
        needScoreFilter = true;
        const char *start = query.data() + pos + kScoreFilterFlag.size();
        autil::StringUtil::strToFloat(start, score);
        query.resize(pos);
    } else {
        needScoreFilter = false;
    }
    // parse topk
    topK = 0;
    pos = query.rfind(kTopKFlag);
    if (pos != std::string::npos) {
        const char *start = query.data() + pos + kTopKFlag.size();
        autil::StringUtil::strToInt32(start, topK);
        query.resize(pos);
    } else {
        IE_LOG(WARN, "find topk with flag[%s] failed", kTopKFlag.data());
        return false;
    }

    vector<QueryInfo> queryInfos;
    // parse from tail to head, ignore extension part
    for (int start = pos - 1, end = pos; start >= 0;) {
        EmbeddingPtr embedding;
        SHARED_PTR_MALLOC(embedding, kDim, float);
        int embeddingPos = kDim;
        // parse embedding
        while (true) {
            // get the pos of a legal embedding: [start+1, end-1]
            while (start >= 0 && CHECK_VALUE(query[start])) {
                --start;
            }
            char backUp = query[end];
            query[end] = '\0';
            if (--embeddingPos < 0) {
                IE_LOG(WARN, "dimension is larger than expected[%d]", kDim);
                return false;
            }
            autil::StringUtil::strToFloat(query.data() + start + 1, embedding.get()[embeddingPos]);
            query[end] = backUp;

            if (start < 0 || kQuerySep == query[start] || kCategoryEmbeddingSep == query[start]) {
                if (embeddingPos != 0) {
                    IE_LOG(WARN, "dimension is smaller than expected[%d]", kDim);
                    return false;
                }
                break;
            } else if (kEmbeddingSep == query[start]) {
                // skip embedding separator
                end = start--;
            } else {
                IE_LOG(WARN, "illegal char[%c] in pos[%d] of query[%s]", query[start], start, query.data());
                return false;
            }
        }
        vector<int64_t> catIds;
        // parse category id
        if (start < 0 || kQuerySep == query[start]) {
            // set a default category id
            catIds.push_back(DEFAULT_CATEGORY_ID);
        } else if (kCategoryEmbeddingSep == query[start]) {
            // skip category embedding separator
            end = start--;
            while (true) {
                while (start >= 0 && CHECK_VALUE(query[start])) {
                    --start;
                }
                char backUp = query[end];
                query[end] = '\0';

                int64_t catId = 0;
                autil::StringUtil::strToInt64(query.data() + start + 1, catId);
                catIds.push_back(catId);
                query[end] = backUp;
                if (start < 0 || kQuerySep == query[start]) {
                    break;
                } else if (kCategorySep == query[start]) {
                    // skip category separator
                    end = start--;
                } else {
                    IE_LOG(WARN, "illegal char[%c] in pos[%d] of query[%s]", query[start], start, query.data());
                    return false;
                }
            }
        }
        // skip query separator
        end = start--;
        for (auto catId : catIds) {
            queryInfos.emplace_back(catId, embedding, mComParam.mDim);
        }
    }
    parseResults.swap(queryInfos);
    return true;
}

#undef CHECK_VALUE

IE_NAMESPACE_END(aitheta_plugin);
