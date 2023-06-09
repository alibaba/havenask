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
#include "indexlib_plugin/plugins/aitheta/util/search_util.h"

using namespace std;
using namespace aitheta;
using namespace autil;
using namespace indexlib::util;
namespace indexlib {
namespace aitheta_plugin {

ContextHolder::~ContextHolder() {
    ScopedWriteLock wrlock(mLock);
    for (auto &outerKv : mContext) {
        auto &signatureContext = outerKv.second;
        for (auto &innerKv : signatureContext) {
            auto &context = innerKv.second;
            delete context;
            context = nullptr;
        }
    }
}

bool ContextHolder::Get(int64_t signature, IndexSearcher::Context *&cxt) {
    pthread_t pid = pthread_self();
    ScopedReadLock rdlock(mLock);
    auto pidIter = mContext.find(pid);
    if (pidIter != mContext.end()) {
        auto &sigctx = pidIter->second;
        auto ctxIter = sigctx.find(signature);
        if (ctxIter != sigctx.end()) {
            cxt = ctxIter->second;
            return true;
        }
    }
    return false;
}

bool ContextHolder::Add(int64_t signature, aitheta::IndexSearcher::Context *cxt) {
    pthread_t pid = pthread_self();
    ScopedWriteLock wrlock(mLock);
    if (nullptr == cxt) {
        return false;
    }
    mContext[pid][signature] = cxt;
    return true;
}

IE_LOG_SETUP(aitheta_plugin, SearchResult);

const uint32_t SearchResult::kMaxRange = UINT_MAX;

SearchResult::SearchResult(const DistType &ty) : mIsL2(ty == DistType::kL2), mSize(0u), mIsSub(false) {
    mRange = std::make_pair(0u, kMaxRange);
    mDocs.reset(new vector<Doc>);
    mLock.reset(new ReadWriteLock);
}

bool SearchResult::Alloc(uint32_t num, SearchResult &result) {
    ScopedWriteLock wrLock(*mLock);
    if (num > mRange.second - mSize) {
        IE_LOG(ERROR, "failed to alloc: num[%u], range_end[%u], mSize[%u]", num, mRange.second, mSize);
        return false;
    }
    uint32_t size = mDocs->size();
    mDocs->resize(size + num, std::make_pair(INVALID_DOCID, 0.0f));
    mSize += num;
    result.mDocs = mDocs;
    result.mLock = mLock;
    result.mSize = 0u;
    result.mIsSub = true;
    result.mIsL2 = mIsL2;
    result.mRange = std::make_pair(size, size + num);
    return true;
}

bool SearchResult::Add(docid_t docid, float score) {
    ScopedWriteLock wrLock(*mLock);
    return AddUnsafe(docid, score);
}

bool SearchResult::AddUnsafe(docid_t docid, float score) {
    if (mSize >= mRange.second - mRange.first) {
        IE_LOG(ERROR, "failed to add: range[%u, %u), mSize[%u]", mRange.first, mRange.second, mSize);
        return false;
    }
    if (mIsSub) {
        (*mDocs)[mSize + mRange.first] = std::make_pair(docid, score);
    } else {
        mDocs->emplace_back(docid, score);
    }
    ++mSize;
    return true;
}

bool SearchResult::SelectTopk(uint32_t retNum, MatchInfoPtr &matchInfo, autil::mem_pool::Pool *pool) {
    {
        ScopedWriteLock wrLock(*mLock);

        if (mIsSub) {
            IE_LOG(ERROR, "do NOT support select from sub search result");
            return false;
        }

        SortDocId(Range(0, mSize));
        uint32_t num = 0u;
        auto &docs = *mDocs;
        for (size_t idx = 0u; idx < docs.size(); ++idx) {
            if (INVALID_DOCID != docs[idx].first) {
                if (0u == idx || docs[idx - 1].first != docs[idx].first) {
                    docs[num++] = docs[idx];
                }
            }
        }
        if (num > retNum) {
            SortScore(Range(0, num));
            SortDocId(Range(0, retNum));
        } else {
            retNum = num;
        }
    }
    Format(Range(0, retNum), matchInfo, pool);

    return true;
}

void SearchResult::Format(const Range &range, MatchInfoPtr &matchInfo, autil::mem_pool::Pool *pool) {
    matchInfo.reset(new MatchInfo);
    ScopedReadLock rdLock(*mLock);
    size_t count = range.second - range.first;
    matchInfo->docIds = IE_POOL_COMPATIBLE_NEW_VECTOR(pool, docid_t, count);
    if (nullptr == matchInfo->docIds) {
        IE_LOG(ERROR, "failed to alloc memory, size[%lu]", count * sizeof(docid_t));
        return;
    }

    matchInfo->matchValues = IE_POOL_COMPATIBLE_NEW_VECTOR(pool, matchvalue_t, count);
    if (nullptr == matchInfo->matchValues) {
        IE_LOG(ERROR, "failed to alloc memory, size[%lu]", count * sizeof(matchvalue_t));
        return;
    }
    auto &docs = *mDocs;
    for (size_t idx = range.first, docNo = 0u; idx < range.second; ++idx, ++docNo) {
        matchInfo->docIds[docNo] = docs[idx].first;
        matchInfo->matchValues[docNo].SetFloat(docs[idx].second);
    }
    matchInfo->matchCount = count;
    matchInfo->pool = pool;
}

void SearchResult::Sort(const Range &range, bool sortDocId) {
    typedef std::pair<docid_t, float> Node;
    auto cmpFunc = [sortDocId, this](const Node &lhs, const Node &rhs) {
        if (sortDocId) {
            if (lhs.first != rhs.first) {
                return (lhs.first < rhs.first);
            }
        }
        return this->mIsL2 ? (lhs.second < rhs.second) : (lhs.second > rhs.second);
    };
    sort(mDocs->begin() + range.first, mDocs->begin() + range.second, cmpFunc);
}

}
}
