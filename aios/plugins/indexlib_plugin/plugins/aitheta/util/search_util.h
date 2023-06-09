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
#ifndef INDEXLIB_PLUGIN_PLUGINS_UTIL_SEARCH_UTIL_H
#define INDEXLIB_PLUGIN_PLUGINS_UTIL_SEARCH_UTIL_H

#include <climits>
#include <vector>
#include <unordered_map>
#include "autil/Lock.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib_plugin/plugins/aitheta/common_define.h"
#include "indexlib_plugin/plugins/aitheta/util/metric_reporter.h"

namespace indexlib {
namespace aitheta_plugin {

class ContextHolder {
 public:
    ContextHolder() = default;
    ~ContextHolder();
    ContextHolder(const ContextHolder &) = delete;
    ContextHolder &operator=(const ContextHolder &) = delete;

 public:
    bool Get(int64_t signature, aitheta::IndexSearcher::Context *&ret);
    bool Add(int64_t signature, aitheta::IndexSearcher::Context *ret);

 private:
    autil::ReadWriteLock mLock;
    typedef std::unordered_map<int64_t, aitheta::IndexSearcher::Context *> SignatureContext;
    std::unordered_map<pthread_t, SignatureContext> mContext;
};
DEFINE_SHARED_PTR(ContextHolder);

class SearchResult {
 public:
    typedef std::pair<docid_t, float> Doc;
    typedef std::pair<uint32_t, uint32_t> Range;

 public:
    SearchResult(const DistType &ty = DistType::kIP);
    ~SearchResult() = default;
    SearchResult(const SearchResult &) = delete;
    SearchResult &operator=(const SearchResult &) = delete;

 public:
    uint32_t Size() const { return mSize; }
    bool Alloc(uint32_t num, SearchResult &result);
    bool Add(docid_t docid, float score);
    bool AddUnsafe(docid_t docid, float score);
    bool SelectTopk(uint32_t retNum, MatchInfoPtr &matchInfo, autil::mem_pool::Pool *pool);

 public:
    const std::shared_ptr<std::vector<Doc>> &GetDocs() const { return mDocs; }

 private:
    void SortDocId(const Range &r) { Sort(r, true); }
    void SortScore(const Range &r) { Sort(r, false); }
    void Sort(const Range &r, bool sortDocId);
    void Format(const Range &r, MatchInfoPtr &matchInfo, autil::mem_pool::Pool *pool);

 private:
    const static uint32_t kMaxRange;
    bool mIsL2;
    uint32_t mSize;
    bool mIsSub;
    // range : [front, end)
    Range mRange;
    std::shared_ptr<std::vector<Doc>> mDocs;
    std::shared_ptr<autil::ReadWriteLock> mLock;
    IE_LOG_DECLARE();
};

}
}

#endif  // INDEXLIB_PLUGIN_PLUGINS_UTIL_SEARCH_UTIL_H
