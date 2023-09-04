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
#include "indexlib/index/ann/aitheta2/util/ResultHolder.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;
namespace indexlibv2::index::ann {

AUTIL_LOG_SETUP(indexlib.index, ResultHolder);

const std::vector<ANNMatchItem>& ResultHolder::GetTopkMatchItems(size_t topk)
{
    UniqAndOrderByDocId();
    OrderByScore();
    TruncateResult(topk);
    OrderByDocId();
    return _matchItems;
}

void ResultHolder::UniqAndOrderByDocId()
{
    if (_matchItems.empty()) {
        return;
    }
    OrderByDocId();

    size_t curCount = 0;
    float curScore = _matchItems[0].score;
    docid_t curDocId = _matchItems[0].docid;
    for (size_t i = 1; i < _matchItems.size(); ++i) {
        if (_matchItems[i].docid != curDocId) {
            _matchItems[curCount++] = {curDocId, curScore};
            std::tie(curDocId, curScore) = {_matchItems[i].docid, _matchItems[i].score};
        } else {
            if (_dropLargeScoreIfNeed) {
                curScore = std::min(curScore, _matchItems[i].score);
            } else {
                curScore = std::max(curScore, _matchItems[i].score);
            }
        }
    }
    _matchItems[curCount++] = {curDocId, curScore};
    _matchItems.resize(curCount);
}

bool ResultHolder::IsDropLargeScoreIfNeed(const std::string& distanceType) const
{
    if (distanceType == SQUARED_EUCLIDEAN) {
        return true;
    } else if (distanceType == INNER_PRODUCT || distanceType == MIPS_SQUARED_EUCLIDEAN) {
        return false;
    } else {
        assert(false);
        AUTIL_LOG(WARN, "no support for[%s]", distanceType.c_str());
        return false;
    }
}

} // namespace indexlibv2::index::ann
