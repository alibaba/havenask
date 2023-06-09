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
#pragma once

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include <memory>
#include <string>
#include <memory>
#include <vector>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/inverted_index/InDocPositionIterator.h"
#include "indexlib/index/inverted_index/TermMatchData.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/Reference.h"
#include "suez/turing/expression/plugin/Scorer.h"
#include "suez/turing/expression/util/FieldBoostTable.h"

#include "ha3/common/Tracer.h"
#include "ha3/config/ResourceReader.h"
#include "ha3/isearch.h"
#include "ha3/search/MetaInfo.h"
#include "ha3/search/MatchData.h"
#include "ha3/rank/ScoringProvider.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace suez {
namespace turing {
class ScoringProvider;
}  // namespace turing
}  // namespace suez


namespace isearch {
namespace rank {
#define FIRST_OCC_TABLE "firstocc"
#define NUM_OCC_TABLE "numocc"
#define PROXIMITY_TABLE "proximity"
#define TABLE_VALUE_SEPERATOR ","
#define STATIC_SCORE_FIELD "static_field"
#define STATIC_SCORE_WEIGHT "static_weight"
#define SCORE_PHASE_NUM "phase_num"
#define SCORE_TOTAL_FREQ "total_freq"
#define PHASE_ONE_SCORE "phase_one_score"
#define LEFT_VALUE_SEPERATOR "{"
#define RIGHT_VALUE_SEPERATOR "}"
#define RESOURCE_FILE_NAME "table_value_file"

class GeneralScorer : public suez::turing::Scorer
{
public:
    GeneralScorer(const KeyValueMap &scorerParameters,
                  config::ResourceReader *resourceReader,
                  const std::string &name = "GeneralScorer");
     GeneralScorer(const GeneralScorer &other);
    ~GeneralScorer();
public:

    //For each request, we will call clone to get a new scorer
    suez::turing::Scorer *clone() override;

    //called before each request
    bool beginRequest(suez::turing::ScoringProvider *provider) override;

    //called for each matched doc
    score_t score(matchdoc::MatchDoc &matchDoc) override;

    //called after each request
    void endRequest() override;

    //call to recycle/delete this scorer
    void destroy() override;
private:
    typedef std::vector<float> FloatVector;
    typedef std::shared_ptr<FloatVector> FloatVecPtr;

private:
    score_t computeTermScore(matchdoc::MatchDoc matchDoc, float &totalDocFreq, int32_t &matchNum);
    int32_t getMatchedDocmentProximity();
    inline score_t getFieldBoost(indexid_t indexId, fieldbitmap_t fieldBitMap);
    bool extractIndexId();

    void initTables (const KeyValueMap &scorerParameters,
                     config::ResourceReader *resourceReader);
    void constructTables(const std::string &content, const std::string &tableName,
                         FloatVecPtr &floatVecPtr, const float *defaultValue);
    bool getContentFromResourceFile(const std::string &fileName,
                                    const KeyValueMap &scorerParameters,
                                    std::string &content,
                                    config::ResourceReader *resourceReader);
    bool extractStrValue(const std::string &content,
                         const std::string &name,
                         FloatVecPtr &floatVecPtr);

    bool string2Table(FloatVecPtr &floatVecPtr,
                      const std::string &name,const KeyValueMap &scorerParameters,
                      config::ResourceReader *resourceReader);
    uint32_t MatchedDocmuentProximityExtractor(matchdoc::MatchDoc matchDoc);
    inline uint32_t getMinDist(std::vector<pos_t> &vctPos, pos_t curPos);
    uint32_t MatchedFieldProximityExtractor(int32_t curFieldId,matchdoc::MatchDoc matchDoc);
    inline float getStaticScore(matchdoc::MatchDoc matchDoc);
    void initScoreField(const KeyValueMap &scorerParameters);
    void initPhaseInfo(const KeyValueMap &scorerParameters);
    bool declarePhaseRef();
    score_t scorePhaseOne(matchdoc::MatchDoc matchDoc, float &totalDocFreq);
    score_t scorePhaseTwo(matchdoc::MatchDoc matchDoc, float totalDocFreq);
    pos_t safeSeekPosition(const std::shared_ptr<indexlib::index::InDocPositionIterator>& posIter, pos_t pos);

private:
    TRACE_DECLARE();

    const matchdoc::Reference<MatchData> *_matchDataRef;
    rank::ScoringProvider *_provider;
    indexid_t *_indexId;

    const matchdoc::Reference<int32_t> *_staticFieldRef;
    const matchdoc::Reference<float> *_totalFreqRef;
    const matchdoc::Reference<score_t> *_phaseOneScoreRef;

    FloatVecPtr _numOccTablePtr;
    FloatVecPtr _firstOccTablePtr;
    FloatVecPtr _proximityTablePtr;

    std::string _staticField;
    float _staticWeight;

    rank::MetaInfo _metaInfo;
    int32_t _phase;

private:
    friend class GeneralScorerTest;

private:
    const static float defaultNumOccTable[256];
    const static float defaultFirstOccTable[256];
    const static float defaultProximityTable[256];
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<GeneralScorer> GeneralScorerPtr;

inline score_t GeneralScorer::getFieldBoost(indexid_t indexId, fieldbitmap_t fieldMap)
{
    const suez::turing::FieldBoostTable *fieldBoostTable = _provider->getFieldBoostTable();
    if (NULL == fieldBoostTable) {
        AUTIL_LOG(ERROR, "fieldBoostTable is NULL.");
        return 0.0;
    }
    return fieldBoostTable->getFieldBoostSum(indexId,fieldMap);
}

inline float GeneralScorer::getStaticScore(matchdoc::MatchDoc matchDoc) {
    int32_t staticScore = 0;
    if (_staticFieldRef && matchdoc::INVALID_MATCHDOC != matchDoc) {
        staticScore = _staticFieldRef->get(matchDoc);
    }
    RANK_TRACE(TRACE1, matchDoc, "docid = [%d], static score = [%d], static weight = [%f]",
               matchDoc.getDocId(), staticScore, _staticWeight);
    return ((float)staticScore * _staticWeight);
}

inline uint32_t GeneralScorer::getMinDist(std::vector<pos_t> &vctPos, pos_t curPos) {
    sort(vctPos.begin(),vctPos.end());
    std::vector<pos_t>::const_iterator highIter = upper_bound(vctPos.begin(),vctPos.end(),curPos);
    if (highIter == vctPos.end()) {
        return (curPos - vctPos[vctPos.size() - 1]);
    } else {
        if (highIter == vctPos.begin()) {
            return (*highIter - curPos);
        } else {
            int32_t forwardDist = curPos - *(highIter - 1);
            int32_t backwardDist = *highIter - curPos;
            assert(forwardDist != 0 && backwardDist != 0);
            return (backwardDist < forwardDist) ? backwardDist : forwardDist;
        }
    }
}

inline pos_t GeneralScorer::safeSeekPosition(const std::shared_ptr<indexlib::index::InDocPositionIterator>& posIter, pos_t pos) {
    pos_t nextPos = INVALID_POSITION;
    auto ec = posIter->SeekPositionWithErrorCode(pos, nextPos);
    if (unlikely(ec != indexlib::index::ErrorCode::OK)) {
        return INVALID_POSITION;
    }
    return nextPos;
}

} // namespace rank
} // namespace isearch
