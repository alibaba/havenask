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
#include "ha3/rank/GeneralScorer.h"

#include <math.h>
#include <algorithm>
#include <cstdint>
#include <map>
#include <new>
#include <utility>

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "matchdoc/MatchDoc.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/plugin/Scorer.h"
#include "suez/turing/expression/provider/ScoringProvider.h"

#include "ha3/common/Tracer.h"
#include "ha3/config/IndexInfoHelper.h"
#include "ha3/config/ResourceReader.h"
#include "ha3/isearch.h"
#include "ha3/search/MatchData.h"
#include "ha3/rank/ScoringProvider.h"
#include "ha3/search/TermMatchData.h"
#include "ha3/search/TermMetaInfo.h"
#include "autil/Log.h"

using namespace suez::turing;

namespace isearch {
namespace rank {
AUTIL_LOG_SETUP(ha3, GeneralScorer);

const float GeneralScorer::defaultNumOccTable[256] = {1.39996,1.79984,2.19964,2.59936,2.999,3.39856,3.79804,4.19744,4.59676,4.99601,5.39517,5.79425,6.19325,6.59217,6.99102,7.38978,7.78847,8.18707,8.5856,8.98404,9.38241,9.7807,10.1789,10.577,10.9751,11.3731,11.7709,12.1688,12.5665,12.9641,13.3617,13.7592,14.1566,14.554,14.9512,15.3484,15.7455,16.1425,16.5395,16.9363,17.3331,17.7298,18.1265,18.523,18.9195,19.3159,19.7122,20.1084,20.5046,20.9007,21.2967,21.6926,22.0884,22.4842,22.8799,23.2755,23.671,24.0665,24.4618,24.8571,25.2524,25.6475,26.0426,26.4375,26.8325,27.2273,27.622,28.0167,28.4113,28.8058,29.2002,29.5946,29.9889,30.3831,30.7772,31.1713,31.5652,31.9591,32.353,32.7467,33.1404,33.5339,33.9275,34.3209,34.7142,35.1075,35.5007,35.8938,36.2869,36.6798,37.0727,37.4655,37.8583,38.2509,38.6435,39.036,39.4284,39.8208,40.2131,40.6053,40.9974,41.3894,41.7814,42.1733,42.5651,42.9568,43.3485,43.7401,44.1316,44.523,44.9143,45.3056,45.6968,46.0879,46.479,46.8699,47.2608,47.6516,48.0424,48.4331,48.8236,49.2141,49.6046,49.9949,50.3852,50.7754,51.1656,51.5556,51.9456,52.3355,52.7253,53.1151,53.5047,53.8943,54.2839,54.6733,55.0627,55.452,55.8412,56.2303,56.6194,57.0084,57.3973,57.7861,58.1749,58.5636,58.9522,59.3408,59.7292,60.1176,60.5059,60.8942,61.2823,61.6704,62.0584,62.4463,62.8342,63.222,63.6097,63.9973,64.3849,64.7724,65.1598,65.5471,65.9344,66.3216,66.7087,67.0957,67.4827,67.8696,68.2564,68.6431,69.0298,69.4163,69.8029,70.1893,70.5757,70.9619,71.3482,71.7343,72.1204,72.5063,72.8923,73.2781,73.6639,74.0496,74.4352,74.8207,75.2062,75.5916,75.9769,76.3621,76.7473,77.1324,77.5174,77.9024,78.2872,78.6721,79.0568,79.4414,79.826,80.2105,80.5949,80.9793,81.3636,81.7478,82.1319,82.516,82.9,83.2839,83.6677,84.0515,84.4352,84.8188,85.2024,85.5858,85.9692,86.3525,86.7358,87.119,87.5021,87.8851,88.2681,88.651,89.0338,89.4165,89.7992,90.1818,90.5643,90.9467,91.3291,91.7114,92.0936,92.4758,92.8579,93.2399,93.6218,94.0037,94.3855,94.7672,95.1488,95.5304,95.9119,96.2933,96.6747,97.0559,97.4371,97.8183,98.1993,98.5803,98.9612,99.3421,99.7229,100.104,100.484,100.865};
const float GeneralScorer::defaultFirstOccTable[256] = {6658.61,6333.86,6024.96,5731.12,5451.61,5185.73,4932.82,4692.24,4463.4,4245.71,4038.65,3841.68,3654.32,3476.1,3306.57,3145.3,2991.9,2845.99,2707.19,2575.16,2449.56,2330.1,2216.46,2108.36,2005.53,1907.72,1814.68,1726.18,1641.99,1561.91,1485.74,1413.28,1344.35,1278.78,1216.42,1157.09,1100.66,1046.98,995.919,947.347,901.144,857.195,815.389,775.622,737.795,701.812,667.584,635.026,604.055,574.595,546.572,519.915,494.559,470.439,447.495,425.67,404.91,385.163,366.378,348.509,331.512,315.344,299.965,285.335,271.419,258.182,245.59,233.613,222.219,211.382,201.072,191.266,181.938,173.065,164.624,156.595,148.958,141.693,134.783,128.209,121.957,116.009,110.351,104.969,99.8496,94.9799,90.3477,85.9414,81.75,77.763,73.9704,70.3629,66.9312,63.6669,60.5619,57.6082,54.7986,52.1261,49.5839,47.1656,44.8653,42.6772,40.5958,38.616,36.7326,34.9412,33.2371,31.6161,30.0741,28.6074,27.2122,25.885,24.6226,23.4218,22.2795,21.1929,20.1593,19.1761,18.2409,17.3513,16.505,15.7001,14.9344,14.206,13.5132,12.8541,12.2272,11.6309,11.0637,10.5241,10.0108,9.52258,9.05816,8.61638,8.19616,7.79643,7.41619,7.0545,6.71045,6.38317,6.07186,5.77573,5.49405,5.2261,4.97122,4.72877,4.49815,4.27877,4.07009,3.87159,3.68277,3.50316,3.33231,3.16979,3.0152,2.86814,2.72826,2.5952,2.46864,2.34824,2.23371,2.12477,2.02115,1.92257,1.82881,1.73962,1.65478,1.57407,1.4973,1.42428,1.35482,1.28874,1.22589,1.1661,1.10923,1.05513,1.00367,0.954722,0.90816,0.863869,0.821737,0.781661,0.743539,0.707276,0.672782,0.63997,0.608758,0.579068,0.550827,0.523963,0.498409,0.474101,0.450979,0.428984,0.408063,0.388161,0.36923,0.351223,0.334093,0.3178,0.3023,0.287557,0.273533,0.260192,0.247503,0.235432,0.22395,0.213027,0.202638,0.192755,0.183354,0.174412,0.165906,0.157815,0.150118,0.142797,0.135832,0.129208,0.122906,0.116912,0.11121,0.105786,0.100627,0.0957194,0.0910511,0.0866105,0.0823864,0.0783684,0.0745463,0.0709107,0.0674523,0.0641626,0.0610334,0.0580567,0.0552253,0.0525319,0.0499699,0.0475328,0.0452146,0.0430095,0.0409119,0.0389166,0.0370186,0.0352132,0.0334958,0.0318622,0.0303083,0.0288301,0.0274241,0.0260866,0.0248143,0.0236041,0.0224529,0.0213579,0.0203162,0.0193254};
const float GeneralScorer::defaultProximityTable[256] = {346.751,300.591,260.576,225.887,195.817,169.749,147.152,127.563,110.581,95.8604,83.0993,72.0369,62.4472,54.1341,46.9277,40.6806,35.2651,30.5705,26.5009,22.973,19.9148,17.2637,14.9655,12.9733,11.2463,9.74914,8.45131,7.32626,6.35097,5.50551,4.77261,4.13727,3.58651,3.10906,2.69518,2.33639,2.02537,1.75574,1.52202,1.3194,1.14376,0.991501,0.85951,0.74509,0.645902,0.559919,0.485381,0.420766,0.364753,0.316196,0.274103,0.237614,0.205983,0.178562,0.154791,0.134185,0.116322,0.100837,0.0874134,0.0757767,0.0656892,0.0569445,0.0493639,0.0427925,0.0370959,0.0321576,0.0278767,0.0241657,0.0209487,0.01816,0.0157425,0.0136468,0.0118301,0.0102553,0.00889006,0.0077066,0.00668068,0.00579133,0.00502038,0.00435206,0.0037727,0.00327047,0.0028351,0.00245768,0.00213051,0.00184689,0.00160103,0.0013879,0.00120314,0.00104297,0.000904132,0.000783772,0.000679435,0.000588987,0.00051058,0.00044261,0.000383689,0.000332611,0.000288334,0.00024995,0.000216676,0.000187832,0.000162827,0.000141151,0.000122361,0.000106072,9.19515e-005,7.97107e-005,6.90994e-005,5.99008e-005,5.19267e-005,4.50141e-005,3.90217e-005,3.38271e-005,2.93239e-005,2.54203e-005,2.20363e-005,1.91027e-005,1.65598e-005,1.43553e-005,1.24443e-005,1.07877e-005,9.35159e-006,8.10669e-006,7.02751e-006,6.09199e-006,5.28101e-006,4.57799e-006,3.96856e-006,3.44026e-006,2.98228e-006,2.58528e-006,2.24112e-006,1.94278e-006,1.68415e-006,1.45995e-006,1.2656e-006,1.09712e-006,9.5107e-007,8.24461e-007,7.14707e-007,6.19564e-007,5.37086e-007,4.65588e-007,4.03608e-007,3.49879e-007,3.03302e-007,2.62926e-007,2.27925e-007,1.97583e-007,1.7128e-007,1.48479e-007,1.28713e-007,1.11579e-007,9.67251e-008,8.38489e-008,7.26867e-008,6.30105e-008,5.46224e-008,4.7351e-008,4.10475e-008,3.55832e-008,3.08463e-008,2.674e-008,2.31803e-008,2.00945e-008,1.74195e-008,1.51005e-008,1.30903e-008,1.13477e-008,9.83708e-009,8.52755e-009,7.39234e-009,6.40826e-009,5.55518e-009,4.81566e-009,4.17459e-009,3.61886e-009,3.13711e-009,2.71949e-009,2.35747e-009,2.04364e-009,1.77158e-009,1.53575e-009,1.3313e-009,1.15408e-009,1.00044e-009,8.67263e-010,7.51812e-010,6.51729e-010,5.64969e-010,4.89759e-010,4.24562e-010,3.68043e-010,3.19048e-010,2.76576e-010,2.39758e-010,2.07841e-010,1.80172e-010,1.56187e-010,1.35395e-010,1.17371e-010,1.01747e-010,8.82019e-011,7.64603e-011,6.62817e-011,5.74582e-011,4.98092e-011,4.31785e-011,3.74305e-011,3.24477e-011,2.81282e-011,2.43837e-011,2.11377e-011,1.83238e-011,1.58845e-011,1.37699e-011,1.19368e-011,1.03478e-011,8.97026e-012,7.77612e-012,6.74094e-012,5.84358e-012,5.06567e-012,4.39131e-012,3.80673e-012,3.29997e-012,2.86067e-012,2.47985e-012,2.14973e-012,1.86355e-012,1.61547e-012,1.40042e-012,1.21399e-012,1.05238e-012,9.12287e-013,7.90842e-013,6.85563e-013,5.943e-013,5.15185e-013,4.46603e-013,3.8715e-013,3.35612e-013,2.90934e-013,2.52205e-013,2.18631e-013,1.89526e-013,1.64296e-013,1.42425e-013,1.23465e-013,1.07029e-013,9.27809e-014,8.04297e-014,6.97228e-014,6.04411e-014,5.23951e-014};
GeneralScorer::GeneralScorer(const KeyValueMap &scorerParameters,
                             config::ResourceReader *resourceReader,
                             const std::string &name ) : Scorer(name) {
    _matchDataRef = NULL;
    _provider = NULL;
    _indexId = NULL;
    _staticFieldRef = NULL;
    _totalFreqRef = NULL;
    _phaseOneScoreRef = NULL;
    _phase = 1;

    _firstOccTablePtr = FloatVecPtr(new FloatVector);
    _numOccTablePtr = FloatVecPtr(new FloatVector);
    _proximityTablePtr = FloatVecPtr(new FloatVector);

    _firstOccTablePtr->reserve(256);
    _numOccTablePtr->reserve(256);
    _proximityTablePtr->reserve(256);
    initTables(scorerParameters, resourceReader);
    initScoreField(scorerParameters);
    initPhaseInfo(scorerParameters);
}

GeneralScorer::GeneralScorer(const GeneralScorer &other)
{
    _matchDataRef = NULL;
    _provider = NULL;
    _indexId = NULL;
    _staticFieldRef = NULL;
    _totalFreqRef = NULL;
    _phaseOneScoreRef = NULL;
    _phase = other._phase;

    _firstOccTablePtr = other._firstOccTablePtr;
    _numOccTablePtr = other._numOccTablePtr;
    _proximityTablePtr = other._proximityTablePtr;

    _staticField = other._staticField;
    _staticWeight = other._staticWeight;

}
GeneralScorer::~GeneralScorer() {
    if (NULL != _indexId) {
        delete [] _indexId;
    }
}

//For each request, we will call clone to get a new scorer
Scorer* GeneralScorer::clone()
{
    return new GeneralScorer(*this);
}

//called before each reques
bool GeneralScorer::beginRequest(suez::turing::ScoringProvider *provider)
{
    _provider = dynamic_cast<rank::ScoringProvider*>(provider);
    if (NULL == _provider) {
        AUTIL_LOG(WARN,"provider pointer is NULL");
        return false;
    }
    TRACE_SETUP(provider);

    _matchDataRef = _provider->requireMatchData();
    _provider->getQueryTermMetaInfo(&_metaInfo);

    if (!_staticField.empty()) {
        _staticFieldRef = _provider->requireAttribute<int32_t>(_staticField.c_str());
        if (NULL == _staticFieldRef) {
            AUTIL_LOG(WARN,"Failed to get static score field:[%s]",_staticField.c_str());
            return false;
        }
    }
    return ( _matchDataRef != NULL && extractIndexId() && declarePhaseRef());
}

//called for each matched doc

score_t GeneralScorer::score(matchdoc::MatchDoc &matchDoc)
{
    if (matchdoc::INVALID_MATCHDOC == matchDoc) {
        return 0.0;
    }
    float totalDocFreq = 1;
    score_t score = 0.0;
    if (1 == _phase) {
        score = scorePhaseOne(matchDoc, totalDocFreq);
    } else if (2 == _phase) {
        score = scorePhaseTwo(matchDoc, totalDocFreq);
    } else {
        score = scorePhaseOne(matchDoc, totalDocFreq);
        score += scorePhaseTwo(matchDoc, totalDocFreq);
    }
    return score ;
}

//called after each request
void GeneralScorer::endRequest()
{
    if (NULL != _indexId) {
        delete [] _indexId;
        _indexId = NULL;
    }
}

//call to recycle/delete this scorer
void GeneralScorer::destroy()
{
    delete this;
}
score_t GeneralScorer::computeTermScore(matchdoc::MatchDoc matchDoc, float &totalDocFreq, int32_t &matchNum)
{
    const MatchData &data = _matchDataRef->getReference(matchDoc);
    static float log2 = log(2.0);
    score_t score = 0.0;
    uint32_t termsCount = data.getNumTerms();

    if (_metaInfo.size() < termsCount) {
        AUTIL_LOG(WARN,"_metaInfo.size is a wrong value");
        return 0.0;
    }
    for (uint32_t i = 0; i < termsCount; ++i) {
        const TermMatchData &tmd = data.getTermMatchData(i);
        if (tmd.isMatched()) {
            int32_t tf = tmd.getTermFreq();
            tf -= 1;
            RANK_TRACE(TRACE3, matchDoc,
                       "docid = [%d], Termi=[%u], tf=[%d] ", matchDoc.getDocId(), i, tf );
            tf = tf > 255 ? 255 : tf;
            float tfValue = _numOccTablePtr->at(tf);

            int32_t fOcc = tmd.getFirstOcc();
            RANK_TRACE(TRACE3, matchDoc,
                       "docid = [%d], Termi=[%u], firstOcc=[%d] ", matchDoc.getDocId(), i, fOcc );
            fOcc = fOcc > 255 ? 255 : fOcc;
            float fOccValue = _firstOccTablePtr->at(fOcc);

            fieldbitmap_t fieldMap = tmd.getFieldBitmap();
            score_t fieldBoostValue = getFieldBoost(_indexId[i],fieldMap);
            RANK_TRACE(TRACE3, matchDoc,
                       "docid = [%d], Termi=[%u], fieldBoostValue=[%f] ", matchDoc.getDocId(), i, fieldBoostValue );
            float tempLogDocFreq;
            if (_metaInfo[i].getTotalTermFreq() != 1) {
                tempLogDocFreq = log(_metaInfo[i].getTotalTermFreq())/log2;
            } else {
                tempLogDocFreq = 1.0;
            }
            RANK_TRACE(TRACE3, matchDoc,
                       "docid = [%d], Termi=[%u], totalTermFreq=[%f] ", matchDoc.getDocId(), i, tempLogDocFreq );
            score_t singleTermScore  = ((score_t)tfValue + fOccValue + fieldBoostValue )/tempLogDocFreq;
            totalDocFreq += tempLogDocFreq;
            score += singleTermScore * _metaInfo[i].getTermBoost();
            matchNum++;
            RANK_TRACE(TRACE2, matchDoc,
                       "docid = [%d], Termi=[%u], singleTermScore=[%f],termBoost=[%d] ", matchDoc.getDocId(), i,
                       singleTermScore, _metaInfo[i].getTermBoost() );
        }
    }
    if (matchNum > 0){
        totalDocFreq /= matchNum;
    }
    RANK_TRACE(TRACE2, matchDoc,
                       "docid = [%d], score=[%f], MatchedTermNum=[%d] ",
               matchDoc.getDocId(), score, matchNum );
    return score;
}

bool GeneralScorer::extractIndexId()
{
    const config::IndexInfoHelper *indexInfoHelper = NULL;
    uint32_t size = _metaInfo.size();
    if (size < 1) {
        AUTIL_LOG(WARN, "metaInfo's size less than one.");
        return false;
    }
    indexInfoHelper = _provider->getIndexInfoHelper();
    if ( NULL == indexInfoHelper) {
        AUTIL_LOG(WARN,"indexInfoHelper is NULL");
        return false;
    }
    _indexId = new (std::nothrow)indexid_t[size];
    if (NULL == _indexId) {
        AUTIL_LOG(WARN, "Failed to alloc memory for indexId ");
        return false;
    }
    for (uint32_t i = 0; i < size; ++i) {
        std::string indexName = _metaInfo[i].getIndexName();
        _indexId[i] = indexInfoHelper->getIndexId(indexName);
    }
    return true;
}

void GeneralScorer::initTables(const KeyValueMap &scorerParameters,
                               config::ResourceReader *resourceReader)
{
    std::string content;
    getContentFromResourceFile(RESOURCE_FILE_NAME,
                               scorerParameters, content, resourceReader);

    constructTables(content, FIRST_OCC_TABLE, _firstOccTablePtr, defaultFirstOccTable);
    constructTables(content, NUM_OCC_TABLE, _numOccTablePtr,  defaultNumOccTable);
    constructTables(content, PROXIMITY_TABLE, _proximityTablePtr, defaultProximityTable);

}
void GeneralScorer::constructTables(const std::string &content,
                                    const std::string &tableName,
                                    FloatVecPtr &floatVecPtr,
                                    const float *defaultValue)
{
    if (content.empty()
        || !extractStrValue(content, tableName,  floatVecPtr))
    {
        floatVecPtr->assign( defaultValue,  defaultValue+256);
    }

}
bool GeneralScorer::getContentFromResourceFile(const std::string &fileName,
                                 const KeyValueMap &scorerParameters,
                                 std::string &content,
                                 config::ResourceReader *resourceReader)
{
    if ( NULL == resourceReader) {
        AUTIL_LOG(WARN,"resourceReader pointers is NULL");
        return false;
    }
    KeyValueMap::const_iterator it = scorerParameters.find(fileName);
    if(it != scorerParameters.end())
    {
        if (resourceReader->getFileContent(content, it->second)) {
            return true;
        } else {
            AUTIL_LOG(WARN,"Can not read resource file:%s",it->second.c_str());
            return false;
        }
    } else {
        AUTIL_LOG(WARN, "Failed to get table File:%s",fileName.c_str());
        return false;
    }
    return false;
}
bool GeneralScorer::extractStrValue(const std::string &content,
                                    const std::string &name,
                                    FloatVecPtr &floatVecPtr)
{
    size_t namePos = content.find(name);
    std::string valueStr;
    if (namePos != std::string::npos) {
        size_t leftSepPos = content.find(LEFT_VALUE_SEPERATOR, namePos+name.length());
        size_t rightSepPos = content.find(RIGHT_VALUE_SEPERATOR, leftSepPos + 1);
        if (leftSepPos != std::string::npos
            && rightSepPos != std::string::npos)
       {
           valueStr = content.substr(leftSepPos+1, rightSepPos - leftSepPos - 1);
       }
    }
    if (valueStr.empty()) {
        return false;
    }
    autil::StringTokenizer stringTokenizer(valueStr,
            TABLE_VALUE_SEPERATOR,
            autil::StringTokenizer::TOKEN_IGNORE_EMPTY | autil::StringTokenizer::TOKEN_TRIM);
    if (stringTokenizer.getNumTokens() != 256) {
        AUTIL_LOG(WARN, "Failed to resolve table value file!!!");
        return false;
    }
    float value = 0.0;
    for(int i = 0; i < 256; ++i)
    {
        if (!autil::StringUtil::strToFloat(stringTokenizer[i].c_str(),value)) {
            AUTIL_LOG(WARN, "Failed to convert table value:[%s]",stringTokenizer[i].c_str());
            return false;
        }
        floatVecPtr->push_back(value);
    }
    return true;
}

uint32_t GeneralScorer::MatchedDocmuentProximityExtractor(matchdoc::MatchDoc matchDoc){
    uint32_t proximity = UINT32_MAX;
    uint32_t dist;
    std::vector<int32_t> vctFieldId;
    int32_t curFieldId;

    const MatchData &md = _matchDataRef->getReference(matchDoc);
    uint32_t oriSize = md.getNumTerms();
    pos_t curPos = 0;
    for(uint32_t i = 0;i<oriSize;i++){
        const TermMatchData &tmd = md.getTermMatchData(i);
        std::shared_ptr<indexlib::index::InDocPositionIterator> posIterPtr = tmd.getInDocPositionIterator();
        while ((NULL != posIterPtr.get()) &&
               (curPos = safeSeekPosition(posIterPtr, curPos)) != INVALID_POSITION)
        {
            if(std::find(vctFieldId.begin(),vctFieldId.end(),
                         posIterPtr->GetFieldPosition())
               == vctFieldId.end())
                vctFieldId.push_back(posIterPtr->GetFieldPosition());
        }
    }

    for(uint32_t i=0; i<vctFieldId.size(); i++){
        curFieldId = vctFieldId[i];
        dist = MatchedFieldProximityExtractor(curFieldId, matchDoc);
        RANK_TRACE(TRACE2, matchDoc,
                       "docid = [%d], FieldId=[%u], distance=[%u] ", matchDoc.getDocId(), curFieldId, dist );
        proximity = proximity>dist? dist:proximity;
    }
    RANK_TRACE(TRACE1, matchDoc,
                       "docid = [%d], proximity=[%u]", matchDoc.getDocId(), proximity );
    return proximity;

}

uint32_t GeneralScorer:: MatchedFieldProximityExtractor(int32_t curFieldId,
        matchdoc::MatchDoc matchDoc)
{
    const MatchData &md =_matchDataRef->getReference(matchDoc);

    uint32_t proximity = UINT32_MAX;
    pos_t curPos;
    std::vector<pos_t> vctPos;
    uint32_t oriSize = md.getNumTerms();
    for (uint32_t i = 0; i < oriSize-1; i++) {
        const TermMatchData &tmd = md.getTermMatchData(i);
        std::shared_ptr<indexlib::index::InDocPositionIterator> posIterPtr = tmd.getInDocPositionIterator();
        curPos = 0;
        vctPos.clear();
        while ((NULL!=posIterPtr.get()) &&
               (curPos = safeSeekPosition(posIterPtr, curPos)) != INVALID_POSITION)
        {
            if ( curFieldId == posIterPtr->GetFieldPosition()) {
                vctPos.push_back(curPos);
            }
        }
        if (vctPos.empty()) {
            continue;
        } else {
            for (uint32_t j = i + 1; j < oriSize; j++) {
                curPos = 0;
                const TermMatchData &_tmd = md.getTermMatchData(j);
                posIterPtr = _tmd.getInDocPositionIterator();

                while ((NULL!=posIterPtr.get()) &&
                       (curPos = safeSeekPosition(posIterPtr, curPos)) != INVALID_POSITION)
                {
                    if ( curFieldId == posIterPtr->GetFieldPosition()) {
                        uint32_t dist = (uint32_t)getMinDist(vctPos,curPos);
                        proximity = proximity>dist? dist : proximity;
                        if (1 == proximity) {
                            //return the min proximity
                            return (uint32_t)1;
                        }
                    }
                }
            }
            RANK_TRACE(TRACE3, matchDoc,
                       "docid = [%d], Termi =[%u], distance=[%u] ", matchDoc.getDocId(), i, proximity );
        }
    }  /* end of for (uint_32 i=0;...)*/
    return proximity;
}

void GeneralScorer::initScoreField(const KeyValueMap &scorerParameters)
{
    _staticWeight = 1;
    KeyValueMap::const_iterator it = scorerParameters.find(STATIC_SCORE_FIELD);
    if (it != scorerParameters.end()) {
        _staticField = it->second;
    }
    it = scorerParameters.find(STATIC_SCORE_WEIGHT);
    if (it != scorerParameters.end()) {
        if( false == autil::StringUtil::strToFloat((it->second).c_str(), _staticWeight)) {
            AUTIL_LOG(WARN, "Failed to convert static socre weight: [%s]", (it->second).c_str());
            _staticWeight = 1;
        }
    }
}

void GeneralScorer::initPhaseInfo(const KeyValueMap &scorerParameters)
{
    KeyValueMap::const_iterator it = scorerParameters.find(SCORE_PHASE_NUM);
    if (it != scorerParameters.end()) {
        if( false == autil::StringUtil::strToInt32((it->second).c_str(), _phase)) {
            AUTIL_LOG(WARN, "Failed to convert score phase : [%s]", (it->second).c_str());
            _phase = 1;
        }
    }
}

bool GeneralScorer::declarePhaseRef()
{
    if ( 1 == _phase || 2 == _phase) {
        _totalFreqRef = _provider->declareVariable<float>(SCORE_TOTAL_FREQ);
        if (NULL == _totalFreqRef) {
            AUTIL_LOG(WARN,"Failed to declare totalFreqRef");
            return false;
        }
        _phaseOneScoreRef = _provider->declareVariable<score_t>(PHASE_ONE_SCORE);
        if (NULL == _phaseOneScoreRef) {
            AUTIL_LOG(WARN,"Failed to declare phaseOneScoreRef");
            return false;
        }
    }
    return true;
}

score_t GeneralScorer::scorePhaseOne(matchdoc::MatchDoc matchDoc, float &totalDocFreq)
{
    RANK_TRACE(TRACE1, matchDoc, "Begin fisrt soreing phase:docid = [%d]",
               matchDoc.getDocId());
    totalDocFreq = 0;
    int32_t matchNum = 0;
    score_t score = computeTermScore(matchDoc, totalDocFreq, matchNum);
    RANK_TRACE(TRACE1, matchDoc, "docid = [%d],dynamic score = [%f]",
               matchDoc.getDocId(), score);
    score = score*0.01 + getStaticScore(matchDoc) * matchNum;
    if ( (1 == _phase) && (_totalFreqRef) && (_phaseOneScoreRef)) {
        _totalFreqRef->set(matchDoc, totalDocFreq);
        _phaseOneScoreRef->set(matchDoc, score);
    }
    return score;
}

score_t GeneralScorer::scorePhaseTwo(matchdoc::MatchDoc matchDoc, float totalDocFreq)
{
    RANK_TRACE(TRACE1, matchDoc, "Begin second soreing phase:docid = [%d]",
               matchDoc.getDocId() );
    score_t score = 0.0;
    const MatchData &data = _matchDataRef->getReference(matchDoc);
    int termsCount = data.getNumTerms();
    if ((2 == _phase) && (_totalFreqRef) && (_phaseOneScoreRef)) {
        totalDocFreq = _totalFreqRef->get(matchDoc);
        score = _phaseOneScoreRef->get(matchDoc);
    }
    if (totalDocFreq <= 0 ) {
            totalDocFreq = 1;
    }
    RANK_TRACE(TRACE1, matchDoc, "docid = [%d], phase one score=[%f] ",
               matchDoc.getDocId(), score );
    if (termsCount > 1) {
        uint32_t proximityIndex = MatchedDocmuentProximityExtractor(matchDoc);
        proximityIndex -= 1;
        proximityIndex = proximityIndex > 255 ? 255 : proximityIndex;
        float proximity = _proximityTablePtr->at(proximityIndex);
        proximity = proximity /  totalDocFreq ;
        RANK_TRACE(TRACE1, matchDoc, "docid = [%d], proximity score=[%f] ",
                   matchDoc.getDocId(), proximity );
        score += (score_t) proximity;
        RANK_TRACE(TRACE1, matchDoc, "docid = [%d], total score=[%f] ",
                   matchDoc.getDocId(), score );
    }
    return score;

}
} // namespace rank
} // namespace isearch
