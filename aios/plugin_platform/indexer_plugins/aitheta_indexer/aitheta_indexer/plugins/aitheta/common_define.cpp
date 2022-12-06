#include "aitheta_indexer/plugins/aitheta/common_define.h"

#include <aitheta/index_distance.h>

#include "util/param_util.h"

using namespace std;
using namespace aitheta;

IE_NAMESPACE_BEGIN(aitheta_plugin);
IE_LOG_SETUP(aitheta_plugin, CommonParam);

CommonParam::CommonParam()
    : mIndexType(""),
      mDim(INVALID_DIMENSION),
      mKnnLrThold(0UL),
      mTopKFlag("&n="),
      mScoreFilterFlag("&sf="),
      mCategoryEmbeddingSeparator('#'),
      mCategorySeparator(','),
      mEmbeddingSeparator(','),
      mExtendSeparator(':'),
      mQuerySeparator(';'),
      mEnableReportMetric(false) {}

CommonParam::CommonParam(const util::KeyValueMap& parameters) : CommonParam() {
    auto iterator = parameters.find(INDEX_TYPE);
    if (iterator != parameters.end()) {
        mIndexType = iterator->second;
    } else {
        IE_LOG(ERROR, "get index type failed");
    }

    iterator = parameters.find(TOPK_FLAG);
    if (iterator != parameters.end()) {
        mTopKFlag = iterator->second;
    }

    iterator = parameters.find(SCORE_FILTER_FLAG);
    if (iterator != parameters.end()) {
        mScoreFilterFlag = iterator->second;
    }

    ParamUtil::ExtractValue(parameters, DIMENSION, &mDim, USE_LINEAR_THRESHOLD, &mKnnLrThold,
                            CATEGORY_EMBEDDING_SEPARATOR, &mCategoryEmbeddingSeparator, CATEGORY_SEPARATOR,
                            &mCategorySeparator, EMBEDDING_SEPARATOR, &mEmbeddingSeparator, EXTEND_SEPARATOR,
                            &mExtendSeparator, QUERY_SEPARATOR, &mQuerySeparator, ENABLE_REPORT_METRIC,
                            &mEnableReportMetric);

    IE_LOG(INFO,
           "CommonParam init:[%s = %s], [%s = %d], [%s = %lu], [%s = %s], [%s = %s], "
           "[%s] = %c], [%s = %c], [%s = %c], [%s = %c], [%s = %c], [%s = %d]",
           INDEX_TYPE.data(), mIndexType.data(), DIMENSION.data(), mDim, USE_LINEAR_THRESHOLD.data(), mKnnLrThold,
           TOPK_FLAG.data(), mTopKFlag.data(), SCORE_FILTER_FLAG.data(), mScoreFilterFlag.data(),
           CATEGORY_EMBEDDING_SEPARATOR.data(), mCategoryEmbeddingSeparator, CATEGORY_SEPARATOR.data(),
           mCategorySeparator, EMBEDDING_SEPARATOR.data(), mEmbeddingSeparator, EXTEND_SEPARATOR.data(),
           mExtendSeparator, QUERY_SEPARATOR.data(), mQuerySeparator, ENABLE_REPORT_METRIC.data(), mEnableReportMetric);
}

void MipsParams::Jsonize(JsonWrapper& json) {
    json.Jsonize("enable", enable, false);
    json.Jsonize("mval", mval, 0U);
    json.Jsonize("uval", uval, 0.0f);
    json.Jsonize("norm", norm, 0.0f);
};

void JsonizableIndexInfo::Jsonize(JsonWrapper& json) {
    json.Jsonize("catid", mCatId, -1);
    json.Jsonize("docnum", mDocNum, 0UL);
    json.Jsonize("mips", mMipsParams, MipsParams());
}

IE_NAMESPACE_END(aitheta_plugin);
