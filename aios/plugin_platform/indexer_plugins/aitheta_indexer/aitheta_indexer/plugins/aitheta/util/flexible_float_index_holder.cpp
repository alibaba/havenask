
#include "aitheta_indexer/plugins/aitheta/util/flexible_float_index_holder.h"
#include "aitheta_indexer/plugins/aitheta/common_define.h"
#include <aitheta/utility/vector.h>

IE_NAMESPACE_BEGIN(aitheta_plugin);
using namespace aitheta;
IE_LOG_SETUP(aitheta_plugin, FlexibleFloatIndexHolder);

bool FlexibleFloatIndexHolder::mipsReform(const MipsParams &inputParams, MipsParams &outputParams) {
    if (mMipsMval > 0) {
        IE_LOG(ERROR, "the data has already been reformed with mval:[%d]", mMipsMval);
        return false;
    }
    if (!inputParams.enable) {
        IE_LOG(ERROR, "trying to MipsReform while inputParams.enabled is false");
        return false;
    }

    mMipsMval = inputParams.mval;

    IE_LOG(INFO, "inputParams:[enable:[%d], mval:[%u], uval:[%f], norm:[%f]]", inputParams.enable, inputParams.mval,
           inputParams.uval, inputParams.norm);
    aitheta::MipsReformer refomer(inputParams.mval, inputParams.uval, inputParams.norm);
    if (inputParams.norm <= 0) {
        IE_LOG(INFO, "begin norm update with data size:[%lu], dimension:[%d]", mDataVecor.size(), mDimension);
        for (const auto &embedding : mDataVecor) {
            refomer.update(embedding.mEmbedding.get(), mDimension);
        }
        outputParams = inputParams;
        outputParams.norm = refomer.norm();  // outputParams use updated norm
        IE_LOG(INFO, "finished norm update:[%f]", refomer.norm());
    }

    IE_LOG(INFO, "begin transFeature with data size:[%lu], dimension:[%d]", mDataVecor.size(), mDimension);
    aitheta::Vector<float> vec;
    for (auto &embedding : mDataVecor) {
        refomer.transFeature(embedding.mEmbedding.get(), mDimension, &vec);
        assert(vec.size() ==size_t (mDimension + mMipsMval));
        SHARED_PTR_MALLOC(embedding.mEmbedding, vec.size(), float);
        std::copy(vec.data(), vec.data() + vec.size(), embedding.mEmbedding.get());
    }
    IE_LOG(INFO, "finished transFeature");
    return true;
}

void FlexibleFloatIndexHolder::clear() {
    mDataVecor.clear();
    mMipsMval = 0;
}

IE_NAMESPACE_END(aitheta_plugin);
