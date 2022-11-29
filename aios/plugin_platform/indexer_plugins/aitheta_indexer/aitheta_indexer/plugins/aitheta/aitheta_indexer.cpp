#include "aitheta_indexer/plugins/aitheta/aitheta_indexer.h"

#include <algorithm>
#include <autil/StringUtil.h>
#include <autil/TimeUtility.h>
#include <autil/legacy/exception.h>
#include <autil/legacy/jsonizable.h>
#include <string>
#include <vector>
#include "aitheta_indexer/plugins/aitheta/common_define.h"
#include "aitheta_indexer/plugins/aitheta/util/indexlib_io_wrapper.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(aitheta_plugin);
IE_LOG_SETUP(aitheta_plugin, AithetaIndexer);

bool AithetaIndexer::Build(const std::vector<const Field *> &fields, docid_t docId) {
    if (fields.size() < 1u || fields.size() > 3u) {
        IE_LOG(WARN, "unexpected fields size[%lu]", fields.size());
        return false;
    }

    if (!mCheckPkeyOnly && fields.size() == 1u) {
        IE_LOG(WARN, "unexpected fields size[%lu]", fields.size());
        return false;
    }

    // get pkey
    int64_t pkey = -1l;
    if (fields[0]->GetFieldTag() == Field::FieldTag::TOKEN_FIELD) {
        auto tokenizeField = static_cast<const IndexTokenizeField *>(fields[0]);
        auto section = *(tokenizeField->Begin());
        const int ONE = 1;
        if (section->GetLength() >= ONE) {
            pkey = section->GetToken(0)->GetHashKey();
        }
    } else {
        auto rawField = static_cast<const IndexRawField *>(fields[0]);
        string pkeyStr(rawField->GetData().data(), rawField->GetData().size());
        StringUtil::strToInt64(pkeyStr.data(), pkey);
    }

    std::vector<int64_t> catIds;
    EmbeddingPtr embedding;
    if (!mCheckPkeyOnly && fields.size() > 1u) {
        // get catIds
        string catIdsStr;
        if (3u == fields.size()) {
            auto rawField = static_cast<const IndexRawField *>(fields[1]);
            catIdsStr = string(rawField->GetData().data(), rawField->GetData().size());
            if (catIdsStr.empty()) {
                IE_LOG(ERROR, "3 fields are provided while category value is empty");
                return false;
            }
        }
        if (!CastCategoryFieldToVector(catIdsStr, catIds)) {
            return false;
        }

        // get embedding
        string embeddingStr;
        auto field = (2u == fields.size() ? fields[1] : fields[2]);
        auto rawField = static_cast<const IndexRawField *>(field);
        embeddingStr = string(rawField->GetData().data(), rawField->GetData().size());
        if (!CastEmbeddingFieldToVector(embeddingStr, embedding)) {
            return false;
        }
    } else {
        catIds.push_back(INVALID_CATEGORY_ID);
    }

    for (int64_t catId : catIds) {
        if (!mBuilder->Build(catId, pkey, docId, embedding)) {
            return false;
        }
    }

    return true;
}

bool AithetaIndexer::Dump(const DirectoryPtr &dir) {
    bool ret = mBuilder->Dump(dir);
    IE_LOG(INFO, "Failure number of casting embedding: [%lu]", mCastEmbeddingFailureNum);
    return ret;
}

bool AithetaIndexer::CastCategoryFieldToVector(const std::string &catIdsStr, std::vector<int64_t> &catIds) {
    auto catIdVec = StringUtil::split(catIdsStr, string("") + mComParam.mCategorySeparator);
    for (const auto &catIdStr : catIdVec) {
        int64_t catId = 0l;
        if (!StringUtil::strToInt64(catIdStr.data(), catId)) {
            IE_LOG(ERROR, "failed to parse [%s] to int64", catIdStr.data());
            return false;
        }
        catIds.push_back(catId);
    }
    if (catIds.empty()) {
        catIds.push_back(DEFAULT_CATEGORY_ID);
    }
    return true;
}

bool AithetaIndexer::CastEmbeddingFieldToVector(const std::string &embeddingStr, EmbeddingPtr &embedding) {
    vector<string> elements = StringUtil::split(embeddingStr, string("") + mComParam.mEmbeddingSeparator);
    size_t dim = elements.size();
    if (dim != static_cast<size_t>(mComParam.mDim)) {
        if (mCastEmbeddingFailureNum == 0u) {
            IE_LOG(ERROR, "embedding[%s] dim[%lu], expected[%d]", embeddingStr.data(), dim, mComParam.mDim);
        }
        ++mCastEmbeddingFailureNum;
        embedding.reset();
        return false;
    }
    SHARED_PTR_MALLOC(embedding, mComParam.mDim, float);
    float fvalue = 0.0f;
    for (size_t pos = 0; pos < dim; ++pos) {
        StringUtil::strToFloat(elements[pos].data(), fvalue);
        embedding.get()[pos] = fvalue;
    }
    return true;
}

bool AithetaIndexer::DoInit(const IndexerResourcePtr &resource) {
    auto phase = resource->options.GetBuildConfig().buildPhase;
    if (phase != BuildConfigBase::BuildPhase::BP_FULL) {
        // TODO(richard.sy): realtime and inc is disabled
        mCheckPkeyOnly = true;
    }
    return true;
}

IE_NAMESPACE_END(aitheta_plugin);
