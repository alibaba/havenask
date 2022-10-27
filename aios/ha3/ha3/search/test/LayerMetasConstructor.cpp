#include <ha3/search/test/LayerMetasConstructor.h>
#include <autil/StringUtil.h>
#include <autil/StringTokenizer.h>

using namespace std;
using namespace autil;

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, LayerMetasConstructor);

LayerMetasConstructor::LayerMetasConstructor() { 
}

LayerMetasConstructor::~LayerMetasConstructor() { 
}

LayerMeta LayerMetasConstructor::createLayerMeta(
        autil::mem_pool::Pool *pool,const string &layerMetaStr)
{
    const auto &vec = StringUtil::split(layerMetaStr, std::string("#"));
    auto newLayerMetaStr = layerMetaStr;
    QuotaType type = QT_PROPOTION;
    if (vec.size() == 2) {
        newLayerMetaStr = vec[1];
        type = (QuotaType)StringUtil::strToInt32WithDefault(vec[0].c_str(), QT_PROPOTION);
    }
    StringTokenizer st1(newLayerMetaStr, ";", StringTokenizer::TOKEN_IGNORE_EMPTY
                        | StringTokenizer::TOKEN_TRIM);
    LayerMeta meta(pool);
    meta.quotaType = type;
    for (size_t i = 0; i < st1.getNumTokens(); ++i) {
        StringTokenizer st2(st1[i], ",", StringTokenizer::TOKEN_IGNORE_EMPTY
                            | StringTokenizer::TOKEN_TRIM);
        assert(st2.getNumTokens() > 0);
        if (st2[0] == "param") {
            meta.quota = numeric_limits<uint32_t>::max();
            if (st2[1] != "UNLIMITED") {
                meta.quota = StringUtil::fromString<uint32_t>(st2[1]);
            }
            if (st2.getNumTokens() > 2) {
                if (st2[2] == "QM_PER_LAYER") {
                    meta.quotaMode = QM_PER_LAYER;
                }
            }
            if (st2.getNumTokens() > 3) {
                meta.maxQuota = StringUtil::fromString<uint32_t>(st2[3]);
            }
            continue;
        }
        assert(st2.getNumTokens() == 3 || st2.getNumTokens() == 1);
        if (st2.getNumTokens() == 3) {
            docid_t begin = StringUtil::fromString<docid_t>(st2[0].c_str());
            docid_t end = StringUtil::fromString<docid_t>(st2[1].c_str());
            uint32_t quota = numeric_limits<uint32_t>::max();
            if (st2[2] != "UNLIMITED") {
                quota = StringUtil::fromString<uint32_t>(st2[2].c_str());
            }
            DocIdRangeMeta range(begin, end, quota);
            meta.push_back(range);
        } else if (st2.getNumTokens() == 1) {
            uint32_t quota = numeric_limits<uint32_t>::max();
            if (st2[0] != "UNLIMITED") {
                quota = StringUtil::fromString<uint32_t>(st2[0].c_str());
            }
            meta.quota = quota;
            return meta;
        }
    }
    return meta;
}

LayerMetas LayerMetasConstructor::createLayerMetas(
        autil::mem_pool::Pool *pool, const string &layerMetasStr)
{
    StringTokenizer st(layerMetasStr, "|", StringTokenizer::TOKEN_TRIM);
    LayerMetas metas(pool);
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        metas.push_back(createLayerMeta(pool, st[i]));
    }
    return metas;
}


END_HA3_NAMESPACE(search);

