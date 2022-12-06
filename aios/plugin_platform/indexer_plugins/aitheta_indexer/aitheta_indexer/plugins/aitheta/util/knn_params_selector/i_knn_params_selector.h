/**
 * @file   knn_params_selector.h
 * @author luoli.hn <luoli.hn@taobao.com>
 * @date   Wed Jan 16 16:47:43 2019
 *
 * @brief
 *
 *
 */

#ifndef __INDEXLIB_AITHETA_UTIL_I_KNN_PARAM_PARSER_H_
#define __INDEXLIB_AITHETA_UTIL_I_KNN_PARAM_PARSER_H_
#include <aitheta/index_framework.h>
#include <indexlib/common_define.h>
#include <indexlib/util/key_value_map.h>
#include <indexlib/misc/log.h>
#include <indexlib/indexlib.h>
#include <proxima/common/params_define.h>

#include "aitheta_indexer/plugins/aitheta/common_define.h"

#include "aitheta_indexer/plugins/aitheta/util/knn_config.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);

typedef std::function<void(const IE_NAMESPACE(util)::KeyValueMap &, aitheta::IndexParams &, bool)> InitParamsFunc;
typedef std::function<void(const IE_NAMESPACE(util)::KeyValueMap &, aitheta::IndexMeta &, bool)> InitMetaFunc;

class IKnnParamsSelector {
 public:
    IKnnParamsSelector(const KnnStrategies &strategies);
    virtual ~IKnnParamsSelector();

    virtual bool InitMeta(const IE_NAMESPACE(util)::KeyValueMap &parameters,
                          aitheta::IndexMeta &meta, bool printParam = true) = 0;

    virtual bool InitKnnBuilderParams(const IE_NAMESPACE(util)::KeyValueMap &parameters,
                                      aitheta::IndexParams &indexParams, bool printParam = true) = 0;

    virtual bool InitKnnSearcherParams(const IE_NAMESPACE(util)::KeyValueMap &parameters,
                                       aitheta::IndexParams &indexParams, bool printParam = true) = 0;

    virtual bool EnableMipsParams(const IE_NAMESPACE(util)::KeyValueMap &parameters) = 0;
    virtual bool InitMipsParams(const IE_NAMESPACE(util)::KeyValueMap &parameters,
                 MipsParams& params, bool printParam = true) = 0;
    virtual const std::string& GetSearcherName() = 0;
    virtual const std::string& GetBuilderName() = 0;

    int64_t GetParamsSignature(const aitheta::IndexParams &indexParams);

 protected:
    aitheta::IndexDistance::Methods GetMethodType(const IE_NAMESPACE(util)::KeyValueMap &parameters,
                                                    const std::string &key);
    bool GetDynamicParams(const IE_NAMESPACE(util)::KeyValueMap &parameters, int32_t docSize,
                                 IE_NAMESPACE(util)::KeyValueMap &params);

 private:
    bool GetDynamicRangeParams(const IE_NAMESPACE(util)::KeyValueMap &parameters, int32_t docSize,
                               KnnRangeParams &rangeParams);

    bool GetDynamicRangeParamsFromStrategy(const KnnStrategy &strategy, int32_t docSize, KnnRangeParams &rangeParams);

 protected:
    KnnStrategies _strategies;

 private:
    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(IKnnParamsSelector);

class NoneLinearKnnParamsSelector: public IKnnParamsSelector {
 public:
    NoneLinearKnnParamsSelector(const KnnStrategies &strategies);
    virtual ~NoneLinearKnnParamsSelector();

    bool InitMeta(const IE_NAMESPACE(util)::KeyValueMap &parameters,
                          aitheta::IndexMeta &meta, bool printParam = true) override;
    bool InitMipsParams(const IE_NAMESPACE(util)::KeyValueMap &parameters,
                 MipsParams& params, bool printParam = true) override;
    bool EnableMipsParams(const IE_NAMESPACE(util)::KeyValueMap &parameters) override;
private:
    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(NoneLinearKnnParamsSelector);

IE_NAMESPACE_END(aitheta_plugin);
#endif  // __INDEXLIB_AITHETA_UTIL_I_KNN_PARAM_PARSER_H_
