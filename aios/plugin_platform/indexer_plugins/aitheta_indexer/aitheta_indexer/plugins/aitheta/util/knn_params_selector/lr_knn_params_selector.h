/**
 * @file   knn_params_selector.h
 * @author luoli.hn <luoli.hn@taobao.com>
 * @date   Wed Jan 16 16:47:43 2019
 *
 * @brief
 *
 *
 */

#ifndef __INDEXLIB_AITHETA_UTIL_LR_KNN_PARAM_PARSER_H_
#define __INDEXLIB_AITHETA_UTIL_LR_KNN_PARAM_PARSER_H_
#include <aitheta/index_framework.h>
#include <indexlib/common_define.h>
#include <indexlib/util/key_value_map.h>
#include <indexlib/misc/log.h>
#include <indexlib/indexlib.h>
#include <proxima/common/params_define.h>

#include "aitheta_indexer/plugins/aitheta/common_define.h"
#include "aitheta_indexer/plugins/aitheta/util/knn_params_selector/i_knn_params_selector.h"
#include "aitheta_indexer/plugins/aitheta/util/knn_config.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);

class LRKnnParamsSelector : public IKnnParamsSelector {
 public:
    LRKnnParamsSelector(const KnnStrategies &strategies);
    virtual ~LRKnnParamsSelector();

    virtual bool InitMeta(const IE_NAMESPACE(util)::KeyValueMap &parameters, aitheta::IndexMeta &meta,
                          bool printParam = true) override;
    virtual bool InitKnnBuilderParams(const IE_NAMESPACE(util)::KeyValueMap &parameters,
                                      aitheta::IndexParams &indexParams, bool printParam = true) override;

    virtual bool InitKnnSearcherParams(const IE_NAMESPACE(util)::KeyValueMap &parameters,
                                       aitheta::IndexParams &indexParams, bool printParam = true) override;
    bool EnableMipsParams(const IE_NAMESPACE(util)::KeyValueMap &parameters) override;
    bool InitMipsParams(const IE_NAMESPACE(util)::KeyValueMap &parameters, MipsParams &params,
                        bool printParam = true) override;
    const std::string &GetBuilderName() override { return KNN_LR_BUILDER; }
    const std::string &GetSearcherName() override { return KNN_LR_SEARCHER; }

 private:
    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(LRKnnParamsSelector);

IE_NAMESPACE_END(aitheta_plugin);
#endif  // __INDEXLIB_AITHETA_UTIL_LR_KNN_PARAM_PARSER_H_
