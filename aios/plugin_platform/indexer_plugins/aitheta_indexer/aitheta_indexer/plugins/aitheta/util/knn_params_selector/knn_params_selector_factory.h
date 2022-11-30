/**
 * @file   knn_params_selector_factory.h
 * @author luoli.hn <luoli.hn@taobao.com>
 * @date   Wed Jan 16 15:24:09 2019
 *
 * @brief
 *
 *
 */
#ifndef __INDEXLIB_AITHETA_UTIL_KNN_PARAM_PARSER_FACTORY_H_
#define __INDEXLIB_AITHETA_UTIL_KNN_PARAM_PARSER_FACTORY_H_

#include <map>
#include <string>
#include <indexlib/common_define.h>
#include <indexlib/misc/singleton.h>
#include "aitheta_indexer/plugins/aitheta/util/knn_params_selector/i_knn_params_selector.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);

class KnnParamsSelectorFactory {
 public:
    KnnParamsSelectorFactory();
    ~KnnParamsSelectorFactory();

 public:
    IKnnParamsSelectorPtr CreateSelector(const std::string &knnName, const CommonParam &commonParam,
                                         const KnnConfig &knnDynamicConfigs, size_t docSize);

 private:
    bool EnableKnnLr(const CommonParam &commonParam, size_t docSize);

 private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(aitheta_plugin);

#endif  // __INDEXLIB_AITHETA_UTIL_KNN_PARAM_PARSER_FACTORY_H_
