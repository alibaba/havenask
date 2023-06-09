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
#include "indexlib/common_define.h"
#include "indexlib/util/KeyValueMap.h"
#include "indexlib/misc/log.h"
#include "indexlib/indexlib.h"
#include <proxima/common/params_define.h>

#include "indexlib_plugin/plugins/aitheta/common_define.h"

#include "indexlib_plugin/plugins/aitheta/util/knn_config.h"

namespace indexlib {
namespace aitheta_plugin {

typedef std::function<void(const indexlib::util::KeyValueMap &, aitheta::IndexParams &, bool)> InitParamsFunc;
typedef std::function<void(const indexlib::util::KeyValueMap &, aitheta::IndexMeta &, bool)> InitMetaFunc;

class IKnnParamsSelector {
 public:
    IKnnParamsSelector(const KnnStrategies &strategies);
    virtual ~IKnnParamsSelector();

    virtual bool InitMeta(const indexlib::util::KeyValueMap &parameters, aitheta::IndexMeta &meta,
                          bool printParam = true) = 0;

    virtual bool InitBuilderParams(const indexlib::util::KeyValueMap &parameters, aitheta::IndexParams &indexParams,
                                   bool printParam = true) = 0;

    virtual bool InitSearcherParams(const indexlib::util::KeyValueMap &parameters,
                                    aitheta::IndexParams &indexParams, bool printParam = true) = 0;

    virtual bool InitStreamerParams(const indexlib::util::KeyValueMap &parameters,
                                    aitheta::IndexParams &indexParams, bool printParam = true) = 0;

    virtual bool EnableMipsParams(const indexlib::util::KeyValueMap &parameters) = 0;
    virtual bool InitMipsParams(const indexlib::util::KeyValueMap &parameters, MipsParams &params,
                                bool printParam = true) = 0;

    virtual const std::string &GetSearcherName() = 0;
    virtual const std::string &GetBuilderName() = 0;
    virtual const std::string &GetStreamerName() = 0;

    virtual int64_t GetOnlineSign(const aitheta::IndexParams &indexParams) {
        return IKnnParamsSelector::CreateSign(indexParams, GetSearcherName() + GetStreamerName());
    }

    virtual bool EnableGpuSearch(const SchemaParameter &schemaParameter, size_t docNum);
   
    static bool EnableKnnLr(const SchemaParameter &schemaParameter, size_t docNum) { return docNum <= schemaParameter.knnLrThold; }

 protected:
    aitheta::IndexDistance::Methods GetMethodType(const indexlib::util::KeyValueMap &parameters,
                                                  const std::string &key);
    bool GetDynamicParams(const indexlib::util::KeyValueMap &parameters, int32_t docNum,
                          indexlib::util::KeyValueMap &params);

    static int64_t CreateSign(const aitheta::IndexParams &indexParams, const std::string &ExtraParams = "");

 private:
    bool GetDynamicRangeParams(const indexlib::util::KeyValueMap &parameters, int32_t docNum,
                               KnnRangeParams &rangeParams);

    bool GetDynamicRangeParamsFromStrategy(const KnnStrategy &strategy, int32_t docNum, KnnRangeParams &rangeParams);

 protected:
    KnnStrategies _strategies;

 private:
    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(IKnnParamsSelector);

class NoneLinearKnnParamsSelector : public IKnnParamsSelector {
 public:
    NoneLinearKnnParamsSelector(const KnnStrategies &strategies);
    virtual ~NoneLinearKnnParamsSelector();

    bool InitMeta(const indexlib::util::KeyValueMap &parameters, aitheta::IndexMeta &meta,
                  bool printParam = true) override;
    bool InitMipsParams(const indexlib::util::KeyValueMap &parameters, MipsParams &params,
                        bool printParam = true) override;
    bool EnableMipsParams(const indexlib::util::KeyValueMap &parameters) override;

 private:
    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(NoneLinearKnnParamsSelector);

}
}
#endif  // __INDEXLIB_AITHETA_UTIL_I_KNN_PARAM_PARSER_H_
