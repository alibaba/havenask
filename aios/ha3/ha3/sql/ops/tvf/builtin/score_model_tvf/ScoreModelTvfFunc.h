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
#include <map>
#include <memory>
#include <stdint.h>
#include <string>
#include <unordered_map>
#include <vector>

#include "autil/Log.h"
#include "ha3/isearch.h"
#include "ha3/sql/common/Log.h" // IWYU pragma: keep
#include "ha3/sql/ops/tvf/TvfFunc.h"
#include "ha3/sql/ops/tvf/TvfFuncCreatorR.h"
#include "ha3/turing/common/ModelConfig.h"
#include "aios/network/gig/multi_call/interface/Reply.h"
#include "aios/network/gig/multi_call/interface/Response.h"
#include "aios/network/gig/multi_call/interface/SearchService.h"
#include "navi/builder/ResourceDefBuilder.h"
#include "table/Table.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/types.pb.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace isearch {
namespace sql {
class SqlTvfProfileInfo;
} // namespace sql
} // namespace isearch
namespace multi_call {
class QuerySession;
} // namespace multi_call
namespace suez {
namespace turing {
class GraphRequest;
} // namespace turing
} // namespace suez

namespace iquan {
class TvfFieldDef;
class TvfModel;
} // namespace iquan

namespace isearch {
namespace sql {

class ScoreModelTvfFunc : public OnePassTvfFunc {
public:
    ScoreModelTvfFunc(const std::string &modelBiz);
    ~ScoreModelTvfFunc();
    ScoreModelTvfFunc(const ScoreModelTvfFunc &) = delete;
    ScoreModelTvfFunc &operator=(const ScoreModelTvfFunc &) = delete;

private:
    bool init(TvfFuncInitContext &context) override;
    bool doCompute(const table::TablePtr &input, table::TablePtr &output) override;
    bool fillResult(multi_call::ReplyPtr reply, table::TablePtr &output);
    bool fillOutputTensor(const multi_call::ResponsePtr &response, table::TablePtr &output);
    bool parseEmbeddingTensors(const std::string &embeddingStr);
    bool prepareSearchParam(const table::TablePtr &input, multi_call::SearchParam &param);
    suez::turing::GraphRequest *
    prepareGraphRequest(const std::unordered_map<std::string, std::string> &kvPairs,
                        int64_t timeout,
                        const isearch::turing::ModelConfig &modelConfig,
                        const table::TablePtr &tableInput);
    bool extractPkVec(const std::string &pkColumnName,
                      const table::TablePtr &input,
                      const tensorflow::DataType &type,
                      tensorflow::Tensor &inputTensor);
    bool extractInputFromKvPairs(const std::unordered_map<std::string, std::string> &kvPairs,
                                 const isearch::turing::NodeConfig &node,
                                 const tensorflow::DataType &type,
                                 tensorflow::Tensor &inputTensor);
    void logRecord(const table::TablePtr &input);

private:
    std::string _modelBiz;
    std::unordered_map<std::string, std::string> _kvPairs;
    std::map<std::string, tensorflow::Tensor> _embeddingTensors;
    autil::mem_pool::Pool *_pool;
    autil::mem_pool::Pool *_queryPool;
    isearch::turing::ModelConfigMap *_modelConfigMap;
    multi_call::QuerySession *_querySession;
    int64_t _timeout;
    std::string _rn;
    kmonitor::MetricsReporter *_metricReporter = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ScoreModelTvfFunc> ScoreModelTvfFuncPtr;

class ScoreModelTvfFuncCreator : public TvfFuncCreatorR {
public:
    ScoreModelTvfFuncCreator();
    ~ScoreModelTvfFuncCreator();

public:
    void def(navi::ResourceDefBuilder &builder) const override {
        builder.name("SCORE_MODEL_TVF_FUNC");
    }

public:
    static const std::string SCORE_MODEL_RETURNS;

private:
    ScoreModelTvfFuncCreator(const ScoreModelTvfFuncCreator &) = delete;
    ScoreModelTvfFuncCreator &operator=(const ScoreModelTvfFuncCreator &) = delete;

private:
    bool initTvfModel(iquan::TvfModel &tvfModel,
                      const isearch::sql::SqlTvfProfileInfo &info) override;
    TvfFunc *createFunction(const SqlTvfProfileInfo &info) override;
    bool transToTvfReturnType(const KeyValueMap &kvMap, std::vector<iquan::TvfFieldDef> &newFields);

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ScoreModelTvfFuncCreator> ScoreModelTvfFuncCreatorPtr;
} // namespace sql
} // namespace isearch
