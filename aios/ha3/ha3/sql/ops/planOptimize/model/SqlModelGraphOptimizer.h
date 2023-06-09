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

#include "ha3/isearch.h"
#include "ha3/sql/common/Log.h"
#include "ha3/turing/common/ModelConfig.h"
#include "ha3/sql/ops/planOptimize/GraphOptimizer.h"
#include "autil/legacy/jsonizable.h"
#include "aios/network/gig/multi_call/interface/QuerySession.h"
#include "ha3/service/QrsModelHandler.h"

namespace isearch {
namespace sql {

class SqlModelGraphOptimizer : public GraphOptimizer
{
public:
    SqlModelGraphOptimizer(std::map<std::string, isearch::turing::ModelConfig> *modelConfigMap,
                           autil::mem_pool::Pool *pool,
                           multi_call::QuerySession *querySession,
                           const iquan::DynamicParams &dynamicParams,
                           suez::turing::Tracer *tracer, int64_t leftTimeMs = 50)
        : GraphOptimizer()
        , _modelConfigMap(modelConfigMap)
        , _pool(pool)
        , _querySession(querySession)
        , _dynamicParams(dynamicParams)
        , _tracer(tracer)
        , _qrsModelHandlerPtr(new service::QrsModelHandler(leftTimeMs, tracer))
    {
    }

    ~SqlModelGraphOptimizer() {
    }

public:
    bool optimize(iquan::SqlPlan &sqlPlan) override;
    std::string getName() override {
        return "SqlModelGraphOptimizer";
    }
private:

    void getModelBizs(iquan::SqlPlan &sqlPlan,
                      std::vector<std::string> &keys,
                      std::vector<std::string> &modelBizs,
                      std::vector<std::map<std::string, std::string>> &kvPairs);
    std::string getParam(const std::string &param, iquan::DynamicParams &innerDynamicParams);
private:
    std::map<std::string, isearch::turing::ModelConfig> *_modelConfigMap = nullptr;
    autil::mem_pool::Pool *_pool = nullptr;
    multi_call::QuerySession *_querySession = nullptr;
    const iquan::DynamicParams &_dynamicParams;
    suez::turing::Tracer *_tracer = nullptr;
    service::QrsModelHandlerPtr _qrsModelHandlerPtr;
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SqlModelGraphOptimizer> SqlModelGraphOptimizerPtr;

} // namespace sql
} // namespace isearch
