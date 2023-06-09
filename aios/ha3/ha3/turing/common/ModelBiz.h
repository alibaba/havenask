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
#include "autil/Log.h" // IWYU pragma: keep
#include "autil/RangeUtil.h"
#include "ha3/turing/common/ModelConfig.h"
#include "ha3/turing/common/UdfConfig.h"
#include "ha3/turing/common/Ha3BizBase.h"
#include "ha3/sql/ops/tvf/TvfFuncManager.h"

namespace isearch {
namespace turing {

class ModelBiz: public Ha3BizBase
{
public:
    ModelBiz();
    ~ModelBiz();
private:
    ModelBiz(const ModelBiz &);
    ModelBiz& operator=(const ModelBiz &);

public:
    suez::turing::ReturnStatus initBiz(const std::string &bizName,
                                       const suez::BizMeta &bizMeta,
                                       suez::turing::BizInitOptionBase &initOptions) override;

protected:
    std::string getBizInfoFile() override;
    void loadDeviceInfo() override;
    virtual std::string getFixedZoneName();
private:
    tensorflow::Status initUserMetadata();
    tensorflow::Status fillModelConfig(ModelConfig &modelConfig);
    tensorflow::Status fillFunctionConfig(const ModelConfig &modelConfig,
            UdfConfig &udfConfig);
    bool initSqlTvfFuncManager(const ModelConfig &modelConfig);
private:
    template <typename IquanModels>
    void addUserFunctionModels(const IquanModels &inModels,
                               const std::vector<std::string> &specialCatalogs,
                               const std::string &dbName,
                               IquanModels &functionModels);
private:
    isearch::sql::TvfFuncManagerPtr _tvfFuncManager;
private:
    AUTIL_LOG_DECLARE();
};

template <typename IquanModels>
void ModelBiz::addUserFunctionModels(const IquanModels &inModels,
                                     const std::vector<std::string> &specialCatalogs,
                                     const std::string &dbName,
                                     IquanModels &outModels)
{

    for (auto model : inModels.functions) {
        if (!model.catalogName.empty()) {
            model.databaseName = dbName;
            outModels.functions.emplace_back(model);
        } else if (specialCatalogs.empty()){
            model.catalogName = SQL_DEFAULT_CATALOG_NAME;
            model.databaseName = dbName;
            outModels.functions.emplace_back(model);
        } else {
            SQL_LOG(INFO, "use special catalog name: [%s]",
                    autil::StringUtil::toString(specialCatalogs).c_str());
            for (auto const &catalogName : specialCatalogs) {
                model.catalogName = catalogName;
                model.databaseName = dbName;
                outModels.functions.emplace_back(model);
            }
        }
    }
}

typedef std::shared_ptr<ModelBiz> ModelBizPtr;

} // namespace turing
} // namespace isearch
