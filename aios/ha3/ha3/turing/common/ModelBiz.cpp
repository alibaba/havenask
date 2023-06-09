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
#include <algorithm>
#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

#include "alog/Logger.h"
#include "autil/EnvUtil.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/FileSystem.h"
#include "ha3/common/CommonDef.h"
#include "ha3/sql/ops/tvf/SqlTvfPluginConfig.h"
#include "ha3/sql/ops/tvf/SqlTvfProfileInfo.h"
#include "ha3/sql/ops/tvf/TvfFunc.h"
#include "ha3/sql/ops/tvf/TvfFuncManager.h"
#include "ha3/sql/ops/tvf/builtin/ScoreModelTvfCreatorFactory.h"
#include "ha3/sql/ops/tvf/builtin/ScoreModelTvfFunc.h"
#include "ha3/turing/common/ModelBiz.h"
#include "ha3/turing/common/ModelConfig.h"
#include "ha3/turing/common/ModelMetadata.h"
#include "ha3/turing/common/TvfMetadata.h"
#include "ha3/turing/common/UdfConfig.h"
#include "ha3/turing/common/UdfMetadata.h"
#include "iquan/common/catalog/TvfFunctionModel.h"
#include "resource_reader/ResourceReader.h"
#include "suez/sdk/CmdLineDefine.h"
#include "suez/sdk/BizMeta.h"
#include "suez/sdk/ServiceInfo.h"
#include "suez/turing/common/BizInfo.h"
#include "suez/turing/common/ReturnStatus.h"
#include "suez/turing/search/Biz.h"
#include "suez/turing/search/base/BizBase.h"
#include "suez/turing/search/base/UserMetadata.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/status.h"
#include "tensorflow/core/platform/byte_order.h"

using namespace std;
using namespace tensorflow;
using namespace suez::turing;

using namespace isearch::sql;

namespace isearch {
namespace turing {
AUTIL_LOG_SETUP(ha3, ModelBiz);

ModelBiz::ModelBiz() {
}

ModelBiz::~ModelBiz() {
}

ReturnStatus ModelBiz::initBiz(const std::string &bizName,
                               const suez::BizMeta &bizMeta,
                               BizInitOptionBase &initOptions)
{
    TURING_RETURN_IF_ERROR(BizBase::initBiz(bizName, bizMeta, initOptions));
    TURING_RETURN_IF_TF_ERROR(initUserMetadata());
    return ReturnStatus::OK();
}

string ModelBiz::getBizInfoFile() {
    string fixedZoneName = getFixedZoneName();
    string absPath = fslib::fs::FileSystem::joinFilePath(
            _resourceReader->getConfigPath(), fixedZoneName + "_biz.json");
    if (fslib::fs::FileSystem::isExist(absPath) == fslib::EC_TRUE) {
        AUTIL_LOG(INFO, "Biz info file [%s] exists", absPath.c_str());
        return  fixedZoneName + "_biz.json";
    }
    AUTIL_LOG(INFO, "Biz info file [%s] does not exist.", absPath.c_str());
    return Biz::getBizInfoFile();
}

void ModelBiz::loadDeviceInfo() {
    _myDeviceName = getDeviceName(getFixedZoneName());
    _workerDeviceName = getDeviceName(_bizInfo._workerZone);
}

string ModelBiz::getFixedZoneName() {
    return _serviceInfo.getZoneName();
}

Status ModelBiz::initUserMetadata() {
    ModelConfig modelConfig;
    TF_RETURN_IF_ERROR(fillModelConfig(modelConfig));
    _userMetadata.addMetadata(new ModelMetadata(modelConfig));

    if (modelConfig.modelType == MODEL_TYPE::SCORE_MODEL) {
        if (!initSqlTvfFuncManager(modelConfig)) {
            return errors::Internal("init sqlTvfFuncManager failed");
        }
        _userMetadata.addMetadata(new TvfMetadata(_tvfFuncManager->getTvfNameToCreator()));
        UdfConfig udfConfig;
        TF_RETURN_IF_ERROR(fillFunctionConfig(modelConfig, udfConfig));
        _userMetadata.addMetadata(
                new UdfMetadata(udfConfig.functionModels, udfConfig.tvfModels));
    }
    return Status::OK();
}

Status ModelBiz::fillModelConfig(ModelConfig &modelConfig) {
    static string modelFileName = "model_info.json";
    string modelConfigStr;
    if (!_resourceReader->getFileContent(modelFileName, modelConfigStr)) {
        string errorMsg = "read [" + modelFileName + "] failed";
        AUTIL_LOG(ERROR, "%s", errorMsg.c_str());
        return errors::Internal(errorMsg);
    }
    try {
        FromJsonString(modelConfig, modelConfigStr);
    } catch (const exception &e) {
        AUTIL_LOG(ERROR, "parse model config [%s] failed, exception [%s]",
                  modelConfigStr.c_str(), e.what());
        return errors::Internal("parse model config failed, content\n" + modelConfigStr);
    }
    return Status::OK();
}

Status ModelBiz::fillFunctionConfig(const ModelConfig &modelConfig, UdfConfig &udfConfig) {
    if (_tvfFuncManager) {
        string specialCatalogsStr = autil::EnvUtil::getEnv(
                isearch::common::SPECIAL_CATALOG_LIST, "");
        const vector<string> &specialCatalogs =
            autil::StringUtil::split(specialCatalogsStr, ",");
        iquan::TvfModels inputTvfModels;
        if (!_tvfFuncManager->regTvfModels(inputTvfModels)) {
            return errors::Internal("reg tvf models failed");
        }
        addUserFunctionModels<iquan::TvfModels>(inputTvfModels,
                specialCatalogs, modelConfig.zoneName, udfConfig.tvfModels);
    }
    return Status::OK();
}

bool ModelBiz::initSqlTvfFuncManager(const ModelConfig &modelConfig) {
    vector<vector<string>> param;
    for (auto &node : modelConfig.searcherModelInfo.outputs) {
        param.push_back({node.node, node.type});
    }
    isearch::sql::SqlTvfProfileInfo tvfProfileInfo(
            _bizName,
            "scoreModelTvf",
            {
                {
                    isearch::sql::ScoreModelTvfFuncCreator::SCORE_MODEL_RETURNS,
                    autil::StringUtil::toString(param, string(":"), string(","))
                }
            });
    isearch::sql::SqlTvfPluginConfig sqlTvfPluginConfig;
    sqlTvfPluginConfig.tvfProfileInfos.emplace_back(tvfProfileInfo);
    build_service::config::ResourceReaderPtr pluginResouceReader(
            new build_service::config::ResourceReader(_bizMeta.getLocalConfigPath()));
    _tvfFuncManager.reset(new isearch::sql::TvfFuncManager(pluginResouceReader));
    _tvfFuncManager->addFactoryPtr(
            TvfFuncCreatorFactoryPtr(new ScoreModelTvfCreatorFactory()));
    if (!_tvfFuncManager->init(sqlTvfPluginConfig, false)) {
        AUTIL_LOG(ERROR, "init tvf plugin [%s] failed",
                  ToJsonString(sqlTvfPluginConfig.modules).c_str());
        return false;
    }
    return true;
}

} // namespace turing
} // namespace isearch
