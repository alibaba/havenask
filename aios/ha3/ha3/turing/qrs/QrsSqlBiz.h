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

#include "ha3/monitor/QrsBizMetrics.h"
#include "ha3/turing/common/SqlBiz.h"
#include "ha3/sql/resource/MessageWriterManager.h"

namespace isearch {
namespace turing {
class Ha3ServiceSnapshot;
} // namespace turing
} // namespace isearch

namespace iquan {
class TableModels;
}

namespace isearch {
namespace sql {
class SqlAuthManager;
class NaviConfigAdapter;
} // namespace sql
} // namespace isearch

namespace isearch {
namespace turing {

class ModelConfig;

class QrsSqlBiz: public SqlBiz
{
public:
    QrsSqlBiz(isearch::turing::Ha3ServiceSnapshot *snapshot,
              isearch::turing::Ha3BizMeta *ha3BizMeta);
    ~QrsSqlBiz();
private:
    QrsSqlBiz(const QrsSqlBiz &);
    QrsSqlBiz& operator=(const QrsSqlBiz &);
public:
    monitor::Ha3BizMetricsPtr getBizMetrics() const;
    std::string getDefaultDatabaseName() { return _defaultDatabaseName; }
    std::string getDefaultCatalogName() { return _defaultCatalogName; }
    suez::turing::QueryResourcePtr prepareQueryResource(autil::mem_pool::Pool *pool);
    bool updateNavi(const iquan::IquanPtr &sqlClient,
                    std::map<std::string, isearch::turing::ModelConfig> *modelConfigMap);
    size_t getSinglePoolUsageLimit() const;
    isearch::sql::SqlAuthManager *getSqlAuthManager() const;

protected:
    suez::turing::QueryResourcePtr createQueryResource() override;
    bool updateFlowConfig(const std::string &zoneBizName) override;
    std::string getBizInfoFile() override;
    bool getDefaultBizJson(std::string &defaultBizJson) override;
    tensorflow::Status doCreateSession(suez::turing::TfSession &tfsession,
                                       const tensorflow::SessionOptions &options,
                                       int sessionCount,
                                       const suez::turing::RestoreNodeMap &restoreNodeMap = {}) override {
        return tensorflow::Status::OK();
    }
    tensorflow::Status loadUserPlugins() override;

private:
    tensorflow::Status initUserMetadata() override;
    tensorflow::Status fillTableModels(const std::vector<std::string> &clusterNames,
                                       iquan::TableModels &tableModels);
    tensorflow::Status fillExternalTableModels(iquan::TableModels &tableModels);
    suez::turing::BizInfo getBizInfoWithJoinInfo(const std::string &bizName);
    bool initSqlAuthManager(const isearch::sql::AuthenticationConfig &authenticationConfig);
private:
    monitor::Ha3BizMetricsPtr _bizMetrics;
    std::string _defaultCatalogName;
    std::string _defaultDatabaseName;
    std::shared_ptr<isearch::sql::SqlAuthManager> _sqlAuthManager;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<QrsSqlBiz> QrsSqlBizPtr;
} // namespace turing
} // namespace isearch
