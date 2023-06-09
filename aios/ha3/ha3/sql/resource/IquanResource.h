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

#include <unordered_map>

#include "autil/Log.h" // IWYU pragma: keep
#include "iquan/common/catalog/IquanCatalogClient.h"
#include "iquan/config/ClientConfig.h"
#include "iquan/config/JniConfig.h"
#include "iquan/config/WarmupConfig.h"
#include "iquan/jni/Iquan.h"
#include "navi/engine/Resource.h"

namespace iquan {
class CatalogInfo;
}
namespace catalog {
class CatalogClient;
}

namespace isearch {
namespace sql {

class SqlConfig;
class SqlConfigResource;

class IquanResource : public navi::Resource {
public:
//    using SubscribeHandler = iquan::IquanCatalogClient::SubscribeHandler;

public:
    IquanResource();
    IquanResource(std::shared_ptr<iquan::Iquan> sqlClient,
                  catalog::CatalogClient *catalogClient,
                  SqlConfigResource *sqlConfigResource);
    ~IquanResource();
    IquanResource(const IquanResource &) = delete;
    IquanResource &operator=(const IquanResource &) = delete;

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    bool initIquanClient();
    iquan::Iquan *getSqlClient() {
        return _sqlClient.get();
    }
    const iquan::JniConfig &getJniConfig() {
        return _jniConfig;
    }
    const iquan::WarmupConfig &getWarmupConfig() {
        return _warmupConfig;
    }
    bool updateCatalogInfo(const iquan::CatalogInfo &catalogInfo);

private:
    bool fillLogicTables(iquan::TableModels &tableModels);

    void dupKhronosCatalogInfo(const iquan::CatalogInfo &catalogInfo,
                               std::unordered_map<std::string, iquan::CatalogInfo> &catalogInfos);
    void createIquanCatalogClient(catalog::CatalogClient *catalogClient);
public:
    // TODO public and static func only reuse for initSqlClient
    static void fillSummaryTables(iquan::TableModels &tableModels, const SqlConfig &config);
    static bool fillKhronosTable(iquan::TableModels &tableModels, bool &isKhronosTable);
    static bool fillKhronosMetaTable(const iquan::TableModel &tm, iquan::TableModels &tableModels);
    static bool fillKhronosDataTable(const std::string &khronosType,
                                     const iquan::TableModel &tm,
                                     iquan::TableModels &tableModels);
    static bool fillKhronosDataFields(const std::string &khronosType,
                                      const std::string &valueSuffix,
                                      const std::string &fieldsStr,
                                      const std::vector<iquan::FieldDef> fields,
                                      iquan::TableDef &tableDef);

public:
    static const std::string RESOURCE_ID;

private:
    std::string _dbName;
    iquan::JniConfig _jniConfig;
    iquan::ClientConfig _clientConfig;
    iquan::WarmupConfig _warmupConfig;
    std::shared_ptr<iquan::Iquan> _sqlClient;
    std::unique_ptr<iquan::IquanCatalogClient> _iquanCatalogClient;
    SqlConfigResource *_sqlConfigResource = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<IquanResource> IquanResourcePtr;

} // end namespace sql
} // end namespace isearch
