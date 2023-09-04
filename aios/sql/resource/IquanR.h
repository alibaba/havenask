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
#include <string>
#include <unordered_map>
#include <vector>

#include "iquan/common/catalog/LayerTableModel.h"
#include "iquan/common/catalog/TableModel.h"
#include "iquan/config/ClientConfig.h"
#include "iquan/config/JniConfig.h"
#include "iquan/config/WarmupConfig.h"
#include "navi/common.h"
#include "navi/engine/Resource.h"
#include "navi/engine/ResourceConfigContext.h"
#include "navi/resource/GigMetaR.h"
#include "sql/config/ExternalTableConfigR.h"
#include "sql/resource/SqlConfigResource.h"
#include "sql/resource/UdfModelR.h"

namespace navi {
class ResourceDefBuilder;
class ResourceInitContext;
} // namespace navi

namespace iquan {
class CatalogInfo;
class FieldDef;
class Iquan;
class TableDef;
} // namespace iquan

namespace sql {

class IquanR : public navi::Resource {
public:
    IquanR();
    ~IquanR();
    IquanR(const IquanR &) = delete;
    IquanR &operator=(const IquanR &) = delete;

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

private:
    bool loadLogicTables(iquan::TableModels &tableModels) const;

private:
    bool updateCatalogInfo();
    bool getGigTableModels(iquan::TableModels &tableModels) const;
    bool fillLogicTables(iquan::TableModels &tableModels);
    bool fillExternalTableModels(iquan::TableModels &tableModels);

public:
    // TODO public and static func only reuse for initSqlClient
    static void fillSummaryTables(iquan::TableModels &tableModels,
                                  const std::vector<std::string> &summaryTables);

    static bool fillKhronosTable(iquan::TableModels &tableModels);
    static bool getIquanConfigFromFile(const std::string &filePath, std::string &configStr);

private:
    static void
    dupKhronosCatalogInfo(const iquan::CatalogInfo &catalogInfo,
                          std::unordered_map<std::string, iquan::CatalogInfo> &catalogInfos);
    static void
    addAliasTableName(iquan::TableModels &tableModels,
                      const std::map<std::string, std::vector<std::string>> &tableNameAlias);
    static bool isKhronosTable(const iquan::TableDef &td);
    static bool fillKhronosMetaTable(const iquan::TableModel &tm, iquan::TableModels &tableModels);
    static void fillKhronosBuiltinFields(iquan::TableModel &tableModel);
    static bool fillKhronosLogicTableV2(const iquan::TableModel &tm,
                                        iquan::TableModels &tableModels);
    static bool fillKhronosLogicTableV3(const iquan::TableModel &tm,
                                        iquan::TableModels &tableModels);
    static bool doFillKhronosMetaTable(const std::string &tableName,
                                       const std::string &dbName,
                                       const std::string &catalogName,
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
    RESOURCE_DEPEND_DECLARE();

private:
    RESOURCE_DEPEND_ON(navi::GigMetaR, _gigMetaR);
    RESOURCE_DEPEND_ON_FALSE(UdfModelR, _udfModelR);
    RESOURCE_DEPEND_ON(SqlConfigResource, _sqlConfigResource);
    RESOURCE_DEPEND_ON(ExternalTableConfigR, _externalTableConfigR);
    std::string _configPath;
    std::string _dbName;
    iquan::JniConfig _jniConfig;
    iquan::ClientConfig _clientConfig;
    iquan::WarmupConfig _warmupConfig;
    iquan::TableModels _logicTables;
    iquan::TableModels _configTables;
    iquan::LayerTableModels _layerTables;
    std::vector<std::string> _summaryTables;
    std::map<std::string, std::vector<std::string>> _tableNameAlias;
    std::shared_ptr<iquan::Iquan> _sqlClient;
    bool _innerDocIdOptimizeEnable = false;
};

NAVI_TYPEDEF_PTR(IquanR);

} // namespace sql
