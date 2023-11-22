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
class FieldDef;
class Iquan;
class TableDef;
struct LocationSign;
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

private:
    bool loadLogicTables(iquan::CatalogDefs &catalogDefs,
                         const iquan::LocationSign &locationSign) const;
    bool loadLayerTables(iquan::CatalogDefs &catalogDefs,
                         const iquan::LocationSign &locationSign) const;

private:
    bool getGigCatalogDef(iquan::CatalogDefs &catalogDefs) const;
    bool fillExternalTableModels(iquan::CatalogDefs &catalogDefs,
                                 const iquan::LocationSign &locationSign);
    bool updateCatalogDef();

public:
    // TODO public and static func only reuse for initSqlClient
    static bool getIquanConfigFromFile(const std::string &filePath, std::string &configStr);

public:
    static const std::string RESOURCE_ID;

private:
    RESOURCE_DEPEND_DECLARE();

private:
    RESOURCE_DEPEND_ON(navi::GigMetaR, _gigMetaR);
    RESOURCE_DEPEND_ON_FALSE(UdfModelR, _udfModelR);
    RESOURCE_DEPEND_ON(ExternalTableConfigR, _externalTableConfigR);
    std::string _configPath;
    std::string _dbName;
    iquan::JniConfig _jniConfig;
    iquan::ClientConfig _clientConfig;
    iquan::WarmupConfig _warmupConfig;
    bool _disableSqlWarmup;
    std::map<std::string, std::vector<iquan::TableModel>> _logicTables;
    std::map<std::string, std::vector<iquan::LayerTableModel>> _layerTables;
    std::shared_ptr<iquan::Iquan> _sqlClient;
};

NAVI_TYPEDEF_PTR(IquanR);

} // namespace sql
