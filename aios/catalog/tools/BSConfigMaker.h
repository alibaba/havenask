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
#include "autil/legacy/json.h"
#include "catalog/entity/LoadStrategy.h"
#include "catalog/entity/Partition.h"
#include "catalog/util/StatusBuilder.h"

namespace catalog {

class BSConfigMaker {
public:
    static Status Make(const Partition &partition,
                       const std::string &bsTemplateConfigPath,
                       const std::string &storeRoot,
                       std::string *configPath);
    static Status validateSchema(const proto::Table &table);
    static Status mergeOnlineConfig(const std::string &baseConfigPath,
                                    const catalog::LoadStrategy *loadStrategy,
                                    std::string *mergedConfigPath);

private:
    struct BSConfig {
        std::string analyzerJson;
        std::string bsHippoJson;
        std::string buildAppJson;
        std::string clusterJson;
        std::string tableJson;
        std::string schemaJson;
    };

private:
    static Status genAnalyzerJson(const std::string &templatePath, std::string *content);
    static Status genBSHippoJson(const std::string &templatePath, std::string *content);
    static Status genBuildAppJson(const std::string &templatePath,
                                  const Partition &partition,
                                  const std::string &indexRoot,
                                  std::string *content);
    static Status genClusterJson(const std::string &templatePath, const Partition &partition, std::string *content);
    static Status genTableJson(const std::string &templatePath, const Partition &partition, std::string *content);
    static Status genSchemaJson(const Partition &partition, std::string *content);
    static Status genSortDescriptions(const proto::TableStructureConfig::SortOption &sortOption,
                                      std::string *sortDescription);
    static Status genHashMode(const proto::TableStructureConfig::ShardInfo &shardInfo, std::string *hashMode);
    static Status genDataDescriptions(const proto::DataSource::DataVersion &dataVersion, std::string *dataDescriptions);
    static std::string getPartitionRoot(const std::string &storeRoot, const Partition &partition);
    static Status uploadConfig(const BSConfig &bsConfig, const std::string &tableName, const std::string &targetPath);

    static Status mergeClusterJson(const std::string &configPath, const catalog::LoadStrategy &loadStrategy);
    static Status genOnlineIndexConfig(const catalog::LoadStrategy &loadStrategy, std::string *onlineIndexConfigStr);
    static Status getMapField(autil::legacy::json::JsonMap *jsonMap,
                              const std::string &fieldName,
                              autil::legacy::json::JsonMap *&field);
};

} // namespace catalog
