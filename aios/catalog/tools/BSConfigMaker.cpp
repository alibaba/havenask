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
#include "catalog/tools/BSConfigMaker.h"

#include "autil/StringUtil.h"
#include "autil/legacy/jsonizable.h"
#include "catalog/tools/ConfigFileUtil.h"
#include "catalog/tools/TableSchemaConfig.h"
#include "fslib/fs/FileSystem.h"

using namespace std;

namespace catalog {

Status BSConfigMaker::Make(const Partition &partition,
                           const string &templatePath,
                           const string &storeRoot,
                           string *configPath) {
    CATALOG_CHECK(!storeRoot.empty(), Status::INVALID_ARGUMENTS, "store root is empty");
    CATALOG_CHECK(!templatePath.empty(), Status::INVALID_ARGUMENTS, "bs template config path is empty");
    CATALOG_CHECK_OK(ConfigFileUtil::assertPathExist(templatePath, "template config path"));
    string partitionRoot = getPartitionRoot(storeRoot, partition);
    string indexRoot = partitionRoot + "/index";

    BSConfig bsConfig;
    CATALOG_CHECK_OK(genAnalyzerJson(templatePath, &bsConfig.analyzerJson));
    CATALOG_CHECK_OK(genBSHippoJson(templatePath, &bsConfig.bsHippoJson));
    CATALOG_CHECK_OK(genBuildAppJson(templatePath, partition, indexRoot, &bsConfig.buildAppJson));
    CATALOG_CHECK_OK(genClusterJson(templatePath, partition, &bsConfig.clusterJson));
    CATALOG_CHECK_OK(genTableJson(templatePath, partition, &bsConfig.tableJson));
    CATALOG_CHECK_OK(genSchemaJson(partition, &bsConfig.schemaJson));
    string bsConfigPath = partitionRoot + "/config";
    if (fslib::fs::FileSystem::isExist(bsConfigPath) == fslib::EC_TRUE) {
        *configPath = bsConfigPath;
        return StatusBuilder::ok();
    }

    string tmpPath = bsConfigPath + ".tmp";
    CATALOG_CHECK_OK(ConfigFileUtil::rmPath(tmpPath));
    CATALOG_CHECK_OK(uploadConfig(bsConfig, partition.id().tableName, tmpPath));
    CATALOG_CHECK_OK(ConfigFileUtil::rmPath(bsConfigPath));
    CATALOG_CHECK_OK(ConfigFileUtil::mvPath(tmpPath, bsConfigPath));
    *configPath = bsConfigPath;

    return StatusBuilder::ok();
}

Status BSConfigMaker::genAnalyzerJson(const string &templatePath, string *content) {
    return ConfigFileUtil::readFile(templatePath + "/analyzer.json", content);
}
Status BSConfigMaker::genBSHippoJson(const string &templatePath, string *content) {
    return ConfigFileUtil::readFile(templatePath + "/bs_hippo.json", content);
}

Status BSConfigMaker::genBuildAppJson(const string &templatePath,
                                      const Partition &partition,
                                      const string &indexRoot,
                                      string *content) {
    CATALOG_CHECK_OK(ConfigFileUtil::readFile(templatePath + "/build_app.json", content));
    autil::StringUtil::replaceAll(*content, "${index_root}", indexRoot);
    const auto &custom_metas = partition.partitionConfig().custom_metas();
    auto iter = custom_metas.find("swift_root");
    if (iter != custom_metas.end()) {
        const auto &swiftRoot = iter->second;
        autil::StringUtil::replaceAll(*content, "${swift_root}", swiftRoot);
    }
    iter = custom_metas.find("zookeeper_root");
    if (iter != custom_metas.end()) {
        const auto &zkRoot = iter->second;
        autil::StringUtil::replaceAll(*content, "${zookeeper_root}", zkRoot);
    }
    return StatusBuilder::ok();
}
Status BSConfigMaker::genClusterJson(const string &templatePath, const Partition &partition, string *content) {
    CATALOG_CHECK_OK(ConfigFileUtil::readFile(templatePath + "/clusters/default_cluster.json", content));

    bool sortBuild = partition.getTableStructure()->tableStructureConfig().sort_option().sort_build();
    if (sortBuild) {
        autil::StringUtil::replaceAll(*content, "${sort_build}", "true");
        string sortDescriptions;
        CATALOG_CHECK_OK(genSortDescriptions(partition.getTableStructure()->tableStructureConfig().sort_option(),
                                             &sortDescriptions));
        autil::StringUtil::replaceAll(*content, "${sort_descriptions}", sortDescriptions);
    } else {
        autil::StringUtil::replaceAll(*content, "${sort_build}", "false");
        autil::StringUtil::replaceAll(*content, "${sort_descriptions}", "[]");
    }
    auto partitionCount = partition.getTableStructure()->tableStructureConfig().shard_info().shard_count();
    autil::StringUtil::replaceAll(*content, "${partition_count}", to_string(partitionCount));
    autil::StringUtil::replaceAll(*content, "${table_name}", partition.id().tableName);
    string hashMode;
    CATALOG_CHECK_OK(genHashMode(partition.getTableStructure()->tableStructureConfig().shard_info(), &hashMode));
    autil::StringUtil::replaceAll(*content, "${hash_mode}", hashMode);

    const auto &dataSource = partition.dataSource();
    const auto &dataVersion = dataSource.data_version();
    for (const auto &dataDescription : dataVersion.data_descriptions()) {
        if (dataDescription.src() == "swift") {
            autil::StringUtil::replaceAll(*content, "${swift_root}", dataDescription.swift_root());
            autil::StringUtil::replaceAll(*content, "${topic_name}", dataDescription.swift_topic_name());
        }
    }

    return StatusBuilder::ok();
}
Status BSConfigMaker::genTableJson(const string &templatePath, const Partition &partition, string *content) {
    CATALOG_CHECK_OK(ConfigFileUtil::readFile(templatePath + "/data_tables/default_table.json", content));
    string dataDescriptions;
    CATALOG_CHECK_OK(genDataDescriptions(partition.dataSource().data_version(), &dataDescriptions));
    autil::StringUtil::replaceAll(*content, "${data_descriptions}", dataDescriptions);
    autil::StringUtil::replaceAll(*content, "${table_name}", partition.id().tableName);
    return StatusBuilder::ok();
}
Status BSConfigMaker::genSchemaJson(const Partition &partition, string *content) {
    TableSchemaConfig schemaConfig;
    const auto &id = partition.id();
    string partitionId = id.catalogName + "." + id.databaseName + "." + id.tableName + "." + id.partitionName;
    CATALOG_CHECK(schemaConfig.init(id.tableName, partition.getTableStructure()),
                  Status::INTERNAL_ERROR,
                  "table schema config init failed, [",
                  partitionId,
                  "]");
    *content = autil::legacy::ToJsonString(schemaConfig);
    return StatusBuilder::ok();
}

string BSConfigMaker::getPartitionRoot(const string &storeRoot, const Partition &partition) {
    string partitionRoot = storeRoot;
    if (partitionRoot.back() == '/') {
        partitionRoot.pop_back();
    }
    const auto &id = partition.id();
    const auto &signatureStr = partition.getSignature();
    if (!signatureStr.empty()) {
        return partitionRoot + "/" + id.catalogName + "/" + id.databaseName + "/" + id.tableName + "/" +
               id.partitionName + "/" + signatureStr;
    }
    return partitionRoot + "/" + id.catalogName + "/" + id.databaseName + "/" + id.tableName + "/" + id.partitionName;
}

Status BSConfigMaker::uploadConfig(const BSConfig &bsConfig, const string &tableName, const string &targetPath) {
    CATALOG_CHECK_OK(ConfigFileUtil::mkDir(targetPath, true));
    CATALOG_CHECK_OK(ConfigFileUtil::uploadFile(targetPath + "/analyzer.json", bsConfig.analyzerJson));
    CATALOG_CHECK_OK(ConfigFileUtil::uploadFile(targetPath + "/bs_hippo.json", bsConfig.bsHippoJson));
    CATALOG_CHECK_OK(ConfigFileUtil::uploadFile(targetPath + "/build_app.json", bsConfig.buildAppJson));
    CATALOG_CHECK_OK(ConfigFileUtil::mkDir(targetPath + "/clusters"));
    CATALOG_CHECK_OK(
        ConfigFileUtil::uploadFile(targetPath + "/clusters/" + tableName + "_cluster.json", bsConfig.clusterJson));
    CATALOG_CHECK_OK(ConfigFileUtil::mkDir(targetPath + "/data_tables"));
    CATALOG_CHECK_OK(
        ConfigFileUtil::uploadFile(targetPath + "/data_tables/" + tableName + "_table.json", bsConfig.tableJson));
    CATALOG_CHECK_OK(ConfigFileUtil::mkDir(targetPath + "/schemas"));
    CATALOG_CHECK_OK(
        ConfigFileUtil::uploadFile(targetPath + "/schemas/" + tableName + "_schema.json", bsConfig.schemaJson));
    return StatusBuilder::ok();
}

Status BSConfigMaker::genSortDescriptions(const proto::TableStructureConfig::SortOption &sortOption,
                                          string *sortDescriptions) {
    vector<map<string, string>> result;
    result.reserve(sortOption.sort_descriptions_size());
    for (int i = 0; i < sortOption.sort_descriptions_size(); ++i) {
        const auto &sortDescription = sortOption.sort_descriptions(i);
        const auto &sortField = sortDescription.sort_field();
        const auto &sortPattern = sortDescription.sort_pattern();
        CATALOG_CHECK(!sortField.empty(),
                      Status::INVALID_ARGUMENTS,
                      "sort field is empty, [",
                      sortOption.ShortDebugString(),
                      "]");
        CATALOG_CHECK(sortPattern != proto::TableStructureConfig::SortOption::SortDescription::UNKNOWN,
                      Status::INVALID_ARGUMENTS,
                      "sort pattern can not be UNKNOWN, [",
                      sortOption.ShortDebugString(),
                      "]");
        string sortPatternStr = "asc";
        if (sortPattern == proto::TableStructureConfig::SortOption::SortDescription::DESC) {
            sortPatternStr = "desc";
        }
        map<string, string> m;
        m["sort_field"] = sortField;
        m["sort_pattern"] = sortPatternStr;
        result.push_back(m);
    }
    *sortDescriptions = autil::legacy::ToJsonString(result);
    return StatusBuilder::ok();
}

Status BSConfigMaker::genHashMode(const proto::TableStructureConfig::ShardInfo &shardInfo, std::string *hashMode) {
    vector<autil::legacy::Any> hashFields;
    hashFields.reserve(shardInfo.shard_fields_size());
    for (int i = 0; i < shardInfo.shard_fields_size(); ++i) {
        autil::legacy::Any any = shardInfo.shard_fields(i);
        hashFields.push_back(any);
    }
    CATALOG_CHECK(
        !hashFields.empty(), Status::INVALID_ARGUMENTS, "shard fields is empty, [", shardInfo.ShortDebugString(), "]");
    const auto &shardFunc = shardInfo.shard_func();
    CATALOG_CHECK(
        !shardFunc.empty(), Status::INVALID_ARGUMENTS, "shard func is empty, [", shardInfo.ShortDebugString(), "]");
    // TODO(makuo.mnb): support routing hash
    map<string, autil::legacy::Any> result;
    autil::legacy::Any hashFieldsAny = hashFields;
    autil::legacy::Any hashFunctionAny = shardFunc;
    result.emplace("hash_fields", hashFieldsAny);
    result.emplace("hash_function", hashFunctionAny);
    *hashMode = autil::legacy::ToJsonString(result);
    return StatusBuilder::ok();
}

Status BSConfigMaker::genDataDescriptions(const proto::DataSource::DataVersion &dataVersion,
                                          std::string *dataDescriptions) {
    vector<map<string, string>> result;
    result.reserve(dataVersion.data_descriptions_size());
    for (int i = 0; i < dataVersion.data_descriptions_size(); ++i) {
        const auto &description = dataVersion.data_descriptions(i);
        map<string, string> m;
        const auto &src = description.src();
        CATALOG_CHECK(!src.empty(), Status::INVALID_ARGUMENTS, "src is empty, [", dataVersion.ShortDebugString(), "]");
        m["src"] = src;
        if (src == "file") {
            const auto &dataPath = description.data_path();
            CATALOG_CHECK(!dataPath.empty(),
                          Status::INVALID_ARGUMENTS,
                          "data path is empty, [",
                          dataVersion.ShortDebugString(),
                          "]");
            m["type"] = "file";
            m["data"] = dataPath;
        } else if (src == "swift") {
            m["type"] = "swift";
            m["swift_root"] = description.swift_root();
            m["topic_name"] = description.swift_topic_name();
            if (!description.swift_start_timestamp().empty()) {
                m["swift_start_timestamp"] = description.swift_start_timestamp();
            }
        } else if (src == "odps") {
            m["type"] = "plugin";
            m["account_type"] = description.odps_account_type();
            m["access_id"] = description.odps_access_id();
            m["access_key"] = description.odps_access_key();
            m["odps_endpoint"] = description.odps_endpoint();
            m["project_name"] = description.odps_project_name();
            m["table_name"] = description.odps_table_name();
            m["partition_name"] = description.odps_partition_name();

        } else {
            CATALOG_CHECK(false,
                          Status::INVALID_ARGUMENTS,
                          "unsupported src [",
                          src,
                          "], [",
                          dataVersion.ShortDebugString(),
                          "]");
        }
        result.push_back(m);
    }
    *dataDescriptions = autil::legacy::ToJsonString(result);
    autil::StringUtil::replaceAll(*dataDescriptions, "\\/", "/");
    return StatusBuilder::ok();
}

} // namespace catalog
