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
#include "build_service/io/IndexlibIndexInput.h"

#include <iosfwd>
#include <map>
#include <utility>

#include "alog/Logger.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/util/ErrorLogCollector.h"
#include "build_service/util/IndexPathConstructor.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/module_info.h"
#include "indexlib/partition/remote_access/partition_iterator.h"
#include "indexlib/partition/remote_access/partition_resource_provider_factory.h"

using namespace std;
using namespace build_service::util;
using namespace build_service::config;
using namespace indexlib::config;
using namespace indexlib::partition;

namespace build_service { namespace io {
BS_LOG_SETUP(io, IndexlibIndexInput);

const string IndexlibIndexInput::INDEX_ROOT = "input_index_root";
const string IndexlibIndexInput::PARTITION_RANGE_FROM = "partition_range_from";
const string IndexlibIndexInput::PARTITION_RANGE_TO = "partition_range_to";
const string IndexlibIndexInput::GENERATION_ID = "generation_id";
const string IndexlibIndexInput::CLUSTER_NAME = "cluster_name";
const string IndexlibIndexInput::INDEX_PARTITION_OPTIONS = "index_partition_options";
const string IndexlibIndexInput::PLUGIN_PATH = "plugin_path";

IndexlibIndexInput::IndexlibIndexInput(const config::TaskInputConfig& inputConfig) : Input(inputConfig) {}

IndexlibIndexInput::~IndexlibIndexInput() {}

bool IndexlibIndexInput::getIndexPath(const KeyValueMap& params, std::string& indexPath)
{
    string indexRoot = getValueFromKeyValueMap(params, INDEX_ROOT);
    if (indexRoot == EMPTY_STRING) {
        BS_LOG(ERROR, "create indelxib index input failed, no index_root!");
        return false;
    }
    string rangeFrom = getValueFromKeyValueMap(params, PARTITION_RANGE_FROM);
    if (rangeFrom == EMPTY_STRING) {
        BS_LOG(ERROR, "create indelxib index input failed, no partition_range_from!");
        return false;
    }

    string rangeTo = getValueFromKeyValueMap(params, PARTITION_RANGE_TO);
    if (rangeTo == EMPTY_STRING) {
        BS_LOG(ERROR, "create indelxib index input failed, no partition_range_to!");
        return false;
    }

    string generationId = getValueFromKeyValueMap(params, GENERATION_ID);
    if (generationId == EMPTY_STRING) {
        BS_LOG(ERROR, "create indelxib index input failed, no generation_id!");
        return false;
    }

    string clusterName = getValueFromKeyValueMap(params, CLUSTER_NAME);
    if (clusterName == EMPTY_STRING) {
        BS_LOG(ERROR, "create indelxib index input failed, no cluster_name!");
        return false;
    }

    indexPath = IndexPathConstructor::constructIndexPath(indexRoot, clusterName, generationId, rangeFrom, rangeTo);
    return true;
}

bool IndexlibIndexInput::init(const KeyValueMap& params)
{
    for (auto iter = params.begin(); iter != params.end(); iter++) {
        _inputConfig.addParameters(iter->first, iter->second);
    }

    const KeyValueMap& configParams = _inputConfig.getParameters();
    string indexPath;
    if (!getIndexPath(configParams, indexPath)) {
        return false;
    }
    IndexPartitionOptions options;
    string optionsStr = getValueFromKeyValueMap(configParams, INDEX_PARTITION_OPTIONS);
    if (optionsStr != EMPTY_STRING) {
        try {
            FromJsonString(options, optionsStr);
        } catch (const autil::legacy::ExceptionBase& e) {
            string errorMsg = "parse index_partition_options fail[" + optionsStr + "] failed.";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
    }

    string pluginPath = getValueFromKeyValueMap(configParams, PLUGIN_PATH);
    try {
        _provider = PartitionResourceProviderFactory::GetInstance()->CreateProvider(indexPath, options, pluginPath);
        if (!_provider) {
            BS_LOG(ERROR, "create index partition resource provider failed!");
            return false;
        }
        return true;
    } catch (const autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "create index partition resource provider fail, exception [%s]!", e.what());
        return false;
    }
    return false;
}

}} // namespace build_service::io
