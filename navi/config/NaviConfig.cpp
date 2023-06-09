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
#include "navi/config/NaviConfig.h"
#include "autil/legacy/jsonizable_exception.h"
#include "rapidjson/prettywriter.h"
#include "navi/log/NaviLogger.h"
#include "rapidjson/error/en.h"
namespace navi {

NaviBizConfig::NaviBizConfig()
    : partCount(INVALID_NAVI_PART_COUNT)
{
}

void NaviBizConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("part_count", partCount);
    json.Jsonize("part_ids", partIds);
    json.Jsonize("config_path", configPath, configPath);
    json.Jsonize("meta_info", metaInfo, metaInfo);
    json.Jsonize("init_resource", initResources, initResources);
    json.Jsonize("graph_config", graphConfigMap, graphConfigMap);
    json.Jsonize("kernel_config", kernelConfigVec, kernelConfigVec);
    json.Jsonize("resource_config", resourceConfigVec, resourceConfigVec);
}

ConcurrencyConfig::ConcurrencyConfig()
    : threadNum(DEFAULT_THREAD_NUMBER)
    , minThreadNum(DEFAULT_THREAD_NUMBER)
    , maxThreadNum(DEFAULT_THREAD_NUMBER)
    , queueSize(DEFAULT_QUEUE_SIZE)
    , processingSize(DEFAULT_PROCESSING_SIZE)
{}

ConcurrencyConfig::ConcurrencyConfig(int threadNum_, size_t queueSize_, size_t processingSize_)
    : threadNum(threadNum_)
    , minThreadNum(DEFAULT_THREAD_NUMBER)
    , maxThreadNum(DEFAULT_THREAD_NUMBER)
    , queueSize(queueSize_)
    , processingSize(processingSize_)
{}

void ConcurrencyConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("thread_num", threadNum, threadNum);
    json.Jsonize("min_thread_num", minThreadNum, minThreadNum);
    json.Jsonize("max_thread_num", maxThreadNum, maxThreadNum);
    json.Jsonize("queue_size", queueSize, queueSize);
    json.Jsonize("processing_size", processingSize, processingSize);
}

EngineConfig::EngineConfig()
    : disablePerf(false)
    , disableSymbolTable(false)
{
}

void EngineConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("disable_perf", disablePerf, disablePerf);
    json.Jsonize("disable_symbol_table", disableSymbolTable, disableSymbolTable);
    json.Jsonize("builtin_task_queue", builtinTaskQueue, builtinTaskQueue);
    json.Jsonize("extra_task_queue", extraTaskQueueMap, extraTaskQueueMap);
}

NaviConfig::NaviConfig() {
}

NaviConfig::~NaviConfig() {
}

bool NaviConfig::loadConfig(const std::string &jsonStr) {
    auto paddedConfigStr = jsonStr + SIMD_PADING_STR;
    document.Parse(paddedConfigStr.c_str());
    if (document.HasParseError()) {
        NAVI_KERNEL_LOG(ERROR, "invalid navi config [%s], error [%s (%lu)]",
                        jsonStr.c_str(),
                        rapidjson::GetParseError_En(document.GetParseError()),
                        document.GetErrorOffset());
        return false;
    }
    try {
        autil::legacy::FromRapidValue(*this, document);
    } catch (const autil::legacy::ExceptionBase &e) {
        NAVI_KERNEL_LOG(ERROR, "invalid config [%s], msg [%s]", jsonStr.c_str(),
                        e.GetMessage().c_str());
        return false;
    }
    configStr = jsonStr;
    return true;
}

bool NaviConfig::dumpToStr(std::string &configStr) const {
    try {
        auto compactJson = FastToJsonString(*this);
        autil::legacy::RapidDocument prettyDoc;
        auto ret = parseToDocument(std::move(compactJson), prettyDoc);
        assert(ret);
        (void)ret;
        rapidjson::StringBuffer buffer;
        rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(
            buffer);
        prettyDoc.Accept(writer);
        configStr.assign(buffer.GetString(), buffer.GetSize());
    } catch (const autil::legacy::ExceptionBase &e) {
        NAVI_KERNEL_LOG(ERROR, "convert config to json failed");
        return false;
    }
    return true;
}

void NaviConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("config_path", configPath, configPath);
    json.Jsonize("modules", modules, modules);
    json.Jsonize("log_config", logConfig, logConfig);
    json.Jsonize("engine_config", engineConfig, engineConfig);
    json.Jsonize("biz_config", bizMap, bizMap);
    json.Jsonize("buildin_resource_config", buildinResourceConfigVec,
                 buildinResourceConfigVec);
    json.Jsonize("buildin_init_resource", buildinInitResources, buildinInitResources);
}

bool NaviConfig::parseToDocument(std::string jsonStr,
                                 autil::legacy::RapidDocument &document)
{
    if (jsonStr.empty()) {
        jsonStr = "{}";
    }
    jsonStr += SIMD_PADING_STR;
    document.Parse(jsonStr.c_str());
    if (document.HasParseError()) {
        NAVI_KERNEL_LOG(ERROR, "invalid json, error [%s (%lu)], json: %s",
                        rapidjson::GetParseError_En(document.GetParseError()),
                        document.GetErrorOffset(), jsonStr.c_str());
        return false;
    }
    return true;
}
}
