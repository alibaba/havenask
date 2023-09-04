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

#include <algorithm>
#include <stddef.h>
#include <string>
#include <unistd.h>

#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"

namespace suez {
namespace turing {

constexpr char SUEZ_CAVA_CONF[] = "../binary/usr/local/etc/misc/suez_cava_config.json";
constexpr char SUEZ_CAVA_PLUGINS[] = "../binary/usr/local/share/cava/plugins";
constexpr size_t CAVA_ALLOC_SIZE_LIMIT = 40;  // 40 M
constexpr size_t CAVA_INIT_SIZE_LIMIT = 1024; // 1024 M
constexpr size_t CAVA_MODULE_CACHE_SIZE_DEFAULT = 100000;
constexpr double CAVA_COMPILE_THREAD_RATIO = 0;
constexpr double CAVA_COMPILE_THREAD_NUM = 1;
constexpr size_t CAVA_COMPILE_QUEUE_SIZE = 512;
constexpr size_t POOL_TRUNK_SIZE = 10;         // 10 M
constexpr size_t POOL_RECYCLE_SIZE_LIMIT = 20; // 20 M

class CavaConfig : public autil::legacy::Jsonizable {
public:
    CavaConfig()
        : _enable(false)
        , _enableQueryCode(false)
        , _cavaConf(SUEZ_CAVA_CONF)
        , _allocSizeLimit(CAVA_ALLOC_SIZE_LIMIT)           // default 40M
        , _initSizeLimit(CAVA_INIT_SIZE_LIMIT)             // default 1024M
        , _moduleCacheSize(CAVA_MODULE_CACHE_SIZE_DEFAULT) // default 100,000
        , _compileThreadRatio(CAVA_COMPILE_THREAD_RATIO)   // default 0
        , _compileThreadNum(CAVA_COMPILE_THREAD_NUM)       // default 1
        , _compileQueueSize(CAVA_COMPILE_QUEUE_SIZE)       // default 512
        , _poolTrunkSize(POOL_TRUNK_SIZE)                  // default 10M
        , _poolRecycleSizeLimit(POOL_RECYCLE_SIZE_LIMIT)   // default 20M
    {}

    ~CavaConfig() {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("enable", _enable, _enable);
        json.Jsonize("enable_query_code", _enableQueryCode, _enableQueryCode);
        json.Jsonize("cava_conf", _cavaConf, _cavaConf);
        json.Jsonize("lib_path", _libPath, _libPath);
        json.Jsonize("source_path", _sourcePath, _sourcePath);
        json.Jsonize("code_cache_path", _codeCachePath, _codeCachePath);
        json.Jsonize("alloc_size_limit", _allocSizeLimit, _allocSizeLimit);
        json.Jsonize("init_size_limit", _initSizeLimit, _initSizeLimit);
        json.Jsonize("module_cache_size", _moduleCacheSize, _moduleCacheSize);
        json.Jsonize("compile_thread_ratio", _compileThreadRatio, _compileThreadRatio);
        json.Jsonize("compile_thread_num", _compileThreadNum, _compileThreadNum);
        json.Jsonize("compile_queue_size", _compileQueueSize, _compileQueueSize);
        json.Jsonize("pool_trunk_size", _poolTrunkSize, _poolTrunkSize);
        json.Jsonize("pool_recycle_size_limit", _poolRecycleSizeLimit, _poolRecycleSizeLimit);
        json.Jsonize("auto_register_function_packages", _autoRegFuncPkgs, _autoRegFuncPkgs);
    }

    size_t getCompileThreadNum() {
        return std::max(_compileThreadNum, size_t(sysconf(_SC_NPROCESSORS_ONLN) * _compileThreadRatio));
    }

    autil::legacy::json::JsonMap getJsonMap() {
        autil::legacy::Jsonizable::JsonWrapper json;
        Jsonize(json);
        return json.GetMap();
    }

public:
    bool _enable;
    bool _enableQueryCode;
    std::string _cavaConf;
    std::string _libPath;
    std::string _sourcePath;
    std::string _codeCachePath;
    size_t _allocSizeLimit; // M
    size_t _initSizeLimit;  // M
    size_t _moduleCacheSize;
    double _compileThreadRatio;
    size_t _compileThreadNum;
    size_t _compileQueueSize;
    size_t _poolTrunkSize;        // M
    size_t _poolRecycleSizeLimit; // M
    std::vector<std::string> _autoRegFuncPkgs;
};
} // namespace turing
} // namespace suez
