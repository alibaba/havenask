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
#include "suez/turing/expression/cava/common/CavaConfig.h"

namespace suez_navi {

struct CavaConfig : public suez::turing::CavaConfig {
public:
    using suez::turing::CavaConfig::Jsonize;
    void Jsonize(navi::NaviConfigContext &ctx) {
        ctx.Jsonize("enable", _enable, _enable);
        ctx.Jsonize("enable_query_code", _enableQueryCode, _enableQueryCode);
        ctx.Jsonize("cava_conf", _cavaConf, _cavaConf);
        ctx.Jsonize("lib_path", _libPath, _libPath);
        ctx.Jsonize("source_path", _sourcePath, _sourcePath);
        ctx.Jsonize("code_cache_path", _codeCachePath, _codeCachePath);
        ctx.Jsonize("alloc_size_limit", _allocSizeLimit, _allocSizeLimit);
        ctx.Jsonize("init_size_limit", _initSizeLimit, _initSizeLimit);
        ctx.Jsonize("module_cache_size", _moduleCacheSize, _moduleCacheSize);
        ctx.Jsonize("compile_thread_ratio", _compileThreadRatio, _compileThreadRatio);
        ctx.Jsonize("compile_thread_num", _compileThreadNum, _compileThreadNum);
        ctx.Jsonize("compile_queue_size", _compileQueueSize, _compileQueueSize);
        ctx.Jsonize("pool_trunk_size", _poolTrunkSize, _poolTrunkSize);
        ctx.Jsonize("pool_recycle_size_limit", _poolRecycleSizeLimit, _poolRecycleSizeLimit);
        ctx.Jsonize("auto_register_function_packages", _autoRegFuncPkgs, _autoRegFuncPkgs);
    }
};

}
