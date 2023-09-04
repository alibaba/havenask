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

#include <string>

#include "autil/legacy/jsonizable.h"

namespace suez {
namespace turing {

class TurboJetConfig : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("legacy", legacy, legacy);
        json.Jsonize("import_pkg_paths", importPkgPaths, importPkgPaths);
    }

    static TurboJetConfig legacyConfig() {
        TurboJetConfig conf;
        conf.legacy = true;
        return conf;
    }

public:
    bool legacy{false};
    std::vector<std::string> importPkgPaths;
};

class PluginConfig : public autil::legacy::Jsonizable {
public:
    PluginConfig() {}
    ~PluginConfig() {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("rank_conf", rankConf, rankConf);
        json.Jsonize("sorter_conf", sorterConf, sorterConf);
        json.Jsonize("function_conf", functionConf, functionConf);
        json.Jsonize("turbojet_conf", turbojetConf, turbojetConf);
        // deprecated config entries
        json.Jsonize("tj_cpp_func_conf", tjCppFuncConf, tjCppFuncConf);
        json.Jsonize("tj_cpp_shared_library_paths", tjCppSharedLibraryPaths, tjCppSharedLibraryPaths);
        json.Jsonize("tj_java_func_conf", tjJavaFuncConf, tjJavaFuncConf);
        json.Jsonize("tj_extra_include_paths", tjExtraIncludePaths, tjExtraIncludePaths);
    }

public:
    std::string rankConf;
    std::string sorterConf;
    std::string functionConf;
    // set legacy flag to preserve compatibility
    TurboJetConfig turbojetConf{TurboJetConfig::legacyConfig()};
    // deprecated config entries
    std::vector<std::string> tjCppFuncConf;
    std::vector<std::pair<std::string, std::string>> tjJavaFuncConf;
    std::vector<std::string> tjExtraIncludePaths;
    std::vector<std::string> tjCppSharedLibraryPaths;
};

} // namespace turing
} // namespace suez
