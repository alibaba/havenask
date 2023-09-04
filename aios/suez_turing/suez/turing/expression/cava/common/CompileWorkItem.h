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
#include "autil/WorkItem.h"
#include "cava/codegen/CavaJitModule.h"
#include "cava/jit/CavaJitConfig.h"
#include "suez/turing/common/Closure.h"
#include "turing_ops_util/variant/Tracer.h"

namespace suez {
namespace turing {

class RuntimeState;
class CavaJitWrapper;

class SingleCompileWorkItem : public autil::WorkItem {
public:
    SingleCompileWorkItem(CavaJitWrapper *cavaJitWrapper,
                          const std::string &moduleName,
                          const std::string &fileName,
                          const std::string &code,
                          size_t hashKey,
                          const cava::CavaModuleOptions *options)
        : _cavaJitWrapper(cavaJitWrapper)
        , _moduleName(moduleName)
        , _fileName(fileName)
        , _code(code)
        , _hashKey(hashKey) {
        if (options != nullptr) {
            // copy and hold `options`
            _options.reset(new cava::CavaModuleOptions(*options));
        }
    }

private:
    SingleCompileWorkItem(const SingleCompileWorkItem &);
    SingleCompileWorkItem &operator=(const SingleCompileWorkItem &);

public:
    virtual void process() override;

private:
    CavaJitWrapper *_cavaJitWrapper;
    std::string _moduleName;
    std::string _fileName;
    std::string _code;
    size_t _hashKey;
    cava::CavaModuleOptionsPtr _options;
};

class CompileWorkItem : public autil::WorkItem {
public:
    CompileWorkItem(CavaJitWrapper *cavaJitWrapper,
                    const std::string &moduleName,
                    const std::vector<std::string> &fileNames,
                    const std::vector<std::string> &codes,
                    const size_t &hashKey,
                    suez::turing::Tracer *tracer,
                    cava::CavaJitModulesWrapper *cavaJitModulesWrapper,
                    ClosurePtr closure,
                    const cava::CavaModuleOptions *options)
        : _cavaJitWrapper(cavaJitWrapper)
        , _moduleName(moduleName)
        , _fileNames(fileNames)
        , _codes(codes)
        , _hashKey(hashKey)
        , _tracer(tracer)
        , _cavaJitModulesWrapper(cavaJitModulesWrapper)
        , _closure(closure) {
        if (options != nullptr) {
            // copy and hold `options`
            _options.reset(new cava::CavaModuleOptions(*options));
        }
    }
    ~CompileWorkItem() {}

private:
    CompileWorkItem(const CompileWorkItem &);
    CompileWorkItem &operator=(const CompileWorkItem &);

public:
    virtual void process() override;

private:
    CavaJitWrapper *_cavaJitWrapper;
    std::string _moduleName;
    std::vector<std::string> _fileNames;
    std::vector<std::string> _codes;
    size_t _hashKey;
    suez::turing::Tracer *_tracer;
    cava::CavaJitModulesWrapper *_cavaJitModulesWrapper;
    ClosurePtr _closure;
    cava::CavaModuleOptionsPtr _options;
};
} // namespace turing
} // namespace suez
