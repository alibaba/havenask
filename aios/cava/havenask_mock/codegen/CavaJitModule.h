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

#include <llvm/ExecutionEngine/JITSymbol.h>
#include <string>

#include "autil/Lock.h"


namespace cava {
class CavaJit;
class CavaModule;

class CavaJitModule {
public:
    CavaJitModule(CavaModule *cavaModule, CavaJit *cavaJit, bool needRemove)
        : _cavaModule(cavaModule), _cavaJit(cavaJit), _needRemove(needRemove) {}
    llvm::JITEvaluatedSymbol findSymbol(const std::string &name) const;
    ~CavaJitModule() {
        
    }

private:
    void releaseCavaModuleUnsafe();

public:
    CavaModule *_cavaModule = nullptr;
    CavaJit *_cavaJit = nullptr;
    bool _needRemove;
};

typedef std::shared_ptr<CavaJitModule> CavaJitModulePtr;

class CavaJitModulesWrapper {
public:
    typedef std::map<size_t, cava::CavaJitModulePtr> CavaJitModuleMap;

public:
    void addCavaJitModule(size_t hashKey, cava::CavaJitModulePtr &cavaJitModule);
    cava::CavaJitModulePtr getCavaJitModule(size_t hashKey) const;
    cava::CavaJitModulePtr getCavaJitModule() const;
    const CavaJitModulesWrapper::CavaJitModuleMap &getCavaJitModules() const;
    void clear();

private:
    mutable autil::ThreadMutex _mutex;
    CavaJitModuleMap _cavaJitModules;
};
} // end namespace cava
