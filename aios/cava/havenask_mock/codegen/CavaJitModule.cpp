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
#include "cava/codegen/CavaJitModule.h"
#include <iosfwd>
#include "cava/common/Common.h"
#include "cava/jit/CavaJit.h"

using namespace std;
using namespace cava;
using namespace autil;


llvm::JITEvaluatedSymbol CavaJitModule::findSymbol(const std::string &name) const
{
    llvm::JITEvaluatedSymbol symbol;
    return symbol;
}

void CavaJitModulesWrapper::addCavaJitModule(size_t hashKey, cava::CavaJitModulePtr &cavaJitModule) {
}

cava::CavaJitModulePtr CavaJitModulesWrapper::getCavaJitModule(size_t hashKey) const {
    return nullptr;
}

cava::CavaJitModulePtr CavaJitModulesWrapper::getCavaJitModule() const {
    return nullptr;
}

const CavaJitModulesWrapper::CavaJitModuleMap &CavaJitModulesWrapper::getCavaJitModules() const {
    return _cavaJitModules;
}

void CavaJitModulesWrapper::clear() {
}
