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

#include <llvm/Support/Error.h>
#include <stddef.h>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "cava/codegen/CavaModule.h"
#include "cava/common/Common.h"

#include "cava/jit/CavaJitConfig.h"
#include "llvm/ExecutionEngine/JITSymbol.h"
#include "llvm/ExecutionEngine/Orc/JITTargetMachineBuilder.h"
#include "llvm/ExecutionEngine/Orc/ThreadSafeModule.h"

namespace cava {


class CavaJit
{
public:
    CavaJit() {

    }
    ~CavaJit() {
        
    }
private:
    CavaJit(const CavaJit &);
    CavaJit& operator=(CavaJit &);
public:
    static bool globalInit();
    bool init(const std::vector<std::string> &searchPaths, const std::string &jitConfig, bool fastCompile = false);
    void unInit();
public:
    llvm::Expected<CavaModule*> createCavaModule(
            const std::string &moduleName,
            const std::string &sourcePath,
            bool isLib = true,
            const CavaModuleOptions *options = nullptr);
    llvm::Expected<CavaModule*> createCavaModule(
            const std::string &moduleName,
            const std::vector<std::string> &pathList,
            bool isLib = true,
            const CavaModuleOptions *options = nullptr);
    llvm::Expected<CavaModule*> createCavaModule(
            const std::string &moduleName,
            const std::vector<std::string> &sourceFileNames,
            const std::vector<std::string> &sourceContents,
            bool isLib = true,
            const CavaModuleOptions *options = nullptr);
    typedef std::function<bool()> PrepareSource;
    llvm::Expected<CavaModule*> createCavaModule(
            const std::string &moduleName,
            PrepareSource prepareSource,
            bool isLib = true,
            const CavaModuleOptions *options = nullptr);
    bool removeCavaModule(CavaModule *cavaModule);
private:
    template <typename T> static std::vector<T> singletonSet(T t) {
        std::vector<T> Vec;
        Vec.push_back(t);
        return Vec;
    }
    bool unloadCavaModule(CavaModule *cavaModule);
    bool initExternalLibraries(const std::vector<std::string> &searchPaths);
    std::string getCompileError() const;
public:
    static bool globSourceFile(const std::string &dirName,
                               std::vector<std::string> &fileList);
private:
    static llvm::orc::JITTargetMachineBuilder createTargetMachine();
    ;
};

CAVA_TYPEDEF_PTR(CavaJit);

} // end namespace cava
