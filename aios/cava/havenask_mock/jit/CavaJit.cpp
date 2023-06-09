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
#include <LLVMContext.h>
#include <Module.h>
#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <llvm/ADT/DenseMap.h>
#include <llvm/ADT/DenseSet.h>
#include <llvm/ADT/StringRef.h>
#include <llvm/ExecutionEngine/JITSymbol.h>
#include <llvm/ExecutionEngine/Orc/JITTargetMachineBuilder.h>
#include <llvm/ExecutionEngine/Orc/SymbolStringPool.h>
#include <llvm/ExecutionEngine/Orc/ThreadSafeModule.h>
#include <llvm/MC/MCTargetOptions.h>
#include <llvm/Support/CodeGen.h>
#include <llvm/Support/DynamicLibrary.h>
#include <llvm/Support/raw_ostream.h>
#include <string.h>
#include <sys/stat.h>
#include <functional>
#include <iosfwd>
#include <iterator>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/jsonizable.h"
#include "cava/codegen/CavaModule.h"
#include "cava/jit/CavaJit.h"
#include "cava/common/Common.h"

#include "llvm/ExecutionEngine/Orc/Core.h"
#include "llvm/Support/Error.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Target/TargetOptions.h"
#include "fslib/util/FileUtil.h"
using namespace std;
using namespace cava;
using namespace llvm::orc;
using namespace fslib::util;


bool CavaJit::globalInit() {
    return true;
}

bool CavaJit::initExternalLibraries(const std::vector<std::string> &searchPaths) {
    return false;
}

bool CavaJit::init(const std::vector<std::string> &searchPaths, const std::string &jitConfig, bool fastCompile) {
    return false;
}

bool CavaJit::unloadCavaModule(CavaModule *cavaModule) {
    return false;
}


void CavaJit::unInit() {
}

llvm::Expected<CavaModule*> CavaJit::createCavaModule(const std::string &moduleName,
                                      PrepareSource prepareSource,
                                      bool isLib,
                                      const CavaModuleOptions *options)
{
    return nullptr;
}

llvm::Expected<CavaModule*> CavaJit::createCavaModule(
        const std::string &moduleName,
        const std::string &sourcePath,
        bool isLib,
        const CavaModuleOptions *options)
{
    return nullptr;
}

llvm::Expected<CavaModule*> CavaJit::createCavaModule(const std::string &moduleName,
                                      const std::vector<std::string> &pathList,
                                      bool isLib,
                                      const CavaModuleOptions *options)
{
    return nullptr;
}


llvm::Expected<CavaModule*> CavaJit::createCavaModule(const std::string &moduleName,
                                      const std::vector<std::string> &sourceFileNames,
                                      const std::vector<std::string> &sourceContents,
                                      bool isLib,
                                      const CavaModuleOptions *options)
{
    return nullptr;
}

bool CavaJit::removeCavaModule(CavaModule *cavaModule) {
    return false;
}


string CavaJit::getCompileError() const {
    return "";
}


bool CavaJit::globSourceFile(const std::string &dirName,
                      std::vector<std::string> &fileList) {
    return false;
}
