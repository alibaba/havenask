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
#include "indexlib/codegen/codegen_info.h"

#include "autil/StringUtil.h"
#include "indexlib/codegen/code_factory.h"
#include "indexlib/codegen/codegen_config.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/KeyHasherTyped.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;
using namespace indexlib::file_system;

namespace indexlib { namespace codegen {
IE_LOG_SETUP(codegen, CodegenInfo);

CodegenInfo::CodegenInfo() : mIsInterfaceClass(false) {}

CodegenInfo::~CodegenInfo() {}

std::string CodegenInfo::NormalizeSourceFile(const std::string& sourceFile)
{
    auto pos = sourceFile.rfind("indexlib/");
    return sourceFile.substr(pos);
    // todo: for normal
    //    size_t start = 0;
    //     auto pos = sourceFile.find("build/");
    //     if (pos != string::npos) {
    //         start = 2;
    //     } else {
    //         pos = sourceFile.find("./");
    //         if (pos != string::npos) {
    //             start = 1;
    //         }
    //     }
    //     vector<string> tmp;
    //     StringUtil::fromString(sourceFile, tmp, "/");
    //     vector<string> targetPaths;
    //     for (size_t i = start; i < tmp.size(); i++) {
    //         targetPaths.push_back(tmp[i]);
    //     }
    //     return StringUtil::toString(targetPaths, "/");
    // }
}

void CodegenInfo::Init(const std::string& sourceFileName, const std::string& sourceClassName, bool needRename)
{
    mSourceFile = NormalizeSourceFile(sourceFileName);
    mSourceClassName = sourceClassName;
    mTargetClassName = mSourceClassName;
    mNeedRename = needRename;
}

bool CodegenInfo::GetCppFile(const std::string& sourcePath, std::string& cppFile)
{
    vector<string> filePaths;
    StringUtil::fromString(mSourceFile, filePaths, ".");
    assert(filePaths.size() == 2);
    if (filePaths[1] != "cpp") {
        filePaths[1] = "cpp";
    }
    string tmpCppFile = StringUtil::toString(filePaths, ".");
    string targetCppFile = FslibWrapper::JoinPath(sourcePath, tmpCppFile);
    if (FslibWrapper::IsExist(targetCppFile).GetOrThrow()) {
        cppFile = targetCppFile;
    }
    return true;
}

void CodegenInfo::DefineConstMemberVar(const std::string& varName, const std::string& value)
{
    mConstMemberVars[varName] = value;
}

void CodegenInfo::DefineTypeRedefine(const std::string& typedefName, const std::string& typedefType)
{
    mTypeRedefines[typedefName] = typedefType;
}

void CodegenInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("sourceFile", mSourceFile);
    json.Jsonize("sourceClassName", mSourceClassName);
    json.Jsonize("constMemberVars", mConstMemberVars);
    json.Jsonize("typeRedefines", mTypeRedefines);
    json.Jsonize("targetClassName", mTargetClassName);
    json.Jsonize("isInterface", mIsInterfaceClass);
}

std::string CodegenInfo::GetTargetClassName() const { return mTargetClassName; }

std::string CodegenInfo::CalculateCodeHash(const std::string& code)
{
    StringView key(code);
    dictkey_t hashKey;
    util::MurmurHasher::GetHashKey(key, hashKey);
    return StringUtil::toString(hashKey);
}

AllCodegenInfo::AllCodegenInfo() : compileParallel(0)
{
    auto info = codegen::CodeFactory::getCompileInfo();
    compileParallel = info.parallel;
    compiler = info.compiler;
    buildOption = info.compileOption;
    codegenVersion = INDEXLIB_CODEGEN_VERSION;
}

void AllCodegenInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON) {
        if (codeHash.empty()) {
            string allCodeStr = ToJsonString(allCodegenInfos);
            codeHash = CodegenInfo::CalculateCodeHash(allCodeStr);
        }
    }
    json.Jsonize("CodegenFiles", allCodegenInfos);
    json.Jsonize("CodeHash", codeHash);
    json.Jsonize("BuildOption", buildOption);
    json.Jsonize("Job", compileParallel);
    json.Jsonize("Compiler", compiler);
    json.Jsonize("CodegenVersion", codegenVersion);
}

const std::string& AllCodegenInfo::GetCodeHash()
{
    string allCodeStr = ToJsonString(allCodegenInfos);
    codeHash = CodegenInfo::CalculateCodeHash(allCodeStr);
    return codeHash;
}
}} // namespace indexlib::codegen
