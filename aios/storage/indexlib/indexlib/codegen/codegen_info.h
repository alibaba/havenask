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
#ifndef __INDEXLIB_CODEGEN_INFO_H
#define __INDEXLIB_CODEGEN_INFO_H

#include <memory>

#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace codegen {

#define STATIC_COLLECT_TYPE_REDEFINE(typeName, targetType, codegenInfo)                                                \
    {                                                                                                                  \
        std::string typeName(#typeName);                                                                               \
        std::string value(targetType);                                                                                 \
        codegenInfo.DefineTypeRedefine(typeName, value);                                                               \
    }

#define STATIC_INIT_CODEGEN_INFO(originClassName, needRename, codegenInfo)                                             \
    {                                                                                                                  \
        std::string className(#originClassName);                                                                       \
        codegenInfo.Init(__FILE__, className, needRename);                                                             \
    }

#define STATIC_COLLECT_CONST_MEM_VAR(varName, value, codegeninfo)                                                      \
    {                                                                                                                  \
        codegenInfo.DefineConstMemberVar(#varName, autil::StringUtil::toString(value));                                \
    }

class CodegenInfo : public autil::legacy::Jsonizable
{
public:
    CodegenInfo();
    ~CodegenInfo();

public:
    static std::string CalculateCodeHash(const std::string& code);

public:
    void Init(const std::string& sourceFileName, const std::string& sourceClassName, bool needRename);

    void DefineConstMemberVar(const std::string& varName, const std::string& value);
    void DefineTypeRedefine(const std::string& varName, const std::string& value);

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    std::string GetSourceFile() { return mSourceFile; }
    bool GetCppFile(const std::string& sourcePath, std::string& cppFile);
    std::string GetSourceClassName() { return mSourceClassName; }
    std::string GetTargetClassName() const;
    void Rename(const std::string& hashCode)
    {
        if (mNeedRename) {
            mTargetClassName = mSourceClassName + "_" + hashCode;
        }
    }
    void MarkInterface() { mIsInterfaceClass = true; }
    bool IsInterface() { return mIsInterfaceClass; }

private:
    std::string NormalizeSourceFile(const std::string& sourceFile);
    std::string GetHashCode();

private:
    typedef std::map<std::string, std::string> KVMap;

private:
    std::string mSourceFile;
    std::string mSourceClassName;
    std::string mTargetClassName;
    KVMap mConstMemberVars;
    KVMap mTypeRedefines;
    bool mNeedRename;
    bool mIsInterfaceClass;

private:
    IE_LOG_DECLARE();
};

struct AllCodegenInfo : public autil::legacy::Jsonizable {
    std::vector<CodegenInfo> allCodegenInfos;
    std::string codeHash;
    std::string buildOption;
    std::string compiler;
    std::string codegenVersion;
    uint64_t compileParallel;
    AllCodegenInfo();

    const std::string& GetCodeHash();
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    std::string GetTargetClassName(const std::string& sourceClassName)
    {
        for (auto codegenInfo : allCodegenInfos) {
            if (codegenInfo.GetSourceClassName() == sourceClassName) {
                return codegenInfo.GetTargetClassName();
            }
        }
        assert(false);
        return std::string();
    }
    void AppendAndRename(CodegenInfo codegenInfo)
    {
        allCodegenInfos.push_back(codegenInfo);
        auto codeHash = GetCodeHash();
        allCodegenInfos.back().Rename(codeHash);
    }
};

DEFINE_SHARED_PTR(CodegenInfo);
}} // namespace indexlib::codegen

#endif //__INDEXLIB_CODEGEN_INFO_H
