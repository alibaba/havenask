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
#ifndef __INDEXLIB_CODEGEN_OBJECT_H
#define __INDEXLIB_CODEGEN_OBJECT_H

#include <memory>

#include "indexlib/codegen/code_factory.h"
#include "indexlib/codegen/codegen_checker.h"
#include "indexlib/codegen/codegen_info.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/plugin/DllWrapper.h"

namespace indexlib { namespace codegen {
#define COMBINE1(x, y) x##y
#define COMBINE(x, y) COMBINE1(x, y)

#define NORMALIZE(line)                                                                                                \
    static char END_CHAR = '\0';                                                                                       \
    if (unlikely(END_CHAR == '\0')) {                                                                                  \
        END_CHAR = __FILE__[strlen(__FILE__) - 1];                                                                     \
    }                                                                                                                  \
    uint64_t identifierKey = END_CHAR;                                                                                 \
    identifierKey = identifierKey << 56;                                                                               \
    identifierKey = identifierKey | (uint64_t)line;

#define CREATE_OBJECT(funcType, className, args...)                                                                    \
    {                                                                                                                  \
        codegen::CodegenObject* ret = NULL;                                                                            \
        collectAllCode();                                                                                              \
        markInterface();                                                                                               \
        if (!mCodegenInfo.IsInterface()) {                                                                             \
            std::cerr << "create codegen object only support interface class" << std::endl;                            \
            assert(false);                                                                                             \
            return ret;                                                                                                \
        }                                                                                                              \
        auto allCodegenInfo = getAllCodegenInfo();                                                                     \
        auto codegenHint = codegen::CodeFactory::getCodegenHint(allCodegenInfo);                                       \
        if (codegenHint) {                                                                                             \
            NORMALIZE(__LINE__);                                                                                       \
            typedef funcType COMBINE(CREATE_, className);                                                              \
            auto fun = codegenHint->getFunction(identifierKey);                                                        \
            funcType* actualFun = NULL;                                                                                \
            if (!fun) {                                                                                                \
                std::string name("CREATE_" #className);                                                                \
                actualFun = (COMBINE(CREATE_, className)*)codegenHint->_handler->dlsym(name);                          \
                if (actualFun) {                                                                                       \
                    codegenHint->registFunction(identifierKey, (void*)actualFun);                                      \
                }                                                                                                      \
            } else {                                                                                                   \
                actualFun = (COMBINE(CREATE_, className)*)fun;                                                         \
            }                                                                                                          \
            if (actualFun) {                                                                                           \
                ret = actualFun(args);                                                                                 \
                if (ret) {                                                                                             \
                    ret->initCodegenInfos(mCodegenInfo, codegenHint);                                                  \
                }                                                                                                      \
            }                                                                                                          \
        }                                                                                                              \
        return ret;                                                                                                    \
    }

#define CREATE_CODEGEN_OBJECT(ret, funcType, className, args...)                                                       \
    do {                                                                                                               \
        collectAllCode();                                                                                              \
        markInterface();                                                                                               \
        if (!mCodegenInfo.IsInterface()) {                                                                             \
            ret = nullptr;                                                                                             \
            std::cerr << "create codegen object only support interface class" << std::endl;                            \
            assert(false);                                                                                             \
            break;                                                                                                     \
        }                                                                                                              \
        auto allCodegenInfo = getAllCodegenInfo();                                                                     \
        auto codegenHint = codegen::CodeFactory::getCodegenHint(allCodegenInfo);                                       \
        if (codegenHint) {                                                                                             \
            NORMALIZE(__LINE__);                                                                                       \
            typedef funcType COMBINE(CREATE_, className);                                                              \
            auto fun = codegenHint->getFunction(identifierKey);                                                        \
            funcType* actualFun = NULL;                                                                                \
            if (!fun) {                                                                                                \
                std::string name("CREATE_" #className);                                                                \
                actualFun = (COMBINE(CREATE_, className)*)codegenHint->_handler->dlsym(name);                          \
                if (actualFun) {                                                                                       \
                    codegenHint->registFunction(identifierKey, (void*)actualFun);                                      \
                }                                                                                                      \
            } else {                                                                                                   \
                actualFun = (COMBINE(CREATE_, className)*)fun;                                                         \
            }                                                                                                          \
            if (actualFun) {                                                                                           \
                ret = actualFun(args);                                                                                 \
                if (ret) {                                                                                             \
                    ret->initCodegenInfos(mCodegenInfo, mCodegenHint);                                                 \
                }                                                                                                      \
            }                                                                                                          \
        }                                                                                                              \
    } while (0)

#define COLLECT_CONST_MEM_VAR(var)                                                                                     \
    {                                                                                                                  \
        std::string varName(#var);                                                                                     \
        std::string value = autil::StringUtil::toString(var);                                                          \
        mCodegenInfo.DefineConstMemberVar(varName, value);                                                             \
    }

#define COLLECT_TYPE_REDEFINE(typeName, targetType)                                                                    \
    {                                                                                                                  \
        std::string typeName(#typeName);                                                                               \
        std::string value(targetType);                                                                                 \
        mCodegenInfo.DefineTypeRedefine(typeName, value);                                                              \
    }

#define INIT_CODEGEN_INFO(originClassName, needRename)                                                                 \
    {                                                                                                                  \
        std::string className(#originClassName);                                                                       \
        mCodegenInfo.Init(std::string(__FILE__).c_str() + strlen("aios/storage/indexlib/"), className, needRename);    \
    }

#define PARTICIPATE_CODEGEN(className, codegenInfo)                                                                    \
    {                                                                                                                  \
        std::string classNameStr(#className);                                                                          \
        codegenInfo.Init(std::string(__FILE__).c_str() + strlen("aios/storage/indexlib/"), classNameStr, false);       \
    }

class CodegenObject
{
public:
    CodegenObject();
    virtual ~CodegenObject();

public:
    std::string getTargetClassName() { return mCodegenInfo.GetTargetClassName(); }
    std::string getSourceCodePath()
    {
        if (mCodegenHint) {
            return mCodegenHint->getSourceCodePath();
        }
        return std::string();
    }
    bool collectAllCode();
    void initCodegenInfos(CodegenInfo codegenInfo, CodegenHintPtr codegenHint)
    {
        mCodegenInfo = codegenInfo;
        mCodegenHint = codegenHint;
    }
    std::string getSourceClassName() { return mCodegenInfo.GetSourceClassName(); }

public:
    // public for test
    typedef std::map<std::string, CodegenCheckerPtr> CodegenCheckers;
    virtual void TEST_collectCodegenResult(CodegenCheckers& checker, std::string id = "") {}

protected:
    const std::string& getCodeHash();
    void combineCodegenInfos(CodegenObject* other);
    void combineCodegenInfos(const AllCodegenInfo& codegenInfos)
    {
        mSubCodegenInfos.insert(mSubCodegenInfos.end(), codegenInfos.allCodegenInfos.begin(),
                                codegenInfos.allCodegenInfos.end());
    }
    AllCodegenInfo getAllCodegenInfo();
    void markInterface() { mCodegenInfo.MarkInterface(); }

private:
    std::string getTargetFunctionCall(std::string funcName, int32_t lineNum) const;

protected:
    std::string getAllCollectCode();
    virtual bool doCollectAllCode() = 0;

protected:
    std::string mCodeHash;
    CodegenInfo mCodegenInfo;
    std::vector<CodegenInfo> mSubCodegenInfos;
    CodegenHintPtr mCodegenHint;

private:
    friend class CodegenObjectTest;
    friend class CodeFactoryTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CodegenObject);
}} // namespace indexlib::codegen

#endif //__INDEXLIB_CODEGEN_OBJECT_H
