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
#ifndef __INDEXLIB_CODE_FACTORY_H
#define __INDEXLIB_CODE_FACTORY_H

#include <memory>

#include "autil/Lock.h"
#include "indexlib/codegen/codegen_hint.h"
#include "indexlib/codegen/codegen_info.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Singleton.h"

namespace indexlib { namespace codegen {

class CodeFactory : public util::Singleton<CodeFactory>
{
public:
    CodeFactory();
    ~CodeFactory();

public:
    struct CompileInfo {
        std::string compileOption;
        std::string compiler;
        uint64_t parallel;
    };
    static CodegenHintPtr getCodegenHint(AllCodegenInfo& allCodegenInfo);
    static CompileInfo getCompileInfo();
    static bool generateCode(AllCodegenInfo& allCodegenInfo);
    static bool launchProc(const std::string& name, const std::string& cmd);
    static void preloadSo();
    static std::string getSourceCodePath(const std::string& codeHash);

    CompileInfo doGetCompileInfo();
    bool doGenerateCode(AllCodegenInfo& allCodegenInfo);
    CodegenHintPtr doGetCodegenHint(AllCodegenInfo& allCodegenInfo);
    void doPreloadSo();
    std::string doGetSourceCodePath(const std::string& codeHash);
    void reinit()
    {
        autil::ScopedLock lock(mCodeGenerateLock); // one time one so to prepare
        mHints.clear();
        mIsReady = init();
    }

private:
    bool init();
    static plugin::DllWrapperPtr loadSo(const std::string& path);
    std::string StoreInfoFile(AllCodegenInfo& allCodegenInfo);

    static int vsystem(const char* cmd);
    bool getCurrentPath(std::string& path);
    void cleanUnusedSo();
    bool prepareSo(AllCodegenInfo& allCodegenInfo);
    std::string getCodePath(const std::string& codeHash)
    {
        return file_system::FslibWrapper::JoinPath(mWorkDir, codeHash);
    }

    void StoreInfoFile(AllCodegenInfo& allCodegenInfo, const std::string& codeInfoFile)
    {
        std::string codegenInfoStr = ToJsonString(allCodegenInfo);
        THROW_IF_FS_ERROR(file_system::FslibWrapper::AtomicStore(codeInfoFile, codegenInfoStr, true).Code(), "path[%s]",
                          codeInfoFile.c_str());
    }

    std::string getCodegenInfoPath(const std::string& codeHash)
    {
        return file_system::FslibWrapper::JoinPath(getCodePath(codeHash), "codegen_info");
    }

    std::string getTargetSoPath(const std::string& codeHash)
    {
        std::string targetSoPath = getCodePath(codeHash);
        return file_system::FslibWrapper::JoinPath(targetSoPath, codeHash + "_codegen.so");
    }
    bool compileSoInLocal(AllCodegenInfo& codegenInfos);
    CodegenHintPtr getHintFromCache(const std::string& codeHash);

private:
    static const std::string COMPILE_FLAGS;
    const int64_t DEFAULT_CLEAN_INTERVAL = 3600 * 24 * 365 * 10; // not clean
    // TODO: string to int64 hash
    CompileInfo mCompileInfo;
    std::map<std::string, CodegenHintPtr> mHints;
    std::string mWorkDir;
    std::string mInstallRoot;
    std::string mPreCompiledSoPath;
    bool mIsReady;
    int64_t mCleanInterval;
    mutable autil::ThreadMutex mCodeGenerateLock;
    mutable autil::ThreadMutex mCleanLock;

private:
    friend class CodeFactoryTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CodeFactory);
}} // namespace indexlib::codegen

#endif //__INDEXLIB_CODE_FACTORY_H
