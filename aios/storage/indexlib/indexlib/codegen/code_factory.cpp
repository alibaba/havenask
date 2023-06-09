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
#include "indexlib/codegen/code_factory.h"

#include <cassert>
#include <limits.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "autil/EnvUtil.h"
#include "autil/LambdaWorkItem.h"
#include "autil/StringUtil.h"
#include "future_lite/Config.h"
#include "indexlib/codegen/codegen_config.h"

#define SHELL_PATH "/bin/sh"
#define SHELL_NAME "sh"
using namespace std;
using namespace autil;

using namespace indexlib::plugin;
using namespace indexlib::file_system;

namespace indexlib { namespace codegen {

IE_LOG_SETUP(codegen, CodeFactory);

CodeFactory::CodeFactory() : mCleanInterval(DEFAULT_CLEAN_INTERVAL) { mIsReady = init(); }

CodeFactory::~CodeFactory() {}

bool CodeFactory::init()
{
    mCompileInfo.parallel = autil::EnvUtil::getEnv("codegen_compile_thread_num", 1u);
#if FUTURE_LITE_USE_COROUTINES
    mCompileInfo.compiler = "clang++";
#else
    mCompileInfo.compiler = "g++";
#endif
    mCompileInfo.compileOption =
        " -g -shared -O2 -fPIC -m64 -DTARGET_64 -D__STDC_LIMIT_MACROS -fvisibility=hidden -Wno-deprecated";
#if FUTURE_LITE_USE_COROUTINES
    mCompileInfo.compileOption += " -D_GLIBCXX_USE_CXX11_ABI=1 -std=c++2a -fcoroutines-ts -Wno-register "
                                  "-Wno-deprecated-declarations -Wno-mismatched-tags -Wno-format "
                                  "-Wno-inconsistent-missing-override -Wno-unused-private-field "
                                  "-Wno-overloaded-virtual -Wno-unused-value -Wno-unused-const-variable "
                                  "-Wno-parentheses -Wno-expansion-to-defined ";
#elif _GLIBCXX_USE_CXX11_ABI
    mCompileInfo.compileOption += " -D_GLIBCXX_USE_CXX11_ABI=1 -std=c++17 ";
#else
    mCompileInfo.compileOption += " -D_GLIBCXX_USE_CXX11_ABI=0 -std=c++17 ";
#endif

    const char* codegenSo = getenv("DISABLE_CODEGEN");
    if (codegenSo && string(codegenSo) == "true") {
        return false;
    }
    const char* codegenSoKeepTime = getenv("CODEGEN_SO_KEEP_TIME");
    if (codegenSoKeepTime) {
        int64_t keepTime = 0;
        if (StringUtil::fromString(string(codegenSoKeepTime), keepTime) && keepTime >= 0) {
            mCleanInterval = keepTime;
        }
    }
    string currentPath;
    if (!getCurrentPath(currentPath)) {
        return false;
    }
    string tmpCodegenPath = string("codegen_dir/") + INDEXLIB_CODEGEN_VERSION;
    mWorkDir = FslibWrapper::JoinPath(currentPath, tmpCodegenPath);
    const char* installRoot = getenv("HIPPO_APP_INST_ROOT");
    if (!installRoot) {
        IE_LOG(ERROR, "no HIPPO_APP_INST_ROOT env, codegen init fail");
        return false;
    }
    mInstallRoot = string(installRoot);
    const char* preCompileSoPath = getenv("CODEGEN_PRECOMPILE_SO_PATH");
    if (preCompileSoPath) {
        mPreCompiledSoPath = FslibWrapper::JoinPath(string(preCompileSoPath), INDEXLIB_CODEGEN_VERSION);
        IE_LOG(INFO, "use codegen_precompile_so_path [%s]", mPreCompiledSoPath.c_str());
    }
    return true;
}

bool CodeFactory::getCurrentPath(std::string& path)
{
    const char* workDir = getenv("HIPPO_APP_WORKDIR");
    if (workDir) {
        path = string(workDir);
        return true;
    }
    char cwdPath[PATH_MAX];
    char* ret = getcwd(cwdPath, PATH_MAX);
    if (NULL == ret) {
        IE_LOG(ERROR, "Failed to get current work directory");
        return false;
    }
    path = string(cwdPath);
    if ('/' != *(path.rbegin())) {
        path += "/";
    }
    return true;
}

CodegenHintPtr CodeFactory::getCodegenHint(AllCodegenInfo& allCodegenInfo)
{
    auto factory = Singleton<CodeFactory>::GetInstance();
    return factory->doGetCodegenHint(allCodegenInfo);
}

CodeFactory::CompileInfo CodeFactory::getCompileInfo()
{
    auto factory = Singleton<CodeFactory>::GetInstance();
    return factory->doGetCompileInfo();
}

CodeFactory::CompileInfo CodeFactory::doGetCompileInfo() { return mCompileInfo; }

bool CodeFactory::prepareSo(AllCodegenInfo& allCodegenInfo)
{
    autil::ScopedLock lock(mCodeGenerateLock); // one time one so to prepare
    string codeHash = allCodegenInfo.GetCodeHash();
    string targetSoFile = getTargetSoPath(codeHash);
    if (FslibWrapper::IsExist(targetSoFile).GetOrThrow()) {
        return true;
    }
    if (!mPreCompiledSoPath.empty()) {
        string srcSoPath = FslibWrapper::JoinPath(mPreCompiledSoPath, codeHash);
        string srcSoFilePath = FslibWrapper::JoinPath(srcSoPath, codeHash + "_codegen.so");
        if (FslibWrapper::IsExist(srcSoFilePath).GetOrThrow()) {
            THROW_IF_FS_ERROR(FslibWrapper::Copy(srcSoPath, targetSoFile).Code(), "path[%s] to path[%s]",
                              srcSoPath.c_str(), targetSoFile.c_str());
            return true;
        }
    }
    return compileSoInLocal(allCodegenInfo);
}

CodegenHintPtr CodeFactory::getHintFromCache(const std::string& codeHash)
{
    CodegenHintPtr hint;
    auto iter = mHints.find(codeHash);
    if (iter != mHints.end()) {
        iter->second->_unuseTimestamp = 0;
        hint = iter->second;
    }
    return hint;
}

std::string CodeFactory::getSourceCodePath(const std::string& codeHash)
{
    auto factory = Singleton<CodeFactory>::GetInstance();
    return factory->doGetSourceCodePath(codeHash);
}

CodegenHintPtr CodeFactory::doGetCodegenHint(AllCodegenInfo& allCodegenInfo)
{
    string codeHash = allCodegenInfo.GetCodeHash();
    CodegenHintPtr hint;
    if (!mIsReady) {
        return hint;
    }
    try {
        // get from cache
        {
            autil::ScopedLock lock(mCleanLock);
            hint = getHintFromCache(codeHash);
            if (hint) {
                return hint;
            }
        }
        prepareSo(allCodegenInfo);
        // compile so
        autil::ScopedLock lock(mCleanLock);
        // protect for multi thread load so
        hint = getHintFromCache(codeHash);
        if (!hint) {
            auto handler = loadSo(getTargetSoPath(codeHash));
            if (handler) {
                hint.reset(new CodegenHint);
                hint->_handler = handler;
                hint->_codeHash = codeHash;
                mHints[codeHash] = hint;
            }
        }
        cleanUnusedSo();
        if (!hint) {
            INDEXLIB_THROW(util::FileIOException, "codegen so failed");
        }
        return hint;
    } catch (const autil::legacy::ExceptionBase& e) {
        IE_LOG(ERROR, "compile so for [%s] exception [%s]", codeHash.c_str(), e.what());
        INDEXLIB_THROW(util::FileIOException, "codegen so failed");
        return CodegenHintPtr();
    } catch (...) {
        IE_LOG(ERROR, "compile so for [%s] failed, unkown exception", codeHash.c_str());
        INDEXLIB_THROW(util::FileIOException, "codegen so failed");
        return CodegenHintPtr();
    }
}

std::string CodeFactory::doGetSourceCodePath(const std::string& codeHash)
{
    return file_system::FslibWrapper::JoinPath(getCodePath(codeHash), "source_code");
}

void CodeFactory::cleanUnusedSo()
{
    int64_t currentTime = TimeUtility::currentTimeInSeconds();
    std::map<std::string, CodegenHintPtr> tmpHandlers;
    for (auto iter = mHints.begin(); iter != mHints.end();) {
        CodegenHintPtr& hint = iter->second;
        if (hint.use_count() == 1) {
            if (hint->_unuseTimestamp == 0) {
                hint->_unuseTimestamp = currentTime;
            }
            if (hint->_unuseTimestamp != 0 && currentTime - hint->_unuseTimestamp >= mCleanInterval) {
                string codePath = getCodePath(iter->first);
                try {
                    IE_LOG(INFO, "delete useless code [%s]", codePath.c_str());
                    FslibWrapper::DeleteDirE(codePath, DeleteOption::NoFence(false));
                } catch (...) {
                    IE_LOG(ERROR, "delete code path [%s] failed", codePath.c_str());
                }
                iter = mHints.erase(iter);
                continue;
            }
        }
        iter++;
    }
}

bool CodeFactory::compileSoInLocal(AllCodegenInfo& codegenInfos)
{
    string codeHash = codegenInfos.GetCodeHash();
    string codegenInfoFile = getCodegenInfoPath(codeHash);
    StoreInfoFile(codegenInfos, codegenInfoFile);
    string targetSo = getTargetSoPath(codeHash);
    string compileCmd = "codegen_tool " + codegenInfoFile;
    return launchProc("tool compile", compileCmd);
}

// void CodeFactory::preloadSo() {
//     auto factory = Singleton<CodeFactory>::GetInstance();
//     return factory->doPreloadSo();
// }

// void CodeFactory::doPreloadSo() {
//     fslib::FileList dirs;
//     FslibWrapper::ListDirE(_workDir, dirs, true);
//     for (size_t i = 0; i < dirs.size(); i++) {
//         auto soPath = FslibWrapper::JoinPath(_workDir, dirs[i] + "/" + dirs[i] + "_codegen.so");
//         std::cout << soPath << std::endl;
//         if (FslibWrapper::IsExist(soPath).GetOrThrow()) {
//             auto handler = loadSo(soPath);
//             CodegenHintPtr hint;
//             {
//                 autil::ScopedLock lock(mCleanLock);
//                 if (handler) {
//                     hint.reset(new CodegenHint);
//                     hint->_handler = handler;
//                     _hints[dirs[i]] = hint;
//                 }
//             }
//         }
//     }
// }

plugin::DllWrapperPtr CodeFactory::loadSo(const std::string& path)
{
    std::cout << "load so:" << path << std::endl;
    DllWrapperPtr dll(new DllWrapper(path));
    if (dll->dlopen()) {
        std::cout << "load so done" << std::endl;
        return dll;
    }
    std::cout << "load so failed" << std::endl;
    return DllWrapperPtr();
}

bool CodeFactory::launchProc(const std::string& name, const std::string& cmd)
{
    std::cout << cmd << std::endl;
    IE_LOG(INFO, "%s %s", name.c_str(), cmd.c_str());
    int ret = vsystem(cmd.c_str());
    int err = errno;
    if (ret != 0) {
        IE_LOG(ERROR, "%s %s compile failed, ret:%d, err:%d", name.c_str(), cmd.c_str(), ret, err);
        return false;
    }
    return true;
}

// a libc system implementation via vfork call.
// TODO: not impl: SIGCHLD will be blocked, and SIGINT and SIGQUIT will be ignored
int CodeFactory::vsystem(const char* cmd)
{
    const char* new_argv[4];
    new_argv[0] = SHELL_NAME;
    new_argv[1] = "-c";
    new_argv[2] = (cmd == nullptr) ? "exit 0" : cmd;
    new_argv[3] = nullptr;
    pid_t pid;
    int status;

    pid = vfork();
    if (pid == 0) {
        // child
        execv(SHELL_PATH, const_cast<char* const*>(new_argv));
    } else if (pid < 0) {
        // error
        return -1;
    } else {
        // parent
        waitpid(pid, &status, 0);
        return (cmd == nullptr) ? (status == 0) : status;
    }
    assert(false);
    return -1;
}
}} // namespace indexlib::codegen
