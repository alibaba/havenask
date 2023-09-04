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
#include "suez/turing/expression/cava/common/CavaJitWrapper.h"

#include <algorithm>
#include <cstddef>
#include <errno.h>
#include <ext/alloc_traits.h>
#include <limits.h>
#include <llvm/Support/Error.h>
#include <sys/stat.h>
#include <unistd.h>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/Thread.h"
#include "autil/TimeUtility.h"
#include "cava/ast/ASTUtil.h"
#include "cava/codegen/CavaJitModule.h"
#include "cava/codegen/CavaModule.h"
#include "cava/common/Common.h"
#include "cava/jit/CavaJit.h"
#include "cava/jit/CavaJitConfig.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/FileSystem.h"
#include "kmonitor/client/MetricMacro.h"
#include "kmonitor/client/MetricsReporter.h" // IWYU pragma: keep
#include "suez/turing/expression/cava/common/CavaJitPool.h"
#include "suez/turing/expression/cava/common/CavaModuleCache.h"
#include "suez/turing/expression/cava/common/CompileWorkItem.h"
#include "turing_ops_util/variant/Tracer.h"

using namespace std;
using namespace kmonitor;
using namespace autil;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, CavaJitWrapper);

static const std::string kCompileQpsMetricName = "cava.compileQps";
static const std::string kCompileErrorQpsMetricName = "cava.compileErrorQps";
static const std::string kCodeCacheHitQpsMetricName = "cava.codeCacheHitQps";
static const std::string kCompileLatencyMetricName = "cava.compileLatency";

CavaJitWrapper::CavaJitWrapper(const std::string &configPath,
                               const CavaConfig &cavaConfig,
                               MetricsReporter *reporter,
                               bool fastCompile)
    : _cavaJitPool(new CavaJitPool)
    , _configPath(configPath)
    , _cavaConfig(cavaConfig)
    , _reporter(reporter)
    , _fastCompile(fastCompile) {}

void CavaJitWrapper::splitSourcePath(const std::string &configPath,
                                     const std::string &userPath,
                                     std::vector<std::string> &pathList) {
    if (!userPath.empty()) {
        std::vector<std::string> pathSeps = {",", ";", "|"};
        std::string sep;
        for (auto &s : pathSeps) {
            if (userPath.find(s) != std::string::npos) {
                sep = s;
                break;
            }
        }

        if (sep.empty()) {
            std::string srcPath = fslib::fs::FileSystem::joinFilePath(configPath, userPath);
            pathList.push_back(srcPath);
        } else {
            auto paths = autil::StringUtil::split(userPath, sep);
            for (auto &path : paths) {
                autil::StringUtil::trim(path); // "1/2/3; 4/5/6"
                std::string srcPath = fslib::fs::FileSystem::joinFilePath(configPath, path);
                pathList.push_back(srcPath);
            }
        }
    }
}

bool CavaJitWrapper::localDirExist(const string &localDirPath) {
    struct stat st;
    if (stat(localDirPath.c_str(), &st) != 0) {
        AUTIL_LOG(DEBUG, "stat localDirPath[%s] error, errno:%d", localDirPath.c_str(), errno);
        return false;
    }
    if (!S_ISDIR(st.st_mode)) {
        AUTIL_LOG(DEBUG, "localDirPath[%s] is not a directory", localDirPath.c_str());
        return false;
    }
    return true;
}

bool CavaJitWrapper::init() {
    AUTIL_LOG(INFO, "cava config  path: %s", _cavaConfig._cavaConf.c_str());

    size_t compileThreadNum = _cavaConfig.getCompileThreadNum();
    AUTIL_LOG(INFO, "cava compile thread num[%ld]", compileThreadNum);
    _compileThreadPool.reset(new autil::ThreadPool(compileThreadNum, _cavaConfig._compileQueueSize));
    _compileThreadPool->start("TuringCavaJit");
    _cavaModuleCache.reset(new CavaModuleCache(_cavaConfig._moduleCacheSize));

    if (!initAddCavaJit(compileThreadNum)) {
        return false;
    }
    if (!initCodeCache()) {
        return false;
    }
    return true;
}

bool CavaJitWrapper::getCurrentPath(std::string &path) {
    char cwdPath[PATH_MAX];
    char *ret = getcwd(cwdPath, PATH_MAX);
    if (NULL == ret) {
        AUTIL_LOG(ERROR, "Failed to get current work directory");
        return false;
    }
    path = string(cwdPath);
    if ('/' != *(path.rbegin())) {
        path += "/";
    }
    return true;
}

bool CavaJitWrapper::initAddCavaJit(size_t compileThreadNum) {
    autil::ScopedTime2 timer;
    // compile multiple cava jit with threads
    vector<int> success(compileThreadNum, 0);
    vector<ThreadPtr> initCavaJitThreads;
    for (size_t i = 0; i < compileThreadNum; ++i) {
        auto *initResultPtr = &success[i];
        auto threadFunc = [this, initResultPtr]() { *initResultPtr = this->initAddSingleCavaJitInThread(); };
        initCavaJitThreads.emplace_back(Thread::createThread(threadFunc));
    }
    for (auto &threadPtr : initCavaJitThreads) {
        threadPtr->join();
    }
    for (size_t i = 0; i < compileThreadNum; ++i) {
        if (!success[i]) {
            AUTIL_LOG(ERROR, "init [%lu] cava jit failed", i);
            return false;
        }
    }
    AUTIL_LOG(INFO, "init all CavaJits use %lf ms", timer.done_ms());

    return true;
}

bool CavaJitWrapper::initAddSingleCavaJitInThread() {
    autil::ScopedTime2 timer;
    CavaJitPoolItem *item = new CavaJitPoolItem;
    _cavaJitPool->initAdd(item);
    auto *cavaJit = &item->cavaJit;
    std::string workDir;
    if (!getCurrentPath(workDir)) {
        AUTIL_LOG(ERROR, "getCurrentPath failed");
        return false;
    }
    AUTIL_LOG(INFO, "current path: %s", workDir.c_str());
    if (!cavaJit->init({_configPath, workDir}, _cavaConfig._cavaConf, _fastCompile)) {
        AUTIL_LOG(ERROR, "init failed: %s", workDir.c_str());
        return false;
    }
    AUTIL_LOG(INFO, "cava jit init use %lf ms", timer.done_ms());
    std::vector<std::string> pathList;
    // add buildin cava plugins
    auto cavaPluginsDir = fslib::fs::FileSystem::joinFilePath(workDir, SUEZ_CAVA_PLUGINS);
    if (localDirExist(cavaPluginsDir)) {
        pathList.push_back(cavaPluginsDir);
    }
    // add user libs
    splitSourcePath(_configPath, _cavaConfig._libPath, pathList);
    // add user plugins
    splitSourcePath(_configPath, _cavaConfig._sourcePath, pathList);
    if (!pathList.empty()) {
        autil::ScopedTime2 timer;
        cava::CavaModuleOptions suezCavaModuleOptions;
        suezCavaModuleOptions.isPreload = true;
        auto ret = cavaJit->createCavaModule("SuezCava", pathList, true, &suezCavaModuleOptions);
        AUTIL_LOG(INFO, "create suez cava module use %lf ms", timer.done_ms());
        if (!ret) {
            AUTIL_LOG(
                ERROR, "create cava module from source and lib failed, error: %s", toString(ret.takeError()).c_str());
            return false;
        }
        auto *cavaModule = *ret;
        {
            autil::ScopedLock lock(_suezBuiltinModuleCreateMutex);
            if (!_suezBuiltinModule) {
                _suezBuiltinModule.reset(new cava::CavaJitModule(cavaModule, cavaJit, false));
            }
        }
    }
    return true;
}

bool CavaJitWrapper::initCodeCache() {
    // constructCodeCache
    if (!_cavaConfig._codeCachePath.empty()) {
        std::vector<std::string> codeCachePaths;
        splitSourcePath(_configPath, _cavaConfig._codeCachePath, codeCachePaths);
        std::vector<std::string> fileList;
        for (auto &path : codeCachePaths) {
            if (!::cava::CavaJit::globSourceFile(path, fileList)) {
                AUTIL_LOG(ERROR, "list dir %s failed", path.c_str());
                return false;
            }
        }
        if (!constructCodeCache(fileList)) {
            return false;
        }
    }
    return true;
}

cava::CavaJitModulePtr CavaJitWrapper::compile(const std::string &moduleName,
                                               const std::vector<std::string> &fileNames,
                                               const std::vector<std::string> &codes,
                                               size_t hashKey,
                                               const cava::CavaModuleOptions *options,
                                               Tracer *tracer) {
    CavaJitPoolItemPtr item = _cavaJitPool->get();
    if (!item) {
        AUTIL_LOG(ERROR, "no available cavaJit");
        return cava::CavaJitModulePtr();
    }
    auto *cavaJit = &item->cavaJit;
    // trigger cava remove jit module
    clearDelList();
    auto cavaJitModule = _cavaModuleCache->get(hashKey);
    if (cavaJitModule) {
        REQUEST_TRACE_WITH_TRACER(tracer, INFO, "cava module hit cache, hash key [%lu]", hashKey);
        return cavaJitModule;
    }
    REPORT_USER_MUTABLE_QPS(_reporter, kCompileQpsMetricName);
    // compile
    autil::ScopedTime2 timer;
    auto ret = cavaJit->createCavaModule(moduleName, fileNames, codes, false, options);
    REPORT_USER_MUTABLE_LATENCY(_reporter, kCompileLatencyMetricName, timer.done_ms());
    if (!ret) {
        AUTIL_LOG(ERROR, "create cava module [%s] failed", moduleName.c_str());
        REQUEST_TRACE_WITH_TRACER(tracer,
                                  ERROR,
                                  "create cava module [%s] failed, error info: %s",
                                  moduleName.c_str(),
                                  toString(ret.takeError()).c_str());
        REPORT_USER_MUTABLE_QPS(_reporter, kCompileErrorQpsMetricName);
        return cava::CavaJitModulePtr();
    }
    auto *cavaModule = *ret;
    if (cava::ASTUtil::hasNativeFunc(cavaModule->getASTContext())) {
        AUTIL_LOG(ERROR, "not support native method for security reason, moduleName %s", moduleName.c_str());
        REQUEST_TRACE_WITH_TRACER(
            tracer, ERROR, "not support native method for security reason, moduleName %s", moduleName.c_str());
        cavaJit->removeCavaModule(cavaModule);
        REPORT_USER_MUTABLE_QPS(_reporter, kCompileErrorQpsMetricName);
        return cava::CavaJitModulePtr();
    }
    REQUEST_TRACE_WITH_TRACER(tracer, INFO, "compile module [%s] succeed, hash key [%lu]", moduleName.c_str(), hashKey);
    cavaJitModule.reset(new cava::CavaJitModule(cavaModule, cavaJit, true));
    _cavaModuleCache->put(hashKey, cavaJitModule);
    return cavaJitModule;
}

::cava::CavaJitModulePtr CavaJitWrapper::compileAsync(const std::string &moduleName,
                                                      const std::string &fileName,
                                                      const std::string &code,
                                                      const cava::CavaModuleOptions *options) {
    size_t hashKey = calcHashKey({code});
    cava::CavaJitModulePtr cavaJitModule = _cavaModuleCache->get(hashKey);
    if (cavaJitModule) {
        return cavaJitModule;
    }
    auto item = new SingleCompileWorkItem(this, moduleName, fileName, code, hashKey, options);
    if (_compileThreadPool->pushWorkItem(item, false) != autil::ThreadPool::ERROR_NONE) {
        AUTIL_LOG(ERROR, "Failed to push compile task");
        item->drop();
    }
    return cavaJitModule;
}

bool CavaJitWrapper::compileAsync(const std::string &moduleName,
                                  const std::vector<std::string> &fileNames,
                                  const std::vector<std::string> &codes,
                                  suez::turing::Tracer *tracer,
                                  cava::CavaJitModulesWrapper *cavaJitModulesWrapper,
                                  ClosurePtr closure,
                                  const cava::CavaModuleOptions *options) {
    size_t hashKey = calcHashKey(codes);
    cava::CavaJitModulePtr cavaJitModule = _cavaModuleCache->get(hashKey);
    if (cavaJitModule) {
        REPORT_USER_MUTABLE_QPS(_reporter, kCodeCacheHitQpsMetricName);
        if (tracer) {
            REQUEST_TRACE_WITH_TRACER(tracer, INFO, "hit cava module cache");
        }
        if (cavaJitModulesWrapper) {
            cavaJitModulesWrapper->addCavaJitModule(hashKey, cavaJitModule);
        }
        closure->run();
        return true;
    }

    CompileWorkItem *item = new CompileWorkItem(
        this, moduleName, fileNames, codes, hashKey, tracer, cavaJitModulesWrapper, closure, options);
    if (_compileThreadPool->pushWorkItem(item, false) != autil::ThreadPool::ERROR_NONE) {
        AUTIL_LOG(ERROR, "Failed to push compile task");
        item->drop();
        return false;
    }
    return true;
}

void CavaJitWrapper::delCavaJitModule(const cava::CavaJitModulePtr &cavaJitModule) {
    autil::ScopedLock lock(_mutex);
    _delModules.push_back(cavaJitModule);
}

void CavaJitWrapper::clearDelList() {
    autil::ScopedLock lock(_mutex);
    _delModules.clear();
}

bool CavaJitWrapper::constructCodeCache(const std::vector<std::string> &fileList) {
    for (auto file : fileList) {
        string context;
        if (fslib::EC_OK != fslib::fs::FileSystem::readFile(file, context)) {
            AUTIL_LOG(ERROR, "read file %s compile failed", file.c_str());
            return false;
        }
        size_t key = calcHashKey({context});
        AUTIL_LOG(INFO, "constructCodeCache, file %s hash: %lu", file.c_str(), key);

        cava::CavaModuleOptions noSafeCheckOptions;
        noSafeCheckOptions.safeCheck = cava::SafeCheckLevel::SCL_NONE;
        cava::CavaModuleOptions *options = nullptr;
        // filename_0.cava;
        const auto &tmp = autil::StringUtil::split(file, "_");
        if (tmp.size() > 1) {
            if (tmp[1] == "0") {
                options = &noSafeCheckOptions;
            }
        }
        auto cavaModuleInfo = compile(file, {file}, {context}, key, options);
        if (!cavaModuleInfo) {
            AUTIL_LOG(ERROR, "file %s compile failed", file.c_str());
            return false;
        }
    }
    return true;
}

size_t CavaJitWrapper::getModuleCacheCount() { return _cavaModuleCache->size(); }

::cava::CavaModule *CavaJitWrapper::getCavaModule() {
    if (_suezBuiltinModule) {
        return _suezBuiltinModule->_cavaModule;
    }
    return nullptr;
}

size_t CavaJitWrapper::calcHashKey(const std::vector<std::string> &codes) {
    size_t hashKey = 0;
    for (auto &code : codes) {
        ::cava::HashCombine(hashKey, ::cava::StringHasher(code));
    }
    return hashKey;
}

} // namespace turing
} // namespace suez
