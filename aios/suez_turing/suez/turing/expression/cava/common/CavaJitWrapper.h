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

#include <memory>
#include <stddef.h>
#include <string>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/ThreadPool.h"
#include "suez/turing/common/Closure.h"
#include "suez/turing/expression/cava/common/CavaConfig.h"

namespace suez {
namespace turing {
class Tracer;
} // namespace turing
} // namespace suez

namespace cava {
class CavaModule;
class CavaJitModule;
class CavaModuleOptions;
class CavaJitModulesWrapper;

typedef std::shared_ptr<CavaJitModule> CavaJitModulePtr;
} // namespace cava

namespace kmonitor {
class MetricsReporter;
}
namespace suez {
namespace turing {
class RuntimeState;
template <class KeyType, class ValType>
class ObjLRUCache;

typedef ObjLRUCache<size_t, ::cava::CavaJitModule> CavaModuleCache;
typedef typename std::shared_ptr<CavaModuleCache> CavaModuleCachePtr;
class CavaJitPool;

typedef std::shared_ptr<CavaJitPool> CavaJitPoolPtr;

class CavaJitWrapper {
public:
    CavaJitWrapper(const std::string &configPath,
                   const CavaConfig &cavaConfig,
                   kmonitor::MetricsReporter *reporter,
                   bool fastCompile = false);
    virtual ~CavaJitWrapper() {
        if (_compileThreadPool) {
            _compileThreadPool->stop(autil::ThreadPool::STOP_AND_CLEAR_QUEUE);
        }
    }
    friend class CompileWorkItem;
    friend class SingleCompileWorkItem;

private:
    CavaJitWrapper(const CavaJitWrapper &);
    CavaJitWrapper &operator=(const CavaJitWrapper &);

public:
    static void
    splitSourcePath(const std::string &configPath, const std::string &userPath, std::vector<std::string> &pathList);
    bool init();

    ::cava::CavaJitModulePtr compileAsync(const std::string &moduleName,
                                          const std::string &fileName,
                                          const std::string &code,
                                          const cava::CavaModuleOptions *options = nullptr);

    virtual bool compileAsync(const std::string &moduleNames,
                              const std::vector<std::string> &fileNames,
                              const std::vector<std::string> &codes,
                              suez::turing::Tracer *tracer,
                              cava::CavaJitModulesWrapper *cavaJitModulesWrapper,
                              ClosurePtr closure,
                              const cava::CavaModuleOptions *options = nullptr);
    // query resource need call delCavaJitModule
    void delCavaJitModule(const ::cava::CavaJitModulePtr &cavaJitModule);

    size_t calcHashKey(const std::vector<std::string> &codes);
    ::cava::CavaJitModulePtr getCavaJitModule() { return _suezBuiltinModule; }
    ::cava::CavaModule *getCavaModule();
    size_t getModuleCacheCount();
    size_t getCompileQueueSize() { return _compileThreadPool->getItemCount(); }

private:
    bool initAddCavaJit(size_t compileThreadNum);
    bool initAddSingleCavaJitInThread();
    bool initCodeCache();
    cava::CavaJitModulePtr compile(const std::string &moduleName,
                                   const std::vector<std::string> &fileNames,
                                   const std::vector<std::string> &codes,
                                   size_t hashKey,
                                   const cava::CavaModuleOptions *options = nullptr,
                                   Tracer *tracer = nullptr);
    void clearDelList();
    bool constructCodeCache(const std::vector<std::string> &fileList);
    static bool localDirExist(const std::string &localDirPath);
    static bool getCurrentPath(std::string &path);

private:
    CavaJitPoolPtr _cavaJitPool;
    std::string _configPath;
    CavaConfig _cavaConfig;
    autil::ThreadMutex _suezBuiltinModuleCreateMutex;
    ::cava::CavaJitModulePtr _suezBuiltinModule;
    CavaModuleCachePtr _cavaModuleCache;
    autil::ThreadPoolPtr _compileThreadPool;

    autil::ThreadMutex _mutex;
    std::vector<::cava::CavaJitModulePtr> _delModules;
    kmonitor::MetricsReporter *_reporter;
    bool _fastCompile;
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<CavaJitWrapper> CavaJitWrapperPtr;
} // namespace turing
} // namespace suez
