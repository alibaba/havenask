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
#include <any>

#include "autil/Log.h"
#include "ha3/sql/ops/scan/ScanBase.h"
#include "ha3/sql/framework/PushDownOp.h"
#include "navi/common.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelConfigContext.h"

namespace navi {
class GraphMemoryPoolResource;
class GigQuerySessionR;
class KernelComputeContext;
class KernelDefBuilder;
class KernelInitContext;
} // namespace navi

namespace isearch {
namespace sql {

class SqlBizResource;
class SqlQueryResource;
class ObjectPoolResource;
class MetaInfoResource;
class SqlConfigResource;

class ScanKernelBase {
public:
    ScanKernelBase();
    virtual ~ScanKernelBase();

private:
    ScanKernelBase(const ScanKernelBase &);
    ScanKernelBase &operator=(const ScanKernelBase &);

public:
    bool config(navi::KernelConfigContext &ctx);
    bool init(navi::KernelInitContext &initContext,
                         const std::string &kernelName,
                         const std::string &nodeName);

protected:
    bool initPushDownOpConfig(navi::KernelConfigContext &pushDownOpsCtx);
    navi::ErrorCode initPushDownOps();
    template <class InitParam, class Wrapper>
    PushDownOpPtr createPushDownOp(std::any &config,
                                   const std::string &opName,
                                   const std::string &nodeName);

protected:
    ScanInitParam _initParam;
    ScanBasePtr _scanBase;
    SqlBizResource *_bizResource = nullptr;
    SqlQueryResource *_queryResource = nullptr;
    ObjectPoolResource *_objectPoolResource = nullptr;
    navi::GraphMemoryPoolResource *_memoryPoolResource = nullptr;
    navi::GigQuerySessionR *_naviQuerySessionR = nullptr;
    MetaInfoResource *_metaInfoResource = nullptr;
    SqlConfigResource *_sqlConfigResource = nullptr;
    TabletManagerR *_tabletManagerR = nullptr;
    std::vector<std::any> _pushDownOpInitParams;
    std::vector<PushDownOpPtr> _pushDownOps;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ScanKernelBase> ScanKernelBasePtr;

template <class InitParam, class Wrapper>
PushDownOpPtr ScanKernelBase::createPushDownOp(
        std::any &config,
        const std::string &opName,
        const std::string &nodeName) {
    try {
        InitParam param = std::any_cast<InitParam>(config);
        param.opName = _initParam.opName + "." + opName;
        param.nodeName = _initParam.nodeName + "." + nodeName;
        PushDownOpPtr opPtr(new Wrapper(param, _bizResource, _queryResource,
                        _metaInfoResource, _memoryPoolResource));
        if (!opPtr->init()) {
            SQL_LOG(ERROR, "init %sth %s wrapper failed",
                    nodeName.c_str(), opName.c_str());
            return PushDownOpPtr();
        }
        return opPtr;
    } catch(...) {
        SQL_LOG(ERROR, "init type and wrapper not match");
        return PushDownOpPtr();
    }
}

} // namespace sql
} // namespace isearch
