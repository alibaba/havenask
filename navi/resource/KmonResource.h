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
#include "navi/engine/Resource.h"
#include "kmonitor/client/MetricsReporter.h"

namespace navi {

class KmonResourceBase : public Resource
{
public:
    KmonResourceBase(kmonitor::MetricsReporterPtr baseReporter);
    ~KmonResourceBase();
private:
    KmonResourceBase(const KmonResourceBase &);
    KmonResourceBase &operator=(const KmonResourceBase &);
public:
    void def(ResourceDefBuilder &builder) const override = 0;
    bool config(ResourceConfigContext &ctx) override;
    ErrorCode init(ResourceInitContext &ctx) override;
public:
    const kmonitor::MetricsReporterPtr &getBaseReporter() {
        return _baseReporter;
    }
protected:
    kmonitor::MetricsReporterPtr _baseReporter;
};

class RootKmonResource : public KmonResourceBase {
public:
    RootKmonResource(kmonitor::MetricsReporterPtr baseReporter);
public:
    void def(ResourceDefBuilder &builder) const override;
};

class BizKmonResource : public KmonResourceBase {
public:
    BizKmonResource();
public:
    void def(ResourceDefBuilder &builder) const override;
    bool config(ResourceConfigContext &ctx) override;
    ErrorCode init(ResourceInitContext &ctx) override;
private:
    RootKmonResource *_rootKmonResource = nullptr;
    Resource *_unusedSqlBizResource = nullptr;
};

}
