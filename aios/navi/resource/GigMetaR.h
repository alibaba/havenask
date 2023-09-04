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
#include "aios/network/gig/multi_call/common/VersionInfo.h"

namespace navi {

class GigMetaR;
NAVI_TYPEDEF_PTR(GigMetaR);

class GigMetaR : public navi::Resource
{
public:
    GigMetaR();
    GigMetaR(const std::vector<multi_call::BizMetaInfo> &bizMetaInfos);
    ~GigMetaR();
    GigMetaR(const GigMetaR &) = delete;
    GigMetaR &operator=(const GigMetaR &) = delete;
public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;
public:
    const std::vector<multi_call::BizMetaInfo> &getBizMetaInfos() const {
        return _bizMetaInfos;
    }
    bool isSame(const GigMetaRPtr &otherPtr) const;
    void log() const;
public:
    static const std::string RESOURCE_ID;
private:
    std::vector<multi_call::BizMetaInfo> _bizMetaInfos;
};

}

