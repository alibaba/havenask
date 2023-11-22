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

namespace sql {

class JoinInfo;

class JoinInfoCollectorR : public navi::Resource {
public:
    JoinInfoCollectorR();
    ~JoinInfoCollectorR();
    JoinInfoCollectorR(const JoinInfoCollectorR &) = delete;
    JoinInfoCollectorR &operator=(const JoinInfoCollectorR &) = delete;

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    void incHashTime(uint64_t time);
    void incCreateTime(uint64_t time);
    void incLeftHashCount(uint64_t count);
    void incRightHashCount(uint64_t count);
    void incHashMapSize(uint64_t size);
    void incComputeTimes();
    void incJoinCount(uint64_t count);
    void incTotalTime(uint64_t time);
    void incJoinTime(uint64_t time);
    void incInitTableTime(uint64_t time);
    void incEvaluateTime(uint64_t time);
    void incRightScanTime(uint64_t time);
    void incRightUpdateQueryTime(uint64_t time);
    void incTotalLeftInputTable(size_t count);
    void incTotalRightInputTable(size_t count);

public:
    static const std::string RESOURCE_ID;

public:
    std::unique_ptr<JoinInfo> _joinInfo;
};

} // namespace sql
