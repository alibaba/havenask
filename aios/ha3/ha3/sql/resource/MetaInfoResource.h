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

#include "autil/Log.h"
#include "ha3/sql/proto/SqlSearchInfoCollector.h"
#include "navi/engine/Resource.h"
#include "navi/resource/MetaInfoResourceBase.h"

namespace isearch {
namespace sql {

class MetaInfoResource : public navi::MetaInfoResourceBase {
public:
    MetaInfoResource();
    ~MetaInfoResource();
    MetaInfoResource(const MetaInfoResource &) = delete;
    MetaInfoResource &operator=(const MetaInfoResource &) = delete;

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    void preSerialize() override;
    void serialize(autil::DataBuffer &dataBuffer) const override;
    bool merge(autil::DataBuffer &dataBuffer) override;
    void finalize() override;

public:
    SqlSearchInfoCollector *getOverwriteInfoCollector() {
        return &_overwriteInfoCollector;
    }
    SqlSearchInfoCollector *getMergeInfoCollector() {
        return &_mergeInfoCollector;
    }

private:
    static constexpr size_t META_INFO_RESOURCE_CHECKSUM = 0xBE000001;
    // no lock needed as SqlSearchInfoCollector has lock
    SqlSearchInfoCollector _overwriteInfoCollector;
    SqlSearchInfoCollector _mergeInfoCollector;

private:
    AUTIL_LOG_DECLARE();
};

NAVI_TYPEDEF_PTR(MetaInfoResource);

} // namespace sql
} // namespace isearch
