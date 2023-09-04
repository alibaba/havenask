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
#include <stdint.h>
#include <string>
#include <vector>

#include "navi/common.h"
#include "navi/engine/Resource.h"
#include "navi/engine/ResourceConfigContext.h"
#include "sql/framework/PushDownOp.h"

namespace indexlib {
namespace index {
class InvertedIndexReader;
} // namespace index
} // namespace indexlib
namespace navi {
class ResourceDefBuilder;
class ResourceInitContext;
} // namespace navi
namespace suez {
namespace turing {
class IndexInfoHelper;
class MetaInfo;
} // namespace turing
} // namespace suez

namespace sql {

class ScanPushDownR : public navi::Resource {
public:
    ScanPushDownR();
    ~ScanPushDownR();
    ScanPushDownR(const ScanPushDownR &) = delete;
    ScanPushDownR &operator=(const ScanPushDownR &) = delete;

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    int32_t getReserveMaxCount() const {
        return _reserveMaxCount;
    }
    size_t size() const {
        return _pushDownOps.size();
    }
    void setMatchInfo(std::shared_ptr<suez::turing::MetaInfo> metaInfo,
                      const suez::turing::IndexInfoHelper *indexInfoHelper,
                      const std::shared_ptr<indexlib::index::InvertedIndexReader> &indexReader);
    bool compute(table::TablePtr &table, bool &eof) const;

private:
    bool initPushDownOp(navi::ResourceInitContext &ctx, navi::KernelConfigContext &pushDownOpsCtx);

public:
    static const std::string RESOURCE_ID;

private:
    std::vector<PushDownOpPtr> _pushDownOps;
    int32_t _reserveMaxCount = -1;
};

NAVI_TYPEDEF_PTR(ScanPushDownR);

} // namespace sql
