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
#include "autil/NoCopyable.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/index/attribute/patch/IAttributePatch.h"

namespace auti::mem_pool {
class Pool;
}

namespace indexlibv2::config {
class AttributeConfig;
}

namespace indexlibv2::index {
class DefaultValueAttributePatch final : public IAttributePatch
{
public:
    DefaultValueAttributePatch() = default;
    ~DefaultValueAttributePatch() = default;

public:
    Status Open(const std::shared_ptr<config::AttributeConfig>& attrConfig);
    void SetPatchReader(const std::shared_ptr<IAttributePatch>& patchReader);

    std::pair<Status, size_t> Seek(docid_t docId, uint8_t* value, size_t maxLen, bool& isNull) override;
    bool UpdateField(docid_t docId, const autil::StringView& value, bool isNull) override;
    uint32_t GetMaxPatchItemLen() const override;

    static std::pair<Status, autil::StringView>
    GetDecodedDefaultValue(const std::shared_ptr<config::AttributeConfig>& attrConfig, autil::mem_pool::Pool* pool);

private:
    autil::mem_pool::Pool _pool;
    autil::StringView _defaultValue;
    std::shared_ptr<IAttributePatch> _patchReader;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
