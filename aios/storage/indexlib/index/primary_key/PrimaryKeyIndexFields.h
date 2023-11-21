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
#include "indexlib/base/Types.h"
#include "indexlib/document/IIndexFields.h"

namespace indexlibv2::index {

class PrimaryKeyIndexFields : public document::IIndexFields
{
public:
    PrimaryKeyIndexFields();
    ~PrimaryKeyIndexFields();

public:
    void serialize(autil::DataBuffer& dataBuffer) const override;
    void deserialize(autil::DataBuffer& dataBuffer) override;

    autil::StringView GetIndexType() const override;
    size_t EstimateMemory() const override;

    void SetPrimaryKey(const std::string& field);
    const std::string& GetPrimaryKey() const;

private:
    static constexpr uint32_t SERIALIZE_VERSION = 0;
    std::string _primaryKey;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
