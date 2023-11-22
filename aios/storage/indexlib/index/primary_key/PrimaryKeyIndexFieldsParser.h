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
#include "indexlib/document/IIndexFieldsParser.h"

namespace indexlibv2::index {

class PrimaryKeyIndexFieldsParser : public document::IIndexFieldsParser
{
public:
    PrimaryKeyIndexFieldsParser();
    ~PrimaryKeyIndexFieldsParser();

public:
    Status Init(const std::vector<std::shared_ptr<config::IIndexConfig>>& indexConfigs,
                const std::shared_ptr<document::DocumentInitParam>& initParam) override;
    indexlib::util::PooledUniquePtr<document::IIndexFields>
    Parse(const document::ExtendDocument& extendDoc, autil::mem_pool::Pool* pool, bool& hasFormatError) const override;
    indexlib::util::PooledUniquePtr<document::IIndexFields> Parse(autil::StringView serializedData,
                                                                  autil::mem_pool::Pool* pool) const override;

private:
    std::string _pkFieldName;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
