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
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/framework/ITabletValidator.h"

namespace indexlib::table {

class NormalTabletValidator : public framework::ITabletValidator
{
public:
    NormalTabletValidator() = default;
    ~NormalTabletValidator() = default;

public:
    Status Validate(const std::string& indexRootPath, const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
                    versionid_t versionId) override;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::table
