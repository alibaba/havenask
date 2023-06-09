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
#include "indexlib/base/Status.h"

namespace indexlibv2::framework {

class ITabletPortal : private autil::NoCopyable
{
public:
    virtual ~ITabletPortal() = default;

public:
    virtual void GetTabletNames(const std::string& schemaName, std::vector<std::string>& tabletNames) const = 0;

    virtual Status QueryTabletSchema(const std::string& tabletName, std::string& schemaStr) const = 0;

    virtual Status QueryTabletInfoSummary(const std::string& tabletName, std::string& summaryInfo) const = 0;

    virtual Status QueryTabletInfoDetail(const std::string& tabletName, std::string& tabletInfoStr) const = 0;

    virtual Status SearchTablet(const std::string& tabletName, const std::string& jsonQuery,
                                std::string& result) const = 0;

    virtual Status BuildRawDoc(const std::string& tabletName, const std::string& docStr,
                               const std::string& formatStr) = 0;

    virtual bool IsRunning() const = 0;
};

} // namespace indexlibv2::framework
