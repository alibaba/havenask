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

#include "alog/Logger.h"
#include "orc/Logger.hh"

namespace indexlibv2::index {

class AlogOrcLoggerAdapter final : public ::orc::Logger
{
public:
    explicit AlogOrcLoggerAdapter(const std::string& path);

public:
    void logV(OrcLogLevel level, const char* fname, int lineno, const char* function, const char* fmt, ...) override;

    void log(OrcLogLevel level, const char* fname, int lineno, const char* function, const std::string& key,
             const std::string& value) override;

    bool isLevelEnabled(OrcLogLevel level) const override;

private:
    static uint32_t ConvertOrcLevelToAlogLevel(OrcLogLevel level);

private:
    alog::Logger* _logger;
};

} // namespace indexlibv2::index
