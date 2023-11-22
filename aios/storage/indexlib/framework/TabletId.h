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

#include <stdint.h>
#include <string>
#include <utility>

namespace indexlib::framework {

class TabletId
{
public:
    TabletId() = default;
    explicit TabletId(const std::string& tableName);
    TabletId(const std::string& tableName, uint32_t generationId, uint16_t from, uint16_t to);

    void SetTableName(const std::string& tableName);
    void SetIp(const std::string& ip);
    void SetPort(uint32_t port);
    void SetGenerationId(uint32_t generationId);
    void SetRange(uint16_t from, uint16_t to);
    const std::string& GetTableName() const;
    const std::string& GetIp() const;
    uint32_t GetPort() const;
    uint32_t GetGenerationId() const;
    std::pair<uint16_t, uint16_t> GetRange() const;
    std::string GenerateTabletName() const;

private:
    std::string _tableName;
    std::string _ip = "0.0.0.0";
    uint32_t _port = 0;
    uint32_t _generationId = 0;
    // range
    uint16_t _from = 0;
    uint16_t _to = 65535;
};

} // namespace indexlib::framework
