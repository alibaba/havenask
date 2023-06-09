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
#include "indexlib/framework/TabletId.h"

#include <bits/stdint-uintn.h>

namespace indexlib::framework {
TabletId::TabletId(const std::string& tableName) : _tableName(tableName) {}
TabletId::TabletId(const std::string& tableName, uint32_t generationId, uint16_t from, uint16_t to)
    : _tableName(tableName)
    , _generationId(generationId)
    , _from(from)
    , _to(to)
{
}

void TabletId::SetTableName(const std::string& tableName) { _tableName = tableName; }

void TabletId::SetIp(const std::string& ip) { _ip = ip; }

void TabletId::SetPort(uint32_t port) { _port = port; }

void TabletId::SetGenerationId(uint32_t generationId) { _generationId = generationId; }

void TabletId::SetRange(uint16_t from, uint16_t to)
{
    _from = from;
    _to = to;
}

std::string TabletId::GenerateTabletName() const
{
    return _tableName + "/" + std::to_string(_generationId) + "/" + std::to_string(_from) + "_" + std::to_string(_to);
}

const std::string& TabletId::GetTableName() const { return _tableName; }

const std::string& TabletId::GetIp() const { return _ip; }

uint32_t TabletId::GetPort() const { return _port; }

uint32_t TabletId::GetGenerationId() const { return _generationId; }

std::pair<uint16_t, uint16_t> TabletId::GetRange() const { return {_from, _to}; }

} // namespace indexlib::framework
