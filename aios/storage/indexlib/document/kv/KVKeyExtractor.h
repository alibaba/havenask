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
#include "autil/StringView.h"
#include "indexlib/base/FieldType.h"

namespace indexlibv2 { namespace document {
class KVDocument;

class KVKeyExtractor
{
public:
    KVKeyExtractor(FieldType fieldType, bool useNumberHash);
    ~KVKeyExtractor();

public:
    void HashKey(const std::string& key, uint64_t& keyHash) const;
    void HashKey(const autil::StringView& key, uint64_t& keyHash) const;

private:
    FieldType _fieldType = ft_unknown;
    bool _useNumberHash = false;

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlibv2::document
