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

#include "autil/Log.h"

namespace indexlibv2::config {
class FieldConfig;
}

namespace indexlibv2 { namespace document {
class RawDocument;

class NullFieldAppender
{
public:
    NullFieldAppender();
    ~NullFieldAppender();

public:
    bool Init(const std::vector<std::shared_ptr<config::FieldConfig>>& fieldConfigs);
    void Append(const std::shared_ptr<RawDocument>& rawDocument);

private:
    std::vector<std::shared_ptr<config::FieldConfig>> _enableNullFields;

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlibv2::document
