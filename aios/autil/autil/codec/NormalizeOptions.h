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

#include <string>

#include "autil/legacy/jsonizable.h"

namespace autil {
namespace codec {

static const std::string CASE_SENSITIVE = "case_sensitive";
static const std::string TRADITIONAL_SENSITIVE = "traditional_sensitive";
static const std::string WIDTH_SENSITIVE = "width_sensitive";
static const std::string TRADITIONAL_TABLE_NAME = "traditional_table_name";

struct NormalizeOptions : public autil::legacy::Jsonizable
{
    NormalizeOptions(bool f1 = true, bool f2 = true, bool f3 = true)
        : caseSensitive(f1)
        , traditionalSensitive(f2)
        , widthSensitive(f3)
    {
    }

    bool caseSensitive;
    bool traditionalSensitive;
    bool widthSensitive;
    std::string tableName;

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override {
        json.Jsonize(CASE_SENSITIVE, caseSensitive, true);
        json.Jsonize(TRADITIONAL_SENSITIVE, traditionalSensitive, true);
        json.Jsonize(WIDTH_SENSITIVE, widthSensitive, true);
        json.Jsonize(TRADITIONAL_TABLE_NAME,tableName, tableName);
    }
};

}}

