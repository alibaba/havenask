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

#include "indexlib/framework/ITabletExporter.h"

namespace indexlib::table {

class NormalTabletExporter : public framework::ITabletExporter
{
public:
    NormalTabletExporter();
    ~NormalTabletExporter();

public:
    std::optional<std::string> CreateTabletOptionsString() override;
    std::unique_ptr<indexlibv2::framework::ITabletDocIterator> CreateTabletDocIterator() override;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::table
