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

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/file_system/IDirectory.h"

namespace indexlibv2::index {

class AttributeDataInfo : public autil::legacy::Jsonizable
{
public:
    AttributeDataInfo() {}
    AttributeDataInfo(uint32_t count, uint32_t length) : uniqItemCount(count), maxItemLen(length) {}

    ~AttributeDataInfo() {}

    inline static const std::string ATTRIBUTE_DATA_INFO_FILE_NAME_ = "data_info";

public:
    Status Load(const std::shared_ptr<indexlib::file_system::IDirectory>& directory, bool& isExist);
    Status Store(const std::shared_ptr<indexlib::file_system::IDirectory>& directory);
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    std::string ToString() const { return ToJsonString(*this); }

    void FromString(const std::string& str) { FromJsonString(*this, str); }

public:
    uint32_t uniqItemCount = 0;
    uint32_t maxItemLen = 0;
    int64_t sliceCount = 0;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
