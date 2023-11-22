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

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_segment_reader.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename Key>
class PrimaryKeyAttributeSegmentReader : public SingleValueAttributeSegmentReader<Key>
{
public:
    PrimaryKeyAttributeSegmentReader(const config::AttributeConfigPtr& config)
        : SingleValueAttributeSegmentReader<Key>(config)
    {
    }
    ~PrimaryKeyAttributeSegmentReader() {}

private:
    file_system::FileReaderPtr CreateFileReader(const file_system::DirectoryPtr& directory, const std::string& fileName,
                                                bool supportFileCompress) const override
    {
        file_system::ReaderOption option(file_system::FSOT_LOAD_CONFIG);
        option.supportCompress = supportFileCompress;
        return directory->CreateFileReader(fileName, option);
    }

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::index
