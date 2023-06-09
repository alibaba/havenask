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

#include "indexlib/config/index_config.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/framework/legacy_index_reader_interface.h"
#include "indexlib/index_base/index_meta/version.h"

namespace indexlib::index {

class LegacyPrimaryKeyReaderInterface : public LegacyIndexReaderInterface
{
public:
    virtual void OpenWithoutPKAttribute(const indexlib::config::IndexConfigPtr& indexConfig,
                                        const indexlib::index_base::PartitionDataPtr& partitionData,
                                        bool forceReverseLookup = false) = 0;
    virtual AttributeReaderPtr GetLegacyPKAttributeReader() const = 0;
};

} // namespace indexlib::index
