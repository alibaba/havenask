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
#include "autil/legacy/jsonizable.h"
#include "indexlib/index/common/Types.h"

namespace indexlibv2::config {
class IndexConfigDeserializeResource;
class InvertedIndexConfig;
} // namespace indexlibv2::config

namespace indexlib::index {

class InvertedIndexConfigSerializer
{
public:
    static void Deserialize(const autil::legacy::Any& any, size_t idxInJsonArray,
                            const indexlibv2::config::IndexConfigDeserializeResource& resource,
                            indexlibv2::config::InvertedIndexConfig* indexConfig);
    static void Serialize(const indexlibv2::config::InvertedIndexConfig& indexConfig,
                          autil::legacy::Jsonizable::JsonWrapper* json);
    static void DeserializeCommonFields(const autil::legacy::Any& any,
                                        indexlibv2 ::config::InvertedIndexConfig* indexConfig);

private:
    static void DeserializeOptionFlag(const autil::legacy::Jsonizable::JsonWrapper& json,
                                      indexlibv2::config::InvertedIndexConfig* indexConfig);
    static void CheckOptionFlag(const autil::legacy::json::JsonMap& jsonMap, InvertedIndexType invertedIndexType);
    static void DeserializeIndexFormatVersion(autil::legacy::Jsonizable::JsonWrapper& json,
                                              indexlibv2::config::InvertedIndexConfig* indexConfig);
    template <typename DictConfigType>
    static std::shared_ptr<DictConfigType>
    DeserializeDictConfig(const autil::legacy::Jsonizable::JsonWrapper& json,
                          const indexlibv2::config::IndexConfigDeserializeResource& resource);
    static void DeserializeFileCompressConfig(const autil::legacy::Jsonizable::JsonWrapper& json,
                                              const indexlibv2::config::IndexConfigDeserializeResource& resource,
                                              indexlibv2::config::InvertedIndexConfig* indexConfig);
    static void JsonizeShortListVbyteCompress(const indexlibv2::config::InvertedIndexConfig& indexConfig,
                                              autil::legacy::Jsonizable::JsonWrapper* json);
    static void JsonizeShortListVbyteCompress(autil::legacy::Jsonizable::JsonWrapper& json,
                                              indexlibv2::config::InvertedIndexConfig* indexConfig);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
