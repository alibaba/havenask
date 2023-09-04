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
#include "suez/drc/BinaryRecordDecoder.h"

#include "autil/Log.h"
#include "indexlib/index/common/field_format/pack_attribute/PackAttributeFormatter.h"
#include "indexlib/index/kv/KVTypeId.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/kv/config/ValueConfig.h"

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez.drc, BinaryRecordDecoder);

BinaryRecordDecoder::BinaryRecordDecoder() {}

BinaryRecordDecoder::~BinaryRecordDecoder() {}

bool BinaryRecordDecoder::init(const indexlibv2::config::KVIndexConfig &indexConfig) {
    auto typeId = indexlibv2::index::MakeKVTypeId(indexConfig, nullptr);
    if (!typeId.isVarLen) {
        AUTIL_LOG(ERROR, "only support var length kv record");
        return false;
    }

    auto [status, packAttrConfig] = indexConfig.GetValueConfig()->CreatePackAttributeConfig();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "create pack attribute config for %s failed", indexConfig.GetIndexName().c_str());
        return false;
    }
    _packAttrFormatter = std::make_unique<indexlibv2::index::PackAttributeFormatter>();
    if (!_packAttrFormatter->Init(packAttrConfig)) {
        AUTIL_LOG(ERROR, "init PackAttributeFormatter for %s failed", indexConfig.GetIndexName().c_str());
        return false;
    }
    return true;
}

BinaryRecord BinaryRecordDecoder::decode(const autil::StringView &data, autil::mem_pool::Pool *pool) {
    return BinaryRecord(_packAttrFormatter.get(), pool, data);
}

} // namespace suez
