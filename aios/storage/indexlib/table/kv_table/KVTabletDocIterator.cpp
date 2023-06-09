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
#include "indexlib/table/kv_table/KVTabletDocIterator.h"

#include "indexlib/framework/TabletData.h"
#include "indexlib/index/common/field_format/pack_attribute/PlainFormatEncoder.h"
#include "indexlib/index/kv/AdapterIgnoreFieldCalculator.h"
#include "indexlib/index/kv/KVShardRecordIterator.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/kv/config/ValueConfig.h"

using namespace std;
using namespace autil;
using namespace indexlibv2::config;
using namespace indexlibv2::index;
using namespace indexlibv2::framework;

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.index, KVTabletDocIterator);

Status KVTabletDocIterator::InitFromTabletData(const shared_ptr<TabletData>& tabletData)
{
    _tabletSchema = tabletData->GetOnDiskVersionReadSchema();
    auto indexConfigs = _tabletSchema->GetIndexConfigs();
    assert(indexConfigs.size() <= 2);
    std::shared_ptr<config::ValueConfig> pkValueConfig;
    for (const auto& indexConfig : indexConfigs) {
        if (index::KV_RAW_KEY_INDEX_NAME != indexConfig->GetIndexName()) {
            _kvIndexConfig = dynamic_pointer_cast<config::KVIndexConfig>(indexConfig);
            _valueConfig = _kvIndexConfig->GetValueConfig();
        } else {
            auto pkValueIndexConfig = dynamic_pointer_cast<config::KVIndexConfig>(indexConfig);
            pkValueConfig = pkValueIndexConfig->GetValueConfig();
        }
    }
    auto status = InitFormatter(_valueConfig, pkValueConfig);
    RETURN_IF_STATUS_ERROR(status, "init formatter failed for indx[%s]", _kvIndexConfig->GetIndexName().c_str());

    _ignoreFieldCalculator = std::make_shared<index::AdapterIgnoreFieldCalculator>();
    if (!_ignoreFieldCalculator->Init(_kvIndexConfig, tabletData.get())) {
        return Status::InternalError("init AdapterIgnoreFieldCalculator fail for index [%s]",
                                     _kvIndexConfig->GetIndexName().c_str());
    }
    return Status::OK();
}

unique_ptr<IShardRecordIterator> KVTabletDocIterator::CreateShardDocIterator() const
{
    return make_unique<KVShardRecordIterator>();
}

autil::StringView KVTabletDocIterator::PreprocessPackValue(const StringView& value)
{
    StringView packValue;
    if (_valueConfig->GetFixedLength() == -1) {
        size_t encodeCountLen = 0;
        MultiValueFormatter::decodeCount(value.data(), encodeCountLen);
        packValue = StringView(value.data() + encodeCountLen, value.size() - encodeCountLen);
        if (_plainFormatEncoder && !_plainFormatEncoder->Decode(packValue, &_pool, packValue)) {
            AUTIL_LOG(ERROR, "decode plain format error.");
        }
    } else {
        assert(!_plainFormatEncoder);
        packValue = value;
    }
    return packValue;
}

} // namespace indexlibv2::table
