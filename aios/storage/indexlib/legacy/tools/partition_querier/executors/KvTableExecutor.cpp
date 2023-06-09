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
#include "indexlib/legacy/tools/partition_querier/executors/KvTableExecutor.h"

#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/kv/KVCommonDefine.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/kv/config/ValueConfig.h"
#include "indexlib/index/kv/kv_define.h"
#include "indexlib/legacy/tools/partition_querier/executors/AttrHelper.h"

using namespace std;
using namespace autil;
using namespace indexlib;

namespace indexlib::tools {

#define RETURN_IF_NOT_OK(...)                                                                                          \
    do {                                                                                                               \
        const Status _status = (__VA_ARGS__);                                                                          \
        if (!_status.IsOK()) {                                                                                         \
            return _status;                                                                                            \
        }                                                                                                              \
    } while (0);

AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib::tools, KvTableExecutor);

Status KvTableExecutor::QueryKVTable(const IndexPartitionReaderPtr& indexPartitionReaderPtr, regionid_t regionId,
                                     const indexlibv2::base::PartitionQuery& query,
                                     indexlibv2::base::PartitionResponse& partitionResponse)
{
    bool hasPkStrInput = query.pk_size() > 0;
    bool hasPkNumberInput = query.pknumber_size() > 0;
    const auto& kvReader = indexPartitionReaderPtr->GetKVReader(regionId);
    if (!kvReader) {
        return Status::InvalidArgs("failed to create kv reader");
    }
    auto attrs = ValidateAttrs(kvReader, vector<string> {query.attrs().begin(), query.attrs().end()});

    if (hasPkStrInput) {
        vector<string> pkList(query.pk().begin(), query.pk().end());
        RETURN_IF_NOT_OK(FillRows(kvReader, attrs, pkList, partitionResponse));
    } else if (hasPkNumberInput) {
        vector<uint64_t> pkList(query.pknumber().begin(), query.pknumber().end());
        RETURN_IF_NOT_OK(FillRows(kvReader, attrs, pkList, partitionResponse));
    } else {
        return Status::InvalidArgs("PKTable PartitionQuery should contain one and only one of pk/pknumber");
    }
    return Status::OK();
}

template <typename T>
Status KvTableExecutor::FillRows(const KVReaderPtr& kvReader, const vector<string>& attrs, const vector<T>& pkList,
                                 indexlibv2::base::PartitionResponse& partitionResponse)
{
    for (int i = 0; i < pkList.size(); ++i) {
        auto row = partitionResponse.add_rows();
        if constexpr (std::is_integral<T>::value) {
            RETURN_IF_NOT_OK(QueryAttrWithPk(kvReader, pkList[i], attrs, *row));
        } else {
            autil::StringView constStr(pkList[i].c_str(), pkList[i].size());
            RETURN_IF_NOT_OK(QueryAttrWithPk(kvReader, constStr, attrs, *row));
        }
    }

    AttrHelper::FillAttrInfo(attrs, partitionResponse);

    return Status::OK();
}

template <typename PkType>
Status KvTableExecutor::QueryAttrWithPk(const KVReaderPtr& kvReader, const PkType& pk,
                                        const std::vector<std::string>& attrs, indexlibv2::base::Row& row)
{
    autil::mem_pool::Pool pool;
    autil::StringView value;
    // 对多个 attr 的查询只取一次 doc Value
    if (!future_lite::interface::syncAwait(kvReader->GetAsync(pk, value, 0, indexlib::tsc_default, &pool))) {
        return Status::NotFound("no record found");
    }
    for (const auto& attr : attrs) {
        FormatKVResult(kvReader, attr, value, row);
    }
    if constexpr (std::is_integral<PkType>::value) {
        row.set_pk(std::to_string(pk));
    } else {
        row.set_pk(std::string(pk.data(), pk.size()));
    }
    return Status::OK();
}

vector<string> KvTableExecutor::ValidateAttrs(const KVReaderPtr& kvReader, const vector<string>& attrs)
{
    vector<string> kvAttrs;
    auto valueConfig = kvReader->GetKVIndexConfig()->GetValueConfig();
    for (size_t i = 0; i < valueConfig->GetAttributeCount(); i++) {
        kvAttrs.push_back(valueConfig->GetAttributeConfig(i)->GetAttrName());
    }
    return AttrHelper::ValidateAttrs(kvAttrs, attrs);
}

void KvTableExecutor::FormatKVResult(const KVReaderPtr& kvReader, const std::string& fieldName,
                                     const autil::StringView& docValue, indexlibv2::base::Row& row)
{
    auto attrValue = row.add_attrvalues();
    if (kvReader->GetValueType() == indexlibv2::index::KVVT_TYPED) {
        const auto& valueConfig = kvReader->GetKVIndexConfig()->GetValueConfig();
        const auto& attrConfig = valueConfig->GetAttributeConfig(0);
        FieldType fieldType = attrConfig->GetFieldType();
        AttrHelper::GetKvValue(docValue, fieldType, *attrValue);
    } else {
        AttrHelper::GetAttributeValue(docValue, kvReader->GetPackAttributeFormatter(), fieldName, *attrValue);
    }
}

#undef RETURN_IF_NOT_OK

} // namespace indexlib::tools
