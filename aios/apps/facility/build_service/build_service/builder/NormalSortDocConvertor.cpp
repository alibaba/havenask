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
#include "build_service/builder/NormalSortDocConvertor.h"

#include <limits>

using namespace std;
using namespace autil;
using namespace indexlib::index_base;
using namespace indexlib::config;
using namespace indexlib::document;

namespace build_service { namespace builder {
BS_LOG_SETUP(builder, NormalSortDocConvertor);

NormalSortDocConvertor::NormalSortDocConvertor() : _dataBuffer(NULL) {}

NormalSortDocConvertor::~NormalSortDocConvertor() { DELETE_AND_SET_NULL(_dataBuffer); }

bool NormalSortDocConvertor::init(const indexlibv2::config::SortDescriptions& sortDesc,
                                  const IndexPartitionSchemaPtr& schema)
{
    _sortDesc = sortDesc;
    _attrSchema = schema->GetAttributeSchema();
    _convertFuncs.resize(_sortDesc.size(), nullptr);
    _fieldIds.resize(_sortDesc.size(), 0);
    for (size_t i = 0; i < _sortDesc.size(); i++) {
        _convertFuncs[i] = initConvertFunc(sortDesc[i]);
        if (_convertFuncs[i] == NULL) {
            string errorMsg = "init emit key function failed.";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
        _fieldIds[i] = _attrSchema->GetAttributeConfig(_sortDesc[i].GetSortFieldName())->GetFieldId();
    }
    _dataBuffer = new DataBuffer();
    return true;
}

indexlibv2::index::SortValueConvertor::ConvertFunc
NormalSortDocConvertor::initConvertFunc(const indexlibv2::config::SortDescription& sortDesc)
{
    AttributeConfigPtr attrConfig = _attrSchema->GetAttributeConfig(sortDesc.GetSortFieldName());
    if (attrConfig == NULL || !attrConfig->GetFieldConfig()->SupportSort()) {
        return NULL;
    }
    return indexlibv2::index::SortValueConvertor::GenerateConvertor(sortDesc.GetSortPattern(),
                                                                    attrConfig->GetFieldType());
}

}} // namespace build_service::builder
