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
#include "indexlib/index/field_meta/meta/MetaFactory.h"

#include "indexlib/config/FieldConfig.h"
#include "indexlib/index/field_meta/Common.h"
#include "indexlib/index/field_meta/meta/DataStatisticsMeta.h"
#include "indexlib/index/field_meta/meta/FieldTokenCountMeta.h"
#include "indexlib/index/field_meta/meta/HistogramMeta.h"
#include "indexlib/index/field_meta/meta/MinMaxFieldMeta.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, MetaFactory);

std::pair<Status, std::vector<std::shared_ptr<IFieldMeta>>>
MetaFactory::CreateFieldMetas(const std::shared_ptr<FieldMetaConfig>& fieldMetaConfig)
{
#define CREATE_FIELD_META(fieldMetaType_, fieldMeta_, fieldType_, fieldMetaValueType_)                                 \
    do {                                                                                                               \
        if (fieldMetaType_ == metaName && fieldType == fieldType_) {                                                   \
            fieldMeta.reset(new fieldMeta_##FieldMeta<fieldMetaValueType_>(fieldMetaInfo));                            \
        }                                                                                                              \
    } while (0)

    auto metas = fieldMetaConfig->GetFieldMetaInfos();
    std::vector<std::shared_ptr<IFieldMeta>> fieldMetas;
    auto fieldConfigs = fieldMetaConfig->GetFieldConfigs();
    assert(fieldConfigs.size() == 1);
    auto fieldType = fieldConfigs[0]->GetFieldType();
    for (const auto& fieldMetaInfo : metas) {
        std::shared_ptr<IFieldMeta> fieldMeta;
        const std::string& metaName = fieldMetaInfo.metaName;
        if (metaName == FIELD_TOKEN_COUNT_META_STR) {
            fieldMeta.reset(new FieldTokenCountMeta(fieldMetaInfo));
        }
        if (metaName == DATA_STATISTICS_META_STR) {
            fieldMeta.reset(new DataStatisticsMeta(fieldMetaInfo));
        }
        if (metaName == HISTOGRAM_META_STR) {
            const auto& jsonMap = fieldMetaInfo.metaParams;
            auto maxValueIter = jsonMap.find(FieldMetaConfig::MAX_VALUE);
            auto minValueIter = jsonMap.find(FieldMetaConfig::MIN_VALUE);
            bool hasValueRange = false;
            if (maxValueIter != jsonMap.end() && minValueIter != jsonMap.end()) {
                hasValueRange = true;
            }
            if (fieldType == ft_uint64) {
                if (hasValueRange) {
                    fieldMeta.reset(new HistogramMeta<uint64_t, true>(fieldMetaInfo));
                } else {
                    fieldMeta.reset(new HistogramMeta<uint64_t, false>(fieldMetaInfo));
                }
            } else {
                if (hasValueRange) {
                    fieldMeta.reset(new HistogramMeta<int64_t, true>(fieldMetaInfo));
                } else {
                    fieldMeta.reset(new HistogramMeta<int64_t, false>(fieldMetaInfo));
                }
            }
        }

        CREATE_FIELD_META(MIN_MAX_META_STR, MinMax, ft_uint8, uint8_t);
        CREATE_FIELD_META(MIN_MAX_META_STR, MinMax, ft_uint16, uint16_t);
        CREATE_FIELD_META(MIN_MAX_META_STR, MinMax, ft_uint32, uint32_t);
        CREATE_FIELD_META(MIN_MAX_META_STR, MinMax, ft_uint64, uint64_t);
        CREATE_FIELD_META(MIN_MAX_META_STR, MinMax, ft_int8, int8_t);
        CREATE_FIELD_META(MIN_MAX_META_STR, MinMax, ft_int16, int16_t);
        CREATE_FIELD_META(MIN_MAX_META_STR, MinMax, ft_int32, int32_t);
        CREATE_FIELD_META(MIN_MAX_META_STR, MinMax, ft_int64, int64_t);
        CREATE_FIELD_META(MIN_MAX_META_STR, MinMax, ft_float, float);
        CREATE_FIELD_META(MIN_MAX_META_STR, MinMax, ft_double, double);
        if (!fieldMeta) {
            return {Status::Corruption("create filed meta [%s] failed", metaName.c_str()), {}};
        }
        fieldMetas.push_back(fieldMeta);
    }
#undef CREATE_FIELD_META
    return {Status::OK(), fieldMetas};
}

} // namespace indexlib::index
