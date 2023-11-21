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

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/util/SourceFieldExtractorUtil.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/ITabletDocIterator.h"

namespace build_service { namespace reader {

class SourceFieldExtractorDocIterator : public indexlibv2::framework::ITabletDocIterator
{
public:
    struct FieldInfo {
        std::shared_ptr<util::SourceFieldExtractorUtil::SourceFieldParam> param;
        std::string defaultValue;
    };

public:
    SourceFieldExtractorDocIterator(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema);
    ~SourceFieldExtractorDocIterator();

public:
    indexlibv2::Status Init(const std::shared_ptr<indexlibv2::framework::TabletData>& tabletData,
                            std::pair<uint32_t /*0-99*/, uint32_t /*0-99*/> rangeInRatio,
                            const std::shared_ptr<indexlibv2::framework::MetricsManager>& metricsManager,
                            const std::optional<std::vector<std::string>>& requiredFields,
                            const std::map<std::string, std::string>& params) override;
    indexlibv2::Status Next(indexlibv2::document::RawDocument* rawDocument, std::string* checkpoint,
                            indexlibv2::framework::Locator::DocInfo* docInfo) override;
    bool HasNext() const override;
    indexlibv2::Status Seek(const std::string& checkpoint) override;

private:
    bool extractSourceFieldValue(const std::string& rawStr,
                                 const util::SourceFieldExtractorUtil::SourceFieldParam& param,
                                 const std::string& defaultValue, std::string& fieldValue);

    static std::shared_ptr<util::SourceFieldExtractorUtil::SourceFieldParam>
    getSourceFieldParam(const std::shared_ptr<indexlibv2::config::FieldConfig>& fieldConfig);

private:
    std::shared_ptr<indexlibv2::framework::ITabletDocIterator> _docIter;
    std::shared_ptr<indexlibv2::config::ITabletSchema> _targetSchema;
    std::map<std::string, std::string> _defaultValues;
    std::map<std::string, FieldInfo> _fieldInfos;
    std::set<std::string> _requiredFields;
    util::SourceFieldMap _sourceField2ParsedInfo;
    friend class SourceFieldExtractorDocIteratorTest;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SourceFieldExtractorDocIterator);

}} // namespace build_service::reader
