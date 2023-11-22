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
#include "autil/NoCopyable.h"
#include "indexlib/base/Status.h"
#include "indexlib/document/IDocumentParser.h"
#include "indexlib/document/normal/TokenizeDocumentConvertor.h"
#include "indexlib/util/metrics/Monitor.h"

namespace indexlibv2::analyzer {
class IAnalyzerFactory;
}

namespace indexlib::util {
class AccumulativeCounter;
}

namespace indexlibv2::document {
class RawDocument;
class SingleDocumentParser;
class IDocumentRewriter;
class NormalDocument;

class NormalDocumentParser : public IDocumentParser
{
public:
    explicit NormalDocumentParser(const std::shared_ptr<analyzer::IAnalyzerFactory>& analyzerFactory,
                                  bool needParseRawDoc);
    virtual ~NormalDocumentParser();

public:
    Status Init(const std::shared_ptr<config::ITabletSchema>& schema,
                const std::shared_ptr<DocumentInitParam>& initParam) override;
    std::unique_ptr<IDocumentBatch> Parse(const std::string& docString, const std::string& docFormat) const override;

    std::pair<Status, std::unique_ptr<IDocumentBatch>> Parse(ExtendDocument& extendDoc) const override;

    std::pair<Status, std::unique_ptr<IDocumentBatch>> Parse(const autil::StringView& serializedData) const override;

    std::pair<Status, std::unique_ptr<IDocumentBatch>>
    ParseRawDoc(const std::shared_ptr<RawDocument>& rawDoc) const override;

    std::pair<Status, std::unique_ptr<IDocumentBatch>>
    BatchParse(document::IMessageIterator* inputIterator) const override;

protected:
    virtual std::unique_ptr<SingleDocumentParser> CreateSingleDocumentParser() const;

private:
    bool IsEmptyUpdate(const std::shared_ptr<NormalDocument>& doc) const;
    Status InitFromDocumentParam(const std::shared_ptr<DocumentInitParam>& initParam);
    void ReportModifyFieldQps(const std::shared_ptr<NormalDocument>& doc) const;
    void ReportIndexAddToUpdateQps(const NormalDocument& doc) const;
    void ReportAddToUpdateFailQps(const NormalDocument& doc) const;
    std::pair<Status, std::shared_ptr<NormalDocument>>
    ParseSingleDocument(const autil::StringView& serializedData) const;

private:
    std::shared_ptr<analyzer::IAnalyzerFactory> _analyzerFactory;
    std::shared_ptr<TokenizeDocumentConvertor> _tokenizeDocConvertor;
    std::shared_ptr<config::ITabletSchema> _schema;
    std::shared_ptr<SingleDocumentParser> _mainParser;
    std::vector<std::shared_ptr<IDocumentRewriter>> _docRewriters;
    IE_DECLARE_METRIC(UselessUpdateQps);
    IE_DECLARE_METRIC(IndexUselessUpdateQps);
    IE_DECLARE_METRIC(ParseFailedQps);
    IE_DECLARE_METRIC(ModifyFieldQps);
    IE_DECLARE_METRIC(IndexUpdateQps);
    IE_DECLARE_METRIC(AttributeUpdateQps);
    IE_DECLARE_METRIC(SummaryUpdateQps);
    IE_DECLARE_METRIC(SingleModifyFieldQps);
    IE_DECLARE_METRIC(DocParserQps);
    IE_DECLARE_METRIC(IndexAddToUpdateQps);
    IE_DECLARE_METRIC(AddToUpdateFailedQps);
    std::shared_ptr<indexlib::util::AccumulativeCounter> _attributeConvertErrorCounter;
    bool _supportNull = false;
    bool _enableModifyFieldStat = false;
    bool _needParseRawDoc = false;

public:
    inline static const std::string ATTRIBUTE_CONVERT_ERROR_COUNTER_NAME = "bs.processor.attributeConvertError";

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::document
