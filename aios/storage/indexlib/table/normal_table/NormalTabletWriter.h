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
#include "indexlib/base/Status.h"
#include "indexlib/framework/Locator.h"
#include "indexlib/table/common/CommonTabletWriter.h"
#include "indexlib/table/normal_table/NormalTabletParallelBuilder.h"

namespace indexlib::index {
class PrimaryKeyIndexReader;
}

namespace indexlibv2::document {
class IDocumentBatch;
class DocumentRewriteChain;
} // namespace indexlibv2::document

namespace indexlibv2::table {
class NormalTabletModifier;
class NormalMemSegment;
class NormalTabletInfoHolder;
} // namespace indexlibv2::table

namespace indexlibv2::table {
class NormalTabletWriter : public table::CommonTabletWriter
{
public:
    NormalTabletWriter(const std::shared_ptr<config::TabletSchema>& schema, const config::TabletOptions* options);
    ~NormalTabletWriter();

    Status Open(const std::shared_ptr<framework::TabletData>& tabletData, const framework::BuildResource& buildResource,
                const framework::OpenOptions& openOptions) override;
    Status DoBuildAndReportMetrics(const std::shared_ptr<document::IDocumentBatch>& batch) override;

    std::unique_ptr<framework::SegmentDumper> CreateSegmentDumper() override;

private:
    void DispatchDocIds(document::IDocumentBatch* batch);
    void ValidateDocumentBatch(document::IDocumentBatch* batch);
    Status ModifyDocumentBatch(document::IDocumentBatch* batch);
    Status RewriteDocumentBatch(document::IDocumentBatch* batch);
    Status PrepareBuiltinIndex(const std::shared_ptr<framework::TabletData>& tabletData);
    Status PreparePrimaryKeyReader(const std::shared_ptr<framework::TabletData>& tabletData);
    Status PrepareModifier(const std::shared_ptr<framework::TabletData>& tabletData);
    std::vector<std::shared_ptr<document::IDocumentBatch>> SplitDocumentBatch(document::IDocumentBatch* batch) const;
    void CalculateDocumentBatchSplitSize(document::IDocumentBatch* batch, std::vector<size_t>& splitSizeVec) const;
    void UpdateNormalTabletInfo();
    void PostBuildActions();

public:
    indexlib::index::PrimaryKeyIndexReader* TEST_GetPKReader() { return _pkReader.get(); }

private:
    std::shared_ptr<NormalMemSegment> _normalBuildingSegment;
    std::shared_ptr<indexlib::index::PrimaryKeyIndexReader> _pkReader;
    std::shared_ptr<NormalTabletModifier> _modifier;
    std::shared_ptr<document::DocumentRewriteChain> _documentRewriteChain;
    framework::Locator _versionLocator;
    std::shared_ptr<NormalTabletInfoHolder> _normalTabletInfoHolder;
    std::shared_ptr<framework::TabletData> _buildTabletData;

    std::unique_ptr<indexlib::table::NormalTabletParallelBuilder> _parallelBuilder = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
