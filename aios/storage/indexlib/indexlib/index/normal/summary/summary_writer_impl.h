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
#ifndef __INDEXLIB_SUMMARY_WRITER_IMPL_H
#define __INDEXLIB_SUMMARY_WRITER_IMPL_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/summary/summary_writer.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, LocalDiskSummaryWriter);

namespace indexlib { namespace index {

class SummaryWriterImpl : public SummaryWriter
{
public:
    SummaryWriterImpl();
    ~SummaryWriterImpl();
    DECLARE_SUMMARY_WRITER_IDENTIFIER(impl);

public:
    void Init(const config::SummarySchemaPtr& summarySchema, util::BuildResourceMetrics* buildResourceMetrics) override;

    // If any validation needs to be done, add a preprocess check instead of returning false here.
    void AddDocument(const document::SerializedSummaryDocumentPtr& document) override;

    void Dump(const file_system::DirectoryPtr& directory, autil::mem_pool::PoolBase* dumpPool = NULL) override;
    const SummarySegmentReaderPtr CreateInMemSegmentReader() override;

private:
    typedef std::vector<LocalDiskSummaryWriterPtr> GroupWriterVec;
    GroupWriterVec mGroupWriterVec;
    config::SummarySchemaPtr mSummarySchema;

private:
    friend class SummaryWriterTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SummaryWriterImpl);
}} // namespace indexlib::index

#endif //__INDEXLIB_SUMMARY_WRITER_IMPL_H
