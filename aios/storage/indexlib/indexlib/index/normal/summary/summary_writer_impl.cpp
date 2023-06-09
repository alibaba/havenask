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
#include "indexlib/index/normal/summary/summary_writer_impl.h"

#include "indexlib/config/summary_schema.h"
#include "indexlib/document/index_document/normal_document/group_field_formatter.h"
#include "indexlib/document/index_document/normal_document/serialized_summary_document.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/normal/summary/in_mem_summary_segment_reader_container.h"
#include "indexlib/index/normal/summary/local_disk_summary_writer.h"
#include "indexlib/index/normal/summary/summary_segment_reader.h"
#include "indexlib/index/util/build_profiling_metrics.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::document;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::util;

namespace {
class ScopeBuildProfilingReporter
{
public:
    ScopeBuildProfilingReporter(indexlib::index::BuildProfilingMetricsPtr& metric) : _beginTime(0), _metric(metric)
    {
        if (_metric) {
            _beginTime = autil::TimeUtility::currentTime();
        }
    }

    ~ScopeBuildProfilingReporter()
    {
        if (_metric) {
            int64_t endTime = autil::TimeUtility::currentTime();
            _metric->CollectAddSummaryTime(endTime - _beginTime);
        }
    }

private:
    int64_t _beginTime;
    indexlib::index::BuildProfilingMetricsPtr& _metric;
};
} // namespace

namespace indexlib { namespace index {
IE_LOG_SETUP(index, SummaryWriterImpl);

SummaryWriterImpl::SummaryWriterImpl() {}

SummaryWriterImpl::~SummaryWriterImpl() {}

void SummaryWriterImpl::Init(const SummarySchemaPtr& summarySchema, BuildResourceMetrics* buildResourceMetrics)
{
    assert(summarySchema);
    assert(summarySchema->NeedStoreSummary());

    mSummarySchema = summarySchema;
    for (summarygroupid_t groupId = 0; groupId < summarySchema->GetSummaryGroupConfigCount(); ++groupId) {
        const SummaryGroupConfigPtr& groupConfig = mSummarySchema->GetSummaryGroupConfig(groupId);
        if (groupConfig->NeedStoreSummary()) {
            mGroupWriterVec.push_back(LocalDiskSummaryWriterPtr(new LocalDiskSummaryWriter()));
            mGroupWriterVec[groupId]->Init(groupConfig, buildResourceMetrics);
        } else {
            mGroupWriterVec.push_back(LocalDiskSummaryWriterPtr());
        }
    }
}

void SummaryWriterImpl::AddDocument(const SerializedSummaryDocumentPtr& document)
{
    ScopeBuildProfilingReporter reporter(mProfilingMetrics);

    assert(document);
    if (mGroupWriterVec.size() == 1) {
        // only default group
        assert(document->GetDocId() == (docid_t)mGroupWriterVec[0]->GetNumDocuments());
        assert(mGroupWriterVec[0]);
        StringView data(document->GetValue(), document->GetLength());
        mGroupWriterVec[0]->AddDocument(data);
        return;
    }

    // mv to SummaryFormatter::DeserializeSummaryDoc
    const char* cursor = document->GetValue();
    for (size_t i = 0; i < mGroupWriterVec.size(); ++i) {
        if (!mGroupWriterVec[i]) {
            continue;
        }
        assert(document->GetDocId() == (docid_t)mGroupWriterVec[i]->GetNumDocuments());
        uint32_t len = GroupFieldFormatter::ReadVUInt32(cursor);
        StringView data(cursor, len);
        cursor += len;
        mGroupWriterVec[i]->AddDocument(data);
    }
    assert(cursor - document->GetValue() == (int64_t)document->GetLength());
}

void SummaryWriterImpl::Dump(const DirectoryPtr& directory, PoolBase* dumpPool)
{
    if (mGroupWriterVec.empty()) {
        return;
    }
    assert((size_t)mSummarySchema->GetSummaryGroupConfigCount() == mGroupWriterVec.size());
    assert(DEFAULT_SUMMARYGROUPID == 0);
    if (mGroupWriterVec[0]) {
        assert(mSummarySchema->GetSummaryGroupConfig(0)->IsDefaultGroup());
        mGroupWriterVec[0]->Dump(directory, dumpPool, mTemperatureLayer);
    }

    for (size_t i = 1; i < mGroupWriterVec.size(); ++i) {
        if (!mGroupWriterVec[i]) {
            continue;
        }
        const string& groupName = mGroupWriterVec[i]->GetGroupName();
        DirectoryPtr realDirectory = directory->MakeDirectory(groupName);
        mGroupWriterVec[i]->Dump(realDirectory, dumpPool, mTemperatureLayer);
    }
}

const SummarySegmentReaderPtr SummaryWriterImpl::CreateInMemSegmentReader()
{
    InMemSummarySegmentReaderContainerPtr inMemSummarySegmentReaderContainer(new InMemSummarySegmentReaderContainer());
    for (size_t i = 0; i < mGroupWriterVec.size(); ++i) {
        if (mGroupWriterVec[i]) {
            inMemSummarySegmentReaderContainer->AddReader(mGroupWriterVec[i]->CreateInMemSegmentReader());
        } else {
            inMemSummarySegmentReaderContainer->AddReader(InMemSummarySegmentReaderPtr());
        }
    }
    return inMemSummarySegmentReaderContainer;
}
}} // namespace indexlib::index
