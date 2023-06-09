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
#include "indexlib/index/normal/summary/local_disk_summary_segment_reader.h"

#include <sys/mman.h>

#include "indexlib/file_system/Directory.h"
#include "indexlib/index/data_structure/var_len_data_param_helper.h"
#include "indexlib/util/MMapAllocator.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;

using namespace indexlib::config;
using namespace indexlib::util;
using namespace indexlib::document;
using namespace indexlib::file_system;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, LocalDiskSummarySegmentReader);

LocalDiskSummarySegmentReader::LocalDiskSummarySegmentReader(const SummaryGroupConfigPtr& summaryGroupConfig)
    : mSummaryGroupConfig(summaryGroupConfig)
{
    auto param = VarLenDataParamHelper::MakeParamForSummary(summaryGroupConfig);
    auto fileCompressConfig = summaryGroupConfig->GetSummaryGroupDataParam().GetFileCompressConfig();
    if (param.dataCompressorName.empty() && fileCompressConfig) {
        param.dataCompressorName = "uncertain";
    }
    mDataReader.reset(new VarLenDataReader(param, true));
}

LocalDiskSummarySegmentReader::~LocalDiskSummarySegmentReader() {}

bool LocalDiskSummarySegmentReader::Open(const index_base::SegmentData& segmentData)
{
    DirectoryPtr summaryDirectory = segmentData.GetSummaryDirectory(true);
    DirectoryPtr directory = summaryDirectory;
    if (!mSummaryGroupConfig->IsDefaultGroup()) {
        const string& groupName = mSummaryGroupConfig->GetGroupName();
        directory = summaryDirectory->GetDirectory(groupName, true);
    }
    mDataReader->Init(segmentData.GetSegmentInfo()->docCount, directory, SUMMARY_OFFSET_FILE_NAME,
                      SUMMARY_DATA_FILE_NAME);
    return true;
}

bool LocalDiskSummarySegmentReader::GetDocument(docid_t localDocId, SearchSummaryDocument* summaryDoc) const
{
    assert(localDocId != INVALID_DOCID);
    autil::StringView data;
    if (!mDataReader->GetValue(localDocId, data, summaryDoc->getPool())) {
        return false;
    }

    char* value = (char*)data.data();
    uint32_t len = data.size();
    SummaryGroupFormatter formatter(mSummaryGroupConfig);
    bool ret = formatter.DeserializeSummary(summaryDoc, value, len);
    if (ret) {
        return true;
    }
    stringstream ss;
    ss << "Deserialize summary[docid = " << localDocId << "] FAILED.";
    INDEXLIB_THROW(util::IndexCollapsedException, "%s", ss.str().c_str());
    return false;
}

future_lite::coro::Lazy<vector<index::ErrorCode>>
LocalDiskSummarySegmentReader::GetDocument(const std::vector<docid_t>& docIds, autil::mem_pool::Pool* sessionPool,
                                           file_system::ReadOption readOption, const SearchSummaryDocVec* docs) noexcept
{
    assert(docs);
    assert(docIds.size() == docs->size());
    vector<autil::StringView> datas;
    auto ret = co_await mDataReader->GetValue(docIds, sessionPool, readOption, &datas);
    vector<index::ErrorCode> ec(docIds.size(), index::ErrorCode::OK);
    assert(ret.size() == docIds.size());
    for (size_t i = 0; i < ret.size(); ++i) {
        if (index::ErrorCode::OK != ret[i]) {
            ec[i] = ret[i];
            continue;
        }
        char* value = (char*)datas[i].data();
        uint32_t len = datas[i].size();
        SummaryGroupFormatter formatter(mSummaryGroupConfig);
        SearchSummaryDocument* summaryDoc = (*docs)[i];
        if (!formatter.DeserializeSummary(summaryDoc, value, len)) {
            ec[i] = index::ErrorCode::Runtime;
            IE_LOG(ERROR, "Deserialize summary[docid = %d] FAILED.", docIds[i]);
            continue;
        }
    }
    co_return ec;
}
}} // namespace indexlib::index
