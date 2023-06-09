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
#ifndef __INDEXLIB_FIXED_VALUE_ATTRIBUTE_MERGER_H
#define __INDEXLIB_FIXED_VALUE_ATTRIBUTE_MERGER_H

#include <memory>

#include "fslib/fslib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_merger.h"
#include "indexlib/index/normal/attribute/format/single_value_attribute_formatter.h"
#include "indexlib/index/normal/attribute/format/single_value_data_appender.h"
#include "indexlib/index/util/segment_output_mapper.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(file_system, FileWriter);
DECLARE_REFERENCE_CLASS(index, AttributeSegmentReader);

namespace indexlib { namespace index {

class FixedValueAttributeMerger : public AttributeMerger
{
protected:
    struct OutputData {
        size_t outputIdx = 0;
        size_t targetSegmentIndex = 0;
        docid_t targetSegmentBaseDocId = 0;
        AttributeFormatter* formatter = nullptr;
        SingleValueDataAppenderPtr dataAppender;

        OutputData() = default;

        bool operator==(const OutputData& other) const
        {
            return outputIdx == other.outputIdx && targetSegmentIndex == other.targetSegmentIndex &&
                   targetSegmentBaseDocId == other.targetSegmentBaseDocId;
        }

        template <typename T>
        void Set(docid_t globalDocId, const T& value, bool isNull)
        {
            assert(dataAppender);
            assert(dataAppender->GetValueCount() == (uint32_t)(globalDocId - targetSegmentBaseDocId));
            dataAppender->Append(value, isNull);
        }

        bool BufferFull() const
        {
            assert(dataAppender);
            return dataAppender->BufferFull();
        }

        void Flush()
        {
            assert(dataAppender);
            return dataAppender->Flush();
        }
    };

public:
    FixedValueAttributeMerger(bool needMergePatch);
    ~FixedValueAttributeMerger();
    DECLARE_ATTRIBUTE_MERGER_IDENTIFIER(FixedValue);

public:
    void Merge(const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
               const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override;

protected:
    virtual void MergeSegment(const MergerResource& resource, const index_base::SegmentMergeInfo& segMergeInfo,
                              const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                              const config::AttributeConfigPtr& attrConfig) = 0;

    virtual file_system::FileWriterPtr CreateDataFileWriter(const file_system::DirectoryPtr& attributeDir,
                                                            const std::string& temperatureLayer);

    virtual void PrepareCompressOutputData(const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
    {
        assert(false);
    }
    virtual void FlushCompressDataBuffer(OutputData& outputData) { assert(false); }
    virtual void DumpCompressDataBuffer() { assert(false); }

    void FlushDataBuffer(OutputData& outputData);

    virtual void PrepareOutputDatas(const ReclaimMapPtr& reclaimMap,
                                    const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) = 0;
    void DestroyBuffers();
    void CloseFiles();

protected:
    void MergeData(const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                   const index_base::OutputSegmentMergeInfos& outputSegMergeInfos);

    void MergePatches(const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                      const index_base::OutputSegmentMergeInfos& outputSegMergeInfos);

protected:
    static const uint32_t DEFAULT_RECORD_COUNT = 1024 * 1024;
    uint32_t mRecordSize;
    SegmentOutputMapper<OutputData> mSegOutputMapper;

private:
    friend class FixedValueAttributeMergerTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FixedValueAttributeMerger);
}} // namespace indexlib::index

#endif //__INDEXLIB_FIXED_VALUE_ATTRIBUTE_MERGER_H
