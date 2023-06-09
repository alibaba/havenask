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
#include "indexlib/base/Constant.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/DocMapper.h"

namespace indexlibv2::index {

template <typename T>
class SegmentOutputMapper : private autil::NoCopyable
{
public:
    SegmentOutputMapper() {}

    void Init(const std::shared_ptr<DocMapper>& reclaimMap, const std::vector<std::pair<T, segmentid_t>>& outputInfos)
    {
        _docMapper = reclaimMap;
        for (size_t i = 0; i < outputInfos.size(); ++i) {
            _outputs.push_back(outputInfos[i].first);
            _segId2OutputIdx[outputInfos[i].second] = i;
        }
    }

    Status
    Init(std::shared_ptr<DocMapper> docMapper,
         const std::vector<std::shared_ptr<framework::SegmentMeta>>& outputSegMergeInfos,
         std::function<Status(const framework::SegmentMeta& segmentMeta, size_t outputIdx, T& output)> outputCreateFunc)

    {
        _docMapper = docMapper;
        for (size_t i = 0; i < outputSegMergeInfos.size(); ++i) {
            T outputData {};
            auto status = outputCreateFunc(*outputSegMergeInfos[i], i, outputData);
            if (!status.IsOK()) {
                AUTIL_LOG(ERROR, "create output data for segement ouput mapper failed.");
                return status;
            }
            _outputs.emplace_back(std::move(outputData));
            const auto& outputInfo = outputSegMergeInfos[i];
            _segId2OutputIdx[outputInfo->segmentId] = i;
        }
        return Status::OK();
    }

    T* GetOutput(docid_t docIdInPlan)
    {
        auto [targetSegId, targetDocId] = _docMapper->Map(docIdInPlan);
        auto it = _segId2OutputIdx.find(targetSegId);
        if (it == _segId2OutputIdx.end()) {
            return nullptr;
        }

        return &_outputs[it->second];
    }

    T* GetOutputBySegId(segmentid_t segId)
    {
        auto it = _segId2OutputIdx.find(segId);
        if (it == _segId2OutputIdx.end()) {
            return nullptr;
        }
        return &_outputs[it->second];
    }

    const std::shared_ptr<DocMapper>& GetReclaimMap() const { return _docMapper; }

    std::vector<T>& GetOutputs() { return _outputs; }

    void Clear()
    {
        _docMapper.reset();
        _segId2OutputIdx.clear();
        _outputs.clear();
    }

private:
    std::shared_ptr<DocMapper> _docMapper;
    std::map<int32_t, size_t> _segId2OutputIdx;
    std::vector<T> _outputs;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, SegmentOutputMapper, T);

} // namespace indexlibv2::index
