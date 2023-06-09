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
#include <memory>
#include <vector>

#include "autil/legacy/jsonizable.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/framework/TabletData.h"

namespace indexlibv2::table {

class SegmentMergePlan : public autil::legacy::Jsonizable
{
public:
    SegmentMergePlan();
    ~SegmentMergePlan();

public:
    void AddSrcSegment(segmentid_t segmentId)
    {
        _srcSegments.push_back(segmentId);
        std::sort(_srcSegments.begin(), _srcSegments.end());
    }

    void AddTargetSegment(segmentid_t targetSegmentId, framework::SegmentInfo segInfo)
    {
        _targetSegments.push_back(std::make_pair(targetSegmentId, segInfo));
    }

    size_t GetSrcSegmentCount() const { return _srcSegments.size(); }
    size_t GetTargetSegmentCount() const { return _targetSegments.size(); }
    segmentid_t GetSrcSegmentId(size_t idx) const { return _srcSegments[idx]; }

    segmentid_t GetTargetSegmentId(size_t idx) const
    {
        if (idx >= _targetSegments.size()) {
            return INVALID_SEGMENTID;
        }
        return _targetSegments[idx].first;
    }

    const framework::SegmentInfo& GetTargetSegmentInfo(size_t idx) const
    {
        assert(idx < _targetSegments.size());
        return _targetSegments[idx].second;
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    std::string ToString(bool compactFormat = false) const { return ToJsonString(*this, compactFormat); }

    void FromString(const std::string& str) { FromJsonString(*this, str); }

    static void UpdateTargetSegmentInfo(segmentid_t sourceSegId,
                                        const std::shared_ptr<framework::TabletData>& tabletData,
                                        framework::SegmentInfo* targetSegmentInfo);

    const std::vector<segmentid_t>& GetSrcSegments() const { return _srcSegments; }
    const std::vector<std::pair<segmentid_t, framework::SegmentInfo>>& GetTargetSegments() const
    {
        return _targetSegments;
    }

    auto& GetAllParameters() const { return _parameters; }
    template <typename T>
    bool GetParameter(const std::string& key, T& value) const;
    template <typename T>
    void AddParameter(const std::string& key, const T& value);

private:
    std::vector<segmentid_t> _srcSegments;
    std::vector<std::pair<segmentid_t, framework::SegmentInfo>> _targetSegments;
    autil::legacy::json::JsonMap _parameters;
};

template <typename T>
inline void SegmentMergePlan::AddParameter(const std::string& key, const T& value)
{
    _parameters[key] = value;
}

template <typename T>
bool SegmentMergePlan::GetParameter(const std::string& key, T& value) const
{
    auto iter = _parameters.find(key);
    if (iter == _parameters.end()) {
        return false;
    }
    value = autil::legacy::AnyCast<T>(iter->second);
    return true;
}

} // namespace indexlibv2::table
