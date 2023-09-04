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

#include "autil/WorkItem.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/index/DocMapper.h"
#include "indexlib/index/inverted_index/config/TruncateOptionConfig.h"
#include "indexlib/index/inverted_index/config/TruncateProfile.h"
#include "indexlib/index/inverted_index/truncate/BucketMap.h"
#include "indexlib/index/inverted_index/truncate/TruncateAttributeReaderCreator.h"

namespace indexlib::index {

class SortWorkItem : public autil::WorkItem
{
public:
    SortWorkItem(const indexlibv2::config::TruncateProfile& truncProfile, uint32_t newDocCount,
                 TruncateAttributeReaderCreator* truncateAttrCreator, const std::shared_ptr<BucketMap>& bucketMap,
                 const std::shared_ptr<indexlibv2::config::ITabletSchema>& tabletSchema);
    ~SortWorkItem() = default;

public:
    void process() override;
    void destroy() override;

    // public:
    //     static int64_t EstimateMemoryUse(uint32_t totalDocCount);

private:
#pragma pack(push, 1)
    struct Doc {
    public:
        template <typename T>
        T& GetRef()
        {
            return *(T*)GetBuffer();
        }
        uint8_t* GetBuffer() { return (uint8_t*)&buffer; }

        template <typename T>
        const T& GetRef() const
        {
            return *(T*)GetBuffer();
        }
        const uint8_t* GetBuffer() const { return (uint8_t*)&buffer; }

    public:
        docid_t docId;
        uint64_t buffer;
        bool isNull = false;
    };
#pragma pack(pop)
    template <typename T>
    class DocComp
    {
    public:
        DocComp(bool desc) : _desc(desc) {}
        ~DocComp() {}

    public:
        bool operator()(const Doc& left, const Doc& right) const
        {
            // asc or desc, let the null value placed in the last position
            if (left.isNull) {
                return false;
            }
            if (right.isNull) {
                return true;
            }
            if (!_desc) {
                return left.GetRef<T>() < right.GetRef<T>();
            } else {
                return left.GetRef<T>() > right.GetRef<T>();
            }
        }

    private:
        bool _desc;
    };

private:
    void DoSort(Doc* begin, Doc* end, uint32_t sortDim);

    template <typename T>
    void SortTemplate(Doc* begin, Doc* end, uint32_t sortDim);

private:
    indexlibv2::config::TruncateProfile _truncateProfile;
    uint32_t _newDocCount;
    TruncateAttributeReaderCreator* _truncateAttrCreator;
    std::shared_ptr<BucketMap> _bucketMap;
    std::unique_ptr<Doc[]> _docInfos;
    std::shared_ptr<indexlibv2::config::ITabletSchema> _tabletSchema;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
