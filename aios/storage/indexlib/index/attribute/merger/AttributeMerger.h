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
#include <queue>
#include <string>

#include "autil/Log.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/index/DiskIndexerParameter.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/patch/PatchFileInfos.h"

namespace indexlibv2::framework {
class IndexTaskResourceManager;
}

namespace indexlibv2::index {

class DocMapper;

#define DECLARE_ATTRIBUTE_MERGER_IDENTIFIER(id)                                                                        \
    static std::string Identifier() { return std::string("attribute.merger." #id); }                                   \
    std::string GetIdentifier() const override { return Identifier(); }

class AttributePatchReader;
class AttributeDiskIndexer;
class AttributeMerger : public IIndexMerger
{
public:
    AttributeMerger() = default;
    virtual ~AttributeMerger() = default;

public:
    // inherit from IIndexMerger
    Status Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                const std::map<std::string, std::any>& params) override;
    Status Merge(const SegmentMergeInfos& segMergeInfos,
                 const std::shared_ptr<framework::IndexTaskResourceManager>& taskResourceManager) override;

    void SetPatchInfos(const PatchInfos& allPatchInfos) { _allPatchInfos = allPatchInfos; }

public:
    virtual std::string GetIdentifier() const = 0;
    virtual Status DoMerge(const SegmentMergeInfos& segMergeInfos, std::shared_ptr<DocMapper>& docMapper) = 0;
    std::shared_ptr<AttributeConfig> GetAttributeConfig() const { return _attributeConfig; }

    virtual std::pair<Status, std::shared_ptr<indexlib::file_system::IDirectory>>
    GetOutputDirectory(const std::shared_ptr<indexlib::file_system::IDirectory>& segDir) = 0;

public:
    static Status CreatePatchReader(const std::vector<IIndexMerger::SourceSegment>& srcMergeSegments,
                                    const PatchInfos& allPatchInfos,
                                    const std::shared_ptr<AttributeConfig>& attributeConfig,
                                    std::map<segmentid_t, std::shared_ptr<AttributePatchReader>>& segId2PatchReader);

protected:
    std::string GetAttributePath(const std::string& dir) const;
    Status StoreSliceInfo(const SegmentMergeInfos& segMergeInfos);
    std::tuple<Status, bool, std::shared_ptr<indexlib::file_system::IDirectory>>
    GetAttributeDirectory(const std::shared_ptr<framework::Segment>& segment) const
    {
        auto segDir = segment->GetSegmentDirectory();
        assert(segDir != nullptr);
        auto segmentDir = segDir->GetIDirectory();
        assert(segmentDir != nullptr);
        auto fsResult = segmentDir->GetDirectory(_attributeConfig->GetIndexCommonPath());
        if (!fsResult.OK()) {
            AUTIL_LOG(ERROR, "get attribute directory failed, segId[%d], attr [%s]", segment->GetSegmentId(),
                      _attributeConfig->GetIndexName().c_str());
            return {fsResult.Status(), true, fsResult.Value()};
        }

        auto attrDir = fsResult.result;
        const std::string& attrName = _attributeConfig->GetAttrName();

        auto [status, isExist] = attrDir->IsExist(attrName).StatusWith();
        RETURN3_IF_STATUS_ERROR(status, isExist, nullptr, "attribute directory IsExist failed");

        if (!isExist) {
            return {status, isExist, nullptr};
        } else {
            auto [status, directory] = attrDir->GetDirectory(attrName).StatusWith();
            return {status, true, directory};
        }
    }

    template <typename MergerType, typename ConfigType = std::shared_ptr<AttributeConfig>>
    Status DoMergePatches(const IIndexMerger::SegmentMergeInfos& segMergeInfos, const ConfigType& config)
    {
        if (!_needMergePatch) {
            return Status::OK();
        }
        // TODO(xiuchen & yanke) 1,2,3 -> 4,5的场景, patch信息只会写到5下面, 对于1,2,3->4的task直接返回
        MergerType merger(config, _allPatchInfos, nullptr);
        auto status = merger.Merge(segMergeInfos);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "merge attribute patch file failed. error [%s]", status.ToString().c_str());
            return status;
        }
        return Status::OK();
    }

    virtual Status LoadPatchReader(const std::vector<IIndexMerger::SourceSegment>& srcMergeSegments);

protected:
    struct SortMergeItem {
        docid_t newDocId;
        segmentid_t segmentId;

        SortMergeItem(docid_t newId, segmentid_t segId) : newDocId(newId), segmentId(segId) {}
    };

    struct SortMergeItemCompare {
        bool operator()(const SortMergeItem& left, const SortMergeItem& right)
        {
            return left.newDocId > right.newDocId;
        }
    };

    typedef std::priority_queue<SortMergeItem, std::vector<SortMergeItem>, SortMergeItemCompare> SortMergeItemHeap;

protected:
    std::shared_ptr<AttributeConfig> _attributeConfig;
    bool _needMergePatch = false;
    bool _supportNull = false;
    PatchInfos _allPatchInfos;
    std::string _docMapperName;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
