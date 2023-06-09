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

#include <algorithm>
#include <functional>
#include <memory>

#include "autil/Log.h"
#include "indexlib/config/SortDescription.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/deletionmap/DeletionMapDiskIndexer.h"
#include "indexlib/table/index_task/merger/SegmentMergePlan.h"
#include "indexlib/table/normal_table/index_task/ReclaimMap.h"
#include "indexlib/table/normal_table/index_task/SegmentMapper.h"

namespace indexlib { namespace index {
class MultiComparator;
class DocInfoAllocator;
class MultiAttributeEvaluator;
}} // namespace indexlib::index

namespace indexlibv2 { namespace config {
class AttributeConfig;

}} // namespace indexlibv2::config

namespace indexlibv2 { namespace table {

// ReclaimMap记录一个SegmentMergePlan中doc旧docid和新docid的映射关系
class SortedReclaimMap : public ReclaimMap
{
public:
    typedef std::function<std::pair<Status, segmentindex_t>(segmentid_t, docid_t)> SegmentSplitHandler;

public:
    SortedReclaimMap(std::string name, framework::IndexTaskResourceType type) : ReclaimMap(std::move(name), type) {}

    ~SortedReclaimMap() {}

public:
    Status Init(const std::shared_ptr<config::TabletSchema>& schema, const config::SortDescriptions& sortDescriptions,
                const SegmentMergePlan& segmentMergePlan, const std::shared_ptr<framework::TabletData>& tabletData,
                const ReclaimMap::SegmentSplitHandler& segmentSplitHandler);

    Status TEST_Init(segmentid_t baseSegmentId,
                     const std::vector<std::pair<docid_t, std::shared_ptr<framework::Segment>>>& srcSegments,
                     const std::vector<std::shared_ptr<indexlibv2::config::AttributeConfig>>& attrConfigs,
                     const std::vector<indexlibv2::config::SortPattern>& sortPatterns,
                     const SegmentSplitHandler& segmentSplitHandler);

private:
    void InitMultiComparator(const std::vector<std::shared_ptr<indexlibv2::config::AttributeConfig>>& attrConfigs,
                             const std::vector<config::SortPattern>& sortPatterns,
                             const std::shared_ptr<indexlib::index::MultiComparator>& multiComparator,
                             const std::shared_ptr<indexlib::index::DocInfoAllocator>& docInfoAllocator) const;
    Status
    InitMultiAttrEvaluator(const std::pair<docid_t, std::shared_ptr<framework::Segment>>& srcSegment,
                           const std::vector<std::shared_ptr<indexlibv2::config::AttributeConfig>>& attrConfigs,
                           const std::shared_ptr<indexlib::index::DocInfoAllocator>& docInfoAllocator,
                           const std::shared_ptr<indexlib::index::MultiAttributeEvaluator>& multiEvaluatorPtr) const;
    Status SortByWeightInit(size_t targetSegmentCount, segmentid_t baseSegmentId,
                            const std::vector<std::pair<docid_t, std::shared_ptr<framework::Segment>>>& srcSegments,
                            const std::vector<std::shared_ptr<indexlibv2::config::AttributeConfig>>& attrConfigs,
                            const std::vector<config::SortPattern>& sortPatterns,
                            const SegmentSplitHandler& segmentSplitHandler);

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlibv2::table
