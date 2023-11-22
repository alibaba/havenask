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
#include "indexlib/base/Types.h"
#include "indexlib/framework/Version.h"

namespace indexlibv2::index {
class DeletionMapIndexReader;
}

namespace indexlibv2::framework {
class TabletData;
}

namespace indexlibv2::table {
class NormalTabletMeta;

struct TabletInfoHint {
    int32_t infoVersion = -1;
    segmentid_t lastRtSegmentId = INVALID_SEGMENTID;
    size_t lastRtSegmentDocCount = 0;
};

/*
 * 1.通过Get(UN)OrderedDocIdRanges 接口可以知道哪些docid range
 * 的文档是根据sort description排序的(有序部分以segment维度组织)，查询层可根据该信息优化最终结果的归并
 *
 * 2.GetGlobalId GetDocId GetDiffDocIdRanges GetTabletInfoHint 接口可用于查询层缓存结果
 * 在查询时取TabletInfoHint，再将当前的查询请求的docid序列转成global docid进行缓存，下次
 * 对于相同的查询请求可奖global docid再转回docid即可，另外由于有新增的实时和增量切换，需要再
 * 通过GetDiffDocIdRanges接口获取需要额外补查的结果
 *
 *
 * NormalTabletInfo有版本概念，每次重新打开reader/writer(reopen version/ reopen newsegment)时版本会+1
 * 并且记录若干个版本的segment排序信息，对于某个历史版本的查询的global
 * docid再当前版本进行查询时，有可能会被淘汰，需要重查 淘汰的条件有：
 *  a.这个docid对应的数据本身已经不存在了(segment被回收/merge/，doc被删除)
 *  b.这个cache结果太老了，cache整体失效，全部冲重查
 *  c.这个segment的排序状态发生了改变，docid发生了sort，整个segment需要重查
 */

class NormalTabletInfo
{
public:
    struct SegmentSortInfo {
        segmentid_t segmentId = INVALID_SEGMENTID;
        bool isSorted = false;
    };
    struct HistorySegmentInfos {
        int32_t minVersion = 0;
        std::vector<std::vector<SegmentSortInfo>> infos;
    };

    NormalTabletInfo() = default;
    ~NormalTabletInfo() = default;
    NormalTabletInfo(const NormalTabletInfo& other) = default;

    // for sort info
    bool GetOrderedDocIdRanges(DocIdRangeVector& ranges) const;
    bool GetUnorderedDocIdRange(DocIdRange& range) const;

    // for result cache
    globalid_t GetGlobalId(docid_t docId) const;
    docid_t GetDocId(const TabletInfoHint& infoHint, globalid_t gid) const;
    bool GetDiffDocIdRanges(const TabletInfoHint& infoHint, DocIdRangeVector& docIdRanges) const;
    const TabletInfoHint& GetTabletInfoHint() const { return _tabletInfoHint; }

    // meta info
    std::shared_ptr<NormalTabletMeta> GetTabletMeta() const { return _tabletMeta; }
    size_t GetTotalDocCount() const { return _totalDocCount; }
    size_t GetSegmentCount() const { return _segmentCount; }
    size_t GetIncDocCount() const { return _incDocCount; }
    size_t GetDelDocCount() const { return _delDocCount; }
    const std::vector<docid_t>& GetBaseDocIds() const { return _baseDocIds; }

    // for build
    std::unique_ptr<NormalTabletInfo> Clone() const;
    bool NeedUpdate(size_t lastRtSegDocCount) const;
    void UpdateDocCount(size_t lastRtSegDocCount);

    // for init
    HistorySegmentInfos GetSegmentSortInfoHis() const { return _historySegmentInfos; }
    void Init(const std::shared_ptr<framework::TabletData>& tabletData,
              const std::shared_ptr<NormalTabletMeta>& tabletMeta,
              const std::shared_ptr<index::DeletionMapIndexReader>& deletionMapReader,
              const HistorySegmentInfos& historySegmentMap);

private:
    void InitHistorySegMap(const std::shared_ptr<framework::TabletData>& tabletData,
                           const HistorySegmentInfos& historySegmentMap);
    void InitCurrentInfo(const std::shared_ptr<framework::TabletData>& tabletData);
    void InitDocCount(const std::shared_ptr<framework::TabletData>& tabletData);
    void InitOrderedDocIdRanges(const std::shared_ptr<framework::TabletData>& tabletData);
    void InitUnorderedDocIdRange();
    void InitTabletInfoHint(const std::shared_ptr<framework::TabletData>& tabletData);
    bool GetHistoryMapOffset(int32_t version, int32_t* offset) const;

    docid_t GetSegmentDocCount(size_t idx) const;
    void MigrageDocIdRanges(DocIdRangeVector& current, const DocIdRange& toAppend) const;

private:
    static constexpr uint32_t MAX_HISTORY_SEG_INFO_COUNT = 64;

    // for docid convert and get diff docid range
    std::shared_ptr<index::DeletionMapIndexReader> _deletionMapReader;
    HistorySegmentInfos _historySegmentInfos;
    TabletInfoHint _tabletInfoHint;

    // 记录若干个当时的用当时的info
    // version出的某个segment的结果，在当前版本中对应的segment下标，如果找不到说明这个结果失效了 失效的原因有3种
    // 1.这个version info太旧了
    // 2.这个segment没有了
    // 3.这个segment的排序属性发生变化了(非sort变为了sort)
    // unordered_map<segmentid, current segment idx>>
    std::vector<std::unordered_map<segmentid_t, int32_t>> _historySegToIdxMap;
    // [_minHisVersion, _minHisVersion+1, ...., _currentInfoVersion]

    // basic info
    std::shared_ptr<NormalTabletMeta> _tabletMeta;
    std::vector<docid_t> _baseDocIds;
    size_t _segmentCount = 0;
    size_t _delDocCount = 0;
    size_t _totalDocCount = 0;
    size_t _incDocCount = 0;

    // for sort info
    std::vector<DocIdRange> _orderRanges;
    DocIdRange _unorderRange;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
