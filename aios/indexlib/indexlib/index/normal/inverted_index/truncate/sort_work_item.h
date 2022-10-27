#ifndef __INDEXLIB_SORT_WORK_ITEM_H
#define __INDEXLIB_SORT_WORK_ITEM_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/reclaim_map/reclaim_map.h"
#include "indexlib/config/truncate_option_config.h"
#include "indexlib/index/normal/inverted_index/truncate/bucket_map.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_attribute_reader_creator.h"
#include "indexlib/util/resource_control_work_item.h"

IE_NAMESPACE_BEGIN(index);

class SortWorkItem : public util::ResourceControlWorkItem
{
public:
    SortWorkItem(const config::TruncateProfile& truncProfile,
                 uint32_t newDocCount,
                 TruncateAttributeReaderCreator *truncateAttrCreator,
                 const BucketMapPtr &bucketMap);
    ~SortWorkItem();
public:
    void Process() override;
    void Destroy() override;
    //TODO: support estimate resource use
    int64_t GetRequiredResource() const override { return 0; }
public:
    static int64_t EstimateMemoryUse(uint32_t totalDocCount);

private:
#pragma pack(push,1)    
    struct Doc
    {
    public:
        template <typename T>
        T &GetRef() { return *(T*)GetBuffer() ; }
        uint8_t *GetBuffer() { return (uint8_t*)&mBuffer; }

        template <typename T>
        const T &GetRef() const { return *(T*)GetBuffer() ; }
        const uint8_t *GetBuffer() const { return (uint8_t*)&mBuffer; }
    public:
        docid_t mDocId;
        uint64_t mBuffer;
    };
#pragma pack(pop)
    template <typename T>
    class DocComp
    {
    public:
        DocComp(bool desc) : mDesc(desc) {}
        ~DocComp() {}
    public:
        bool operator() (const Doc &left, const Doc &right) const 
        {
            if (!mDesc)
            {
                return left.GetRef<T>() < right.GetRef<T>();
            }
            else
            {
                return left.GetRef<T>() > right.GetRef<T>();
            }
        }
    private:
        bool mDesc;
    };
private:
    void DoSort(Doc *begin, Doc *end, uint32_t sortDim);

    template <typename T>
    void SortTemplate(Doc *begin, Doc *end, uint32_t sortDim);
private:
    config::TruncateProfile mTruncProfile;
    uint32_t mNewDocCount;
    TruncateAttributeReaderCreator *mTruncateAttrCreator;
    BucketMapPtr mBucketMap;
    Doc* mDocInfos;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SortWorkItem);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SORT_WORK_ITEM_H
