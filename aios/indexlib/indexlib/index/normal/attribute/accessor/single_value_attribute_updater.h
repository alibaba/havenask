#ifndef __INDEXLIB_SINGLE_VALUE_ATTRIBUTE_UPDATER_H
#define __INDEXLIB_SINGLE_VALUE_ATTRIBUTE_UPDATER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/hash_map.h"
#include "indexlib/index_define.h"
#include "indexlib/common/field_format/attribute/type_info.h"
#include "indexlib/index/normal/attribute/accessor/attribute_updater.h"
#include "indexlib/index/normal/attribute/accessor/attribute_updater_creator.h"
#include "indexlib/config/pack_attribute_config.h"
#include <unordered_map>
#include <autil/mem_pool/pool_allocator.h>

IE_NAMESPACE_BEGIN(index);

template<typename T>
class SingleValueAttributeUpdater : public AttributeUpdater
{
public:
    SingleValueAttributeUpdater(
            util::BuildResourceMetrics* buildResourceMetrics,
            segmentid_t segId, 
            const config::AttributeConfigPtr& attrConfig);
    
    ~SingleValueAttributeUpdater() {}

public:
    class Creator : public AttributeUpdaterCreator
    {
    public:
        FieldType GetAttributeType() const 
        {
            return common::TypeInfo<T>::GetFieldType();
        }

        AttributeUpdater* Create(
                util::BuildResourceMetrics* buildResourceMetrics,
                segmentid_t segId, 
                const config::AttributeConfigPtr& attrConfig) const 
        {
            return new SingleValueAttributeUpdater<T>(
                    buildResourceMetrics, segId, attrConfig);
        }
    };

public:
    void Update(docid_t docId,
                              const autil::ConstString& attributeValue) override;

    void Dump(const file_system::DirectoryPtr& attributeDir, 
                            segmentid_t srcSegment = INVALID_SEGMENTID) override;

private:
    void UpdateBuildResourceMetrics()
    {
        if (!mBuildResourceMetricsNode)
        {
            return;
        }
        int64_t poolSize = mSimplePool.getUsedBytes();
        int64_t dumpTempMemUse = sizeof(docid_t) * mHashMap.size() +
                                 GetPatchFileWriterBufferSize();

        int64_t dumpFileSize = EstimateDumpFileSize(mHashMap.size() *
                sizeof(typename HashMap::value_type));
        
        mBuildResourceMetricsNode->Update(
                util::BMT_CURRENT_MEMORY_USE, poolSize);
        mBuildResourceMetricsNode->Update(
                util::BMT_DUMP_TEMP_MEMORY_SIZE, dumpTempMemUse);
        mBuildResourceMetricsNode->Update(
                util::BMT_DUMP_FILE_SIZE, dumpFileSize);
    }
    
private:
    typedef autil::mem_pool::pool_allocator<std::pair<const docid_t, T> > AllocatorType;
    typedef std::unordered_map<docid_t, T,
                               std::hash<docid_t>,
                               std::equal_to<docid_t>,
                               AllocatorType> HashMap;   
    const static size_t MEMORY_USE_ESTIMATE_FACTOR = 2;
    HashMap mHashMap;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, SingleValueAttributeUpdater);

template<typename T>
SingleValueAttributeUpdater<T>::SingleValueAttributeUpdater(
        util::BuildResourceMetrics* buildResourceMetrics,
        segmentid_t segId, 
        const config::AttributeConfigPtr& attrConfig)
    : AttributeUpdater(buildResourceMetrics, segId, attrConfig)
    , mHashMap(autil::mem_pool::pool_allocator<std::pair<const docid_t, T> >(&mSimplePool))
{
}

template<typename T>
void SingleValueAttributeUpdater<T>::Update(docid_t docId, 
        const autil::ConstString& attributeValue)
{
    size_t lastCount = mHashMap.size();
    mHashMap[docId] = *(T*)attributeValue.data();
    if (mHashMap.size() > lastCount)
    {
        UpdateBuildResourceMetrics();
    }

}

template<typename T>
void SingleValueAttributeUpdater<T>::Dump(
        const file_system::DirectoryPtr& attributeDir, segmentid_t srcSegment)
{
    std::vector<docid_t> docIdVect;
    docIdVect.reserve(mHashMap.size());
    typename HashMap::iterator it = mHashMap.begin();
    typename HashMap::iterator itEnd = mHashMap.end();
    for (; it != itEnd; ++it)
    {
        docIdVect.push_back(it->first);
    }
    std::sort(docIdVect.begin(), docIdVect.end());

    config::PackAttributeConfig* packAttrConfig = mAttrConfig->GetPackAttributeConfig();
    std::string attrDir =
        packAttrConfig != NULL ?
        packAttrConfig->GetAttrName() + "/" + mAttrConfig->GetAttrName() :
        mAttrConfig->GetAttrName();

    file_system::DirectoryPtr dir = attributeDir->MakeDirectory(attrDir);

    std::string patchFileName = GetPatchFileName(srcSegment);
    file_system::FileWriterPtr patchFileWriter = 
        CreatePatchFileWriter(dir, patchFileName);

    size_t reserveSize = docIdVect.size() * (sizeof(T) + sizeof(docid_t));
    patchFileWriter->ReserveFileNode(reserveSize);

    std::string filePath = patchFileWriter->GetPath();
    IE_LOG(INFO, "Begin dumping attribute patch to file [%ld]: %s",
           reserveSize, filePath.c_str());
    for (size_t i = 0; i < docIdVect.size(); ++i) 
    {
        docid_t docId = docIdVect[i];
        T value = mHashMap[docId];
        patchFileWriter->Write(&docId, sizeof(docid_t));
        patchFileWriter->Write(&value, sizeof(T));
    }

    assert(mAttrConfig->GetCompressType().HasPatchCompress() ||
           patchFileWriter->GetLength() == reserveSize);
    size_t actualSize = patchFileWriter->GetLength();
    patchFileWriter->Close();
    IE_LOG(INFO, "Finish dumping attribute patch to file [%ld]: %s",
           actualSize, filePath.c_str());
}

typedef SingleValueAttributeUpdater<int8_t> Int8AttributeUpdater;
typedef SingleValueAttributeUpdater<uint8_t> UInt8AttributeUpdater;

typedef SingleValueAttributeUpdater<int16_t> Int16AttributeUpdater;
typedef SingleValueAttributeUpdater<uint16_t> UInt16AttributeUpdater;

typedef SingleValueAttributeUpdater<int32_t> Int32AttributeUpdater;
typedef SingleValueAttributeUpdater<uint32_t> UInt32AttributeUpdater;

typedef SingleValueAttributeUpdater<int64_t> Int64AttributeUpdater;
typedef SingleValueAttributeUpdater<uint64_t> UInt64AttributeUpdater;

typedef SingleValueAttributeUpdater<float> FloatAttributeUpdater;
typedef SingleValueAttributeUpdater<double> DoubleAttributeUpdater;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SINGLE_VALUE_ATTRIBUTE_UPDATER_H
