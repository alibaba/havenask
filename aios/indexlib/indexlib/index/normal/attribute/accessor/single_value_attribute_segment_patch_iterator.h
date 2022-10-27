#ifndef __INDEXLIB_SINGLE_VALUE_ATTRIBUTE_SEGMENT_PATCH_ITERATOR_H
#define __INDEXLIB_SINGLE_VALUE_ATTRIBUTE_SEGMENT_PATCH_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_patch_reader.h"
#include "indexlib/index_base/patch/patch_file_finder.h"

IE_NAMESPACE_BEGIN(index);

template <typename T>
class SingleValueAttributeSegmentPatchIterator : public SingleValueAttributePatchReader<T>
{
private:
    using SingleValueAttributePatchReader<T>::Next;
public:
    SingleValueAttributeSegmentPatchIterator(const config::AttributeConfigPtr& config)
        : SingleValueAttributePatchReader<T>(config)
    {}
    ~SingleValueAttributeSegmentPatchIterator() {}

public:
    void Init(const std::vector<std::pair<std::string, segmentid_t> >& patchFileList);
    bool Next(docid_t& docId, T& value);

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, SingleValueAttributeSegmentPatchIterator);

template <typename T>
void SingleValueAttributeSegmentPatchIterator<T>::Init(
        const std::vector<std::pair<std::string, segmentid_t> >& patchFileList)
{
    for (size_t i = 0; i < patchFileList.size(); ++i)
    {
        this->AddPatchFile(patchFileList[i].first, patchFileList[i].second);
    }
}

template <typename T>
bool SingleValueAttributeSegmentPatchIterator<T>::Next(docid_t& docId, T& value)
{
    if (!this->HasNext())
    {
        return false;
    }

    SinglePatchFile* patchFile = this->mPatchHeap.top();
    docId = patchFile->GetCurDocId();
    return this->InnerReadPatch(docId, value);
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SINGLE_VALUE_ATTRIBUTE_SEGMENT_PATCH_ITERATOR_H
