#ifndef __INDEXLIB_ATTRIBUTE_MERGER_TEST_UTIL_H
#define __INDEXLIB_ATTRIBUTE_MERGER_TEST_UTIL_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/index/normal/reclaim_map/reclaim_map.h"
#include "indexlib/common/field_format/attribute/type_info.h"
#include "indexlib/file_system/buffered_file_writer.h"

IE_NAMESPACE_BEGIN(index);

class MockSegmentDirectory: public merger::SegmentDirectory
{
public:
    MockSegmentDirectory(const std::string& root, const index_base::Version& version);

public:
    MOCK_CONST_METHOD3(ListAttrPatch,
                       void(const std::string& attrName, segmentid_t segId,
                               std::vector<std::pair<std::string, segmentid_t> >& patchs));
};

class MockReclaimMap : public ReclaimMap
{
private:
    using ReclaimMap::Init;
public: 
    void Init(const std::vector<uint32_t>& docCounts, 
              const std::vector<std::set<docid_t> >& delDocIdSets);
};

class MockBufferedFileWriter : public file_system::BufferedFileWriter
{
public:
    MockBufferedFileWriter(std::vector<std::string> &dataVec);
    ~MockBufferedFileWriter() {}

    size_t Write(const void *src, size_t lenToWrite) override;
    void Close() override {}
    void Open(const std::string &fileName) override {}
private:
    std::vector<std::string> &mDataVec;
};

//////////////////////////////////////////////////////////////////////
class AttributeMergerTestUtil
{
public:
    template <typename T>
    static config::AttributeConfigPtr CreateAttributeConfig(
            bool multiValue);
    static config::AttributeConfigPtr CreateAttributeConfig(
            FieldType type, bool multiValue);
    /*
     * Format:
     * segMergeInfosString: "docCount1(delId11,delId12,...);"
     *                      "docCount2(delId21,delId22,...)",
     * e.g.                 "2(0);4(1,3);4()"
     */
    static ReclaimMapPtr CreateReclaimMap(
            const std::string& segMergeInfosString);
    /*
     * Format:
     * segMergeInfosString: "docCount1(delId11,delId12,...);"
     *                      "docCount2(delId21,delId22,...)",
     * e.g.                 "2(0);4(1,3);4()"
     */
    static index_base::SegmentMergeInfos CreateSegmentMergeInfos(
            const std::string& segMergeInfosString);
    
private:
    static void CreateDocCountsAndDelDocIdSets(
            const std::string& segMergeInfosString,
            std::vector<uint32_t>& docCounts,
            std::vector<std::set<docid_t> >& delDocIdSets);
};

//////////////////////////////////////////////////////////////////////
template <typename T>
config::AttributeConfigPtr AttributeMergerTestUtil::CreateAttributeConfig(
        bool isMultiValue)
{
    FieldType type = common::TypeInfo<T>::GetFieldType();
    return CreateAttributeConfig(type, isMultiValue);
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTE_MERGER_TEST_UTIL_H
