#ifndef __INDEXLIB_PATCH_FILE_FINDER_H
#define __INDEXLIB_PATCH_FILE_FINDER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(config, AttributeConfig);

IE_NAMESPACE_BEGIN(index_base);

struct PatchFileInfo
{
    segmentid_t srcSegment;
    file_system::DirectoryPtr patchDirectory;
    std::string patchFileName;

    bool operator == (const PatchFileInfo& other) const;
};

typedef std::vector<PatchFileInfo> AttrPatchFileInfoVec;
typedef std::map<segmentid_t, AttrPatchFileInfoVec> AttrPatchInfos;
typedef std::map<segmentid_t, PatchFileInfo> DeletePatchInfos;

class PatchFileFinder
{
public:
    PatchFileFinder();
    virtual ~PatchFileFinder();

public:
    virtual void FindDeletionMapFiles(DeletePatchInfos& patchInfos) = 0;

    virtual void FindAttrPatchFiles(const std::string& attrName,
                                    schema_opid_t opId,
                                    AttrPatchInfos& patchInfos) = 0;

    virtual void FindAttrPatchFilesForSegment(
        const std::string& attrName, segmentid_t segmentId,
        schema_opid_t opId, AttrPatchFileInfoVec& patchInfo) = 0;

    virtual void FindAttrPatchFilesInSegment(
        const std::string& attrName, segmentid_t segmentId,
        schema_opid_t opId, AttrPatchInfos& patchInfos) = 0;

public:
    void FindAttrPatchFiles(const config::AttributeConfigPtr& attrConfig,
                            AttrPatchInfos& patchInfos);

    void FindAttrPatchFilesForSegment(
        const config::AttributeConfigPtr& attrConfig,
        segmentid_t segmentId, AttrPatchFileInfoVec& patchInfo);

    void FindAttrPatchFilesInSegment(
        const config::AttributeConfigPtr& attrConfig,
        segmentid_t segmentId, AttrPatchInfos& patchInfos);
        
    static bool IsAttrPatchFile(const std::string& dataFileName);

protected:
    static segmentid_t ExtractSegmentIdFromDeletionMapFile(
            const std::string& dataFileName);

    static bool ExtractSegmentIdFromAttrPatchFile(
            const std::string& dataFileName, segmentid_t& srcSegment,
            segmentid_t& destSegment);

    static segmentid_t GetSegmentIdFromStr(const std::string& segmentIdStr);

    void FindInPackAttrPatchFiles(
        const std::string& packAttrName, const std::string& subAttrName,
        AttrPatchInfos& patchInfos)
    {
        FindAttrPatchFiles(packAttrName + "/" + subAttrName,
                           INVALID_SCHEMA_OP_ID, patchInfos);
    }
    
    void FindInPackAttrPatchFilesForSegment(
        const std::string& packAttrName, const std::string& subAttrName,
        segmentid_t segmentId, AttrPatchFileInfoVec& patchInfo)
    {
        FindAttrPatchFilesForSegment(packAttrName + "/" + subAttrName,
                segmentId, INVALID_SCHEMA_OP_ID, patchInfo);
    }

    void FindInPackAttrPatchFilesInSegment(
        const std::string& packAttrName, const std::string& attrName,
        segmentid_t segmentId, AttrPatchInfos& patchInfos)
    {
        FindAttrPatchFilesInSegment(
                packAttrName + "/" + attrName, segmentId,
                INVALID_SCHEMA_OP_ID, patchInfos);
    }

private:
    friend class SinglePartPatchFinderTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PatchFileFinder);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_PATCH_FILE_FINDER_H
