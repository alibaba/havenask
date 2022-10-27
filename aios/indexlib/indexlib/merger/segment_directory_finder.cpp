#include <autil/StringUtil.h>
#include "indexlib/merger/segment_directory_finder.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/util/path_util.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, SegmentDirectoryFinder);

SegmentDirectoryFinder::SegmentDirectoryFinder() 
{
}

SegmentDirectoryFinder::~SegmentDirectoryFinder() 
{
}

std::string SegmentDirectoryFinder::GetAttributeDirPath(
        const SegmentDirectoryPtr& segDir,
        const AttributeConfig* attrConfig, segmentid_t segId)
{
    string segPath = segDir->GetSegmentPath(segId);
    if (segPath.empty())
    {
        //TODO: throw exception
        return segPath;
    }

    const string& attrName = attrConfig->GetAttrName();
    AttributeConfig::ConfigType configType = attrConfig->GetConfigType();
    if (configType == AttributeConfig::ct_section)
    {
        return segPath + INDEX_DIR_NAME + '/' + attrName + '/';
    }

    //TODO: add pk type
    assert(configType == AttributeConfig::ct_normal);
    return segPath + ATTRIBUTE_DIR_NAME + '/' + attrName + '/';
}

segmentid_t SegmentDirectoryFinder::GetSegmentIdFromDelMapDataFile(
        const SegmentDirectoryPtr& segDir,
        segmentid_t segmentId,
        const std::string& dataFileName)
{
    size_t pos = dataFileName.find('_');
    if (pos == string::npos)
    {
        IE_LOG(WARN, "Error file name in deletion map: [%s].", dataFileName.c_str());
        return INVALID_SEGMENTID;
    }
    segmentid_t physicalSegId;
    bool ret = StringUtil::strToInt32(dataFileName.substr(pos + 1).c_str(), physicalSegId);
    if (ret)
    {
        return segDir->GetVirtualSegmentId(segmentId, physicalSegId);
    }
    return INVALID_SEGMENTID;
}

void SegmentDirectoryFinder::ListAttrPatch(const SegmentDirectory* segDir,
        const string& attrName, segmentid_t segId,
        vector<pair<string, segmentid_t> >& patches)
{
    InnerListPatchFiles(segDir, ATTRIBUTE_DIR_NAME, ATTRIBUTE_PATCH_FILE_NAME,
                        attrName, segId, patches);
}

void SegmentDirectoryFinder::ListAttrPatch(const SegmentDirectory* segDir,
        const string& attrName, segmentid_t segId, 
        vector<string>& patches)
{
    vector<pair<string, segmentid_t> > patchPairs;
    ListAttrPatch(segDir, attrName, segId, patchPairs);
    for (size_t i = 0; i < patchPairs.size(); ++i)
    {
        patches.push_back(patchPairs[i].first);
    }
}

void SegmentDirectoryFinder::InnerListPatchFiles(
        const SegmentDirectory* segDir,
        const string& dirName, const string& patchSuffixName,
        const string& patchFieldName, segmentid_t segId, 
        vector<pair<string, segmentid_t> >& patches) 
{
    SegmentDirectory::partitionid_t partId;
    segmentid_t physicalSegId;
    segDir->GetPhysicalSegmentId(segId, physicalSegId, partId);

    stringstream ss;
    ss << physicalSegId << '.' << patchSuffixName;
    string patchFile = ss.str();

    const SegmentDirectory::Segments& segments = segDir->GetSegments();
    for (SegmentDirectory::Segments::const_iterator it = segments.begin();
         it != segments.end(); ++it)
    {
        if (it->second.partId != partId)
        {
            continue;
        }
        const string& segPath = it->second.segPath;
        string patchPath = segPath + dirName + '/' + patchFieldName + '/' + patchFile;
        if (segDir->IsExist(patchPath))
        {
            patches.push_back(std::make_pair(patchPath, it->second.physicalSegId));
        }
    }
}

std::string SegmentDirectoryFinder::MakeSegmentPath(const std::string& rootDir, 
        segmentid_t segId, const Version& version, bool isSub)
{
    string segDirName = version.GetSegmentDirName(segId);
    string path = PathUtil::JoinPath(rootDir, segDirName);
    if (isSub)
    {
        path = PathUtil::JoinPath(path, SUB_SEGMENT_DIR_NAME);
    }
    return FileSystemWrapper::NormalizeDir(path);
}

IE_NAMESPACE_END(merger);

