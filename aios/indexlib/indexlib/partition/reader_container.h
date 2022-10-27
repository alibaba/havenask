#ifndef __INDEXLIB_READER_CONTAINER_H
#define __INDEXLIB_READER_CONTAINER_H

#include <tr1/memory>
#include <autil/Lock.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/partition/index_partition_reader.h"

IE_NAMESPACE_BEGIN(partition);

// ReaderContainer is not thread-safe, the user should guarrantee it!
class ReaderContainer
{
private:
    typedef std::pair<index_base::Version, IndexPartitionReaderPtr> ReaderPair;
    typedef std::vector<ReaderPair> ReaderVector;

public:
    ReaderContainer();
    virtual ~ReaderContainer();
public:
    void AddReader(const IndexPartitionReaderPtr& reader);
    // TODO: remove
    virtual bool HasReader(const index_base::Version& version) const; 
    virtual bool HasReader(versionid_t versionId) const;
    // virtual for test
    virtual void GetIncVersions(std::vector<index_base::Version>& versions) const;
    bool IsUsingSegment(segmentid_t segmentId) const; 
    IndexPartitionReaderPtr GetOldestReader() const;
    IndexPartitionReaderPtr GetLatestReader(versionid_t versionId) const;
    IndexPartitionReaderPtr PopOldestReader();
    versionid_t GetOldestReaderVersion() const;
    // virtual for test
    virtual versionid_t GetLatestReaderVersion() const;
    void EvictOldestReader();
    size_t Size() const;
    void Close();
    void GetSwitchRtSegments(std::vector<segmentid_t>& segIds) const;
    bool HasAttributeReader(const std::string& attrName, bool isSub) const;

    bool EvictOldReaders();

private:
    void ResetSwitchRtSegRange();
    
private:
    ReaderVector mReaderVec;
    std::vector<segmentid_t> mSwitchRtSegments;
    mutable autil::ThreadMutex mReaderVecLock;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ReaderContainer);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_READER_CONTAINER_H
