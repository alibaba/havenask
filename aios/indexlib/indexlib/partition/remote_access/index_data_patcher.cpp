#include "indexlib/partition/remote_access/index_data_patcher.h"
#include "indexlib/partition/remote_access/index_field_convertor.h"
#include "indexlib/document/document_rewriter/section_attribute_appender.h"
#include "indexlib/document/index_document/normal_document/index_document.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index/normal/inverted_index/accessor/index_writer_factory.h"
#include "indexlib/index/normal/inverted_index/accessor/index_writer.h"
#include "indexlib/index_define.h"
#include "indexlib/index_base/index_meta/segment_metrics.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/util/path_util.h"
#include "indexlib/util/simple_pool.h"
#include "indexlib/misc/exception.h"
#include "indexlib/util/memory_control/build_resource_calculator.h"
#include "indexlib/util/memory_control/build_resource_metrics.h"
#include "indexlib/config/index_partition_options.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, IndexDataPatcher);

bool IndexDataPatcher::Init(const IndexConfigPtr& indexConfig,
                            const string& segDirPath, uint32_t docCount)
{
    mIndexConfig = indexConfig;
    mDocCount = docCount;
    mPatchDocCount = 0;
    mPool.reset(new autil::mem_pool::Pool);
    mIndexDoc.reset(new IndexDocument(mPool.get()));
    IndexConfig::Iterator iter = indexConfig->CreateIterator();
    while (iter.HasNext())
    {
        FieldConfigPtr fieldConfig = iter.Next();
        fieldid_t fieldId = fieldConfig->GetFieldId();
        assert(fieldId >= 0);
        mFieldConfigMap[fieldId] = fieldConfig;
    }
    mConvertor.reset(new IndexFieldConvertor(mSchema));

    if (!FileSystemWrapper::IsExist(segDirPath))
    {
        IE_LOG(ERROR, "[%s] not exist!", segDirPath.c_str());
        return false;
    }

    if (mDocCount == 0)
    {
        IE_LOG(INFO, "Init IndexDataPatcher [%s] for empty segment [%s]",
               indexConfig->GetIndexName().c_str(), segDirPath.c_str());
        return true;
    }
    mIndexDirPath = PathUtil::JoinPath(segDirPath, INDEX_DIR_NAME);
    FileSystemWrapper::MkDirIfNotExist(mIndexDirPath);
    string targetDirPath = PathUtil::JoinPath(mIndexDirPath,
            indexConfig->GetIndexName());
    if (FileSystemWrapper::IsExist(targetDirPath))
    {
        IE_LOG(ERROR, "target path [%s] already exist, will be cleaned!",
               targetDirPath.c_str());
        FileSystemWrapper::DeleteIfExist(targetDirPath);
    }

    mBuildResourceMetrics.reset(new BuildResourceMetrics);    
    return DoInit();
}

void IndexDataPatcher::AddField(const string& fieldValue, fieldid_t fieldId)
{
    FieldId2FieldConfigMap::const_iterator iter = mFieldConfigMap.find(fieldId);
    if (iter == mFieldConfigMap.end())
    {
        IE_LOG(ERROR, "field [%d] not in index [%s]", fieldId,
               mIndexConfig->GetIndexName().c_str());
        return;
    }
    mConvertor->convertIndexField(mIndexDoc, iter->second, fieldValue,
                                  INDEXLIB_MULTI_VALUE_SEPARATOR_STR, mPool.get());
}

void IndexDataPatcher::EndDocument()
{
    if (!mIndexWriter)
    {
        return;
    }
    
    assert(mIndexDoc);
    FieldId2FieldConfigMap::const_iterator iter = mFieldConfigMap.begin();
    while (iter != mFieldConfigMap.end())
    {
        Field* field = mIndexDoc->GetField(iter->first);
        if (field)
        {
            mIndexWriter->AddField(field);
        }
        iter++;
    }
                                       
    mIndexDoc->SetDocId(mPatchDocCount);
    if (mSectionAttrAppender)
    {
        if (!mSectionAttrAppender->AppendSectionAttribute(mIndexDoc))
        {
            INDEXLIB_FATAL_ERROR(InconsistentState,
                    "append section attribute fail for docid [%d]!",
                    mIndexDoc->GetDocId());
        }
    }
    if (mIndexDoc->GetDocId() >= (docid_t)mDocCount)
    {
        INDEXLIB_FATAL_ERROR(InconsistentState, "docId [%d] over range!",
                             mIndexDoc->GetDocId());
    }
    mIndexWriter->EndDocument(*mIndexDoc);
    ++mPatchDocCount;
    mIndexDoc->ClearData();
    if (mPool->getUsedBytes() >= MAX_POOL_MEMORY_THRESHOLD)
    {
        mPool->release();
    }
}

void IndexDataPatcher::Close()
{
    if (!mIndexWriter)
    {
        return;
    }
    if (mPatchDocCount == 0)
    {
        for (size_t i = 0; i < mDocCount; i++)
        {
            EndDocument();
        }
    }
    mIndexWriter->EndSegment();
    SimplePool pool;
    DirectoryPtr dumpDir = DirectoryCreator::Get(
            mRootDir->GetFileSystem(), mIndexDirPath, true);
    mIndexWriter->Dump(dumpDir, &pool);
    mIndexWriter.reset();
}

int64_t IndexDataPatcher::GetCurrentTotalMemoryUse() const
{
    return mPool->getUsedBytes() +
        BuildResourceCalculator::GetCurrentTotalMemoryUse(mBuildResourceMetrics);
}

uint32_t IndexDataPatcher::GetDistinctTermCount() const
{
    if (!mIndexWriter)
    {
        return 0;
    }
    SegmentMetricsPtr segMetrics(new SegmentMetrics);
    mIndexWriter->FillDistinctTermCount(segMetrics);
    if (mIndexConfig->GetShardingType() != IndexConfig::IST_NEED_SHARDING)
    {
        return segMetrics->GetDistinctTermCount(mIndexConfig->GetIndexName());
    }
    
    uint32_t termCount = 0;
    auto indexConfigs = mIndexConfig->GetShardingIndexConfigs();
    for (size_t i = 0; i < indexConfigs.size(); i++)
    {
        termCount += segMetrics->GetDistinctTermCount(
                indexConfigs[i]->GetIndexName());
    }
    return termCount;
}

bool IndexDataPatcher::DoInit()
{
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    SegmentMetricsPtr segMetrics(new SegmentMetrics);
    segMetrics->SetDistinctTermCount(mIndexConfig->GetIndexName(), mInitDistinctTermCount);
    if (mIndexConfig->GetShardingType() == IndexConfig::IST_NEED_SHARDING)
    {
        auto indexConfigs = mIndexConfig->GetShardingIndexConfigs();
        uint32_t termCount = mInitDistinctTermCount / indexConfigs.size();
        for (size_t i = 0; i < indexConfigs.size(); i++)
        {
            segMetrics->SetDistinctTermCount(
                    indexConfigs[i]->GetIndexName(), termCount);
        }
    }
    
    mIndexWriter.reset(IndexWriterFactory::CreateIndexWriter(
                    mIndexConfig, segMetrics, options,
                    mBuildResourceMetrics.get(), mPluginManager,
                    PartitionSegmentIteratorPtr()));
    if (!mIndexWriter)
    {
        IE_LOG(ERROR, "create index writer for [%s] fail!",
               mIndexConfig->GetIndexName().c_str());
        return false;
    }

    IndexType type = mIndexConfig->GetIndexType();
    if (type != it_pack && type != it_expack)
    {
        return true;
    }
    
    PackageIndexConfigPtr packIndex =
        DYNAMIC_POINTER_CAST(PackageIndexConfig, mIndexConfig);
    if (packIndex && packIndex->HasSectionAttribute())
    {
        mSectionAttrAppender.reset(new SectionAttributeAppender);
        if (!mSectionAttrAppender->Init(mSchema))
        {
            IE_LOG(ERROR, "create section attribute appender for index [%s] fail!",
                   mIndexConfig->GetIndexName().c_str());
            return false;
        }
    }
    return true;
}

IE_NAMESPACE_END(partition);

