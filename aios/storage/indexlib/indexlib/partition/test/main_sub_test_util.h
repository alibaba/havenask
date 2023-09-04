#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/document/index_document/normal_document/modified_tokens.h"
#include "indexlib/partition/modifier/sub_doc_modifier.h"
#include "indexlib/partition/segment/sub_doc_segment_writer.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(file_system, IFileSystem);

namespace indexlib { namespace partition {

class MainSubTestUtil
{
public:
    class MockSubDocModifier : public SubDocModifier
    {
    public:
        MockSubDocModifier(const config::IndexPartitionSchemaPtr& schema, const PartitionModifierPtr& mainModifier,
                           const PartitionModifierPtr& subModifier)
            : SubDocModifier(schema)
        {
            mMainModifier = mainModifier;
            mSubModifier = subModifier;
        }
    };

    class MockSubDocSegmentWriter : public SubDocSegmentWriter
    {
    public:
        MockSubDocSegmentWriter(const config::IndexPartitionSchemaPtr& schema,
                                const config::IndexPartitionOptions& options, const SingleSegmentWriterPtr& mainWriter,
                                const SingleSegmentWriterPtr& subWriter)
            : SubDocSegmentWriter(schema, options)
        {
            mMainWriter = mainWriter;
            mSubWriter = subWriter;
        }
    };

public:
    static partition::SubDocSegmentWriterPtr PrepareSubSegmentWriter(const config::IndexPartitionSchemaPtr& schema,
                                                                     const file_system::IFileSystemPtr& fileSystem);
    static config::IndexPartitionSchemaPtr CreateSchema();

private:
    MainSubTestUtil();
    ~MainSubTestUtil();

    MainSubTestUtil(const MainSubTestUtil&) = delete;
    MainSubTestUtil& operator=(const MainSubTestUtil&) = delete;
    MainSubTestUtil(MainSubTestUtil&&) = delete;
    MainSubTestUtil& operator=(MainSubTestUtil&&) = delete;

private:
    IE_LOG_DECLARE();
};

}} // namespace indexlib::partition
