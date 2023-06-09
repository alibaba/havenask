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
#ifndef __INDEXLIB_ATTRIBUTE_WRITER_H
#define __INDEXLIB_ATTRIBUTE_WRITER_H

#include <memory>
#include <queue>

#include "autil/mem_pool/Pool.h"
#include "indexlib/common/field_format/attribute/attribute_convertor.h"
#include "indexlib/common_define.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/document/index_document/normal_document/attribute_document.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/common/DefaultFSWriterParamDecider.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/MMapAllocator.h"
#include "indexlib/util/SimplePool.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"
#include "indexlib/util/slice_array/ByteAlignedSliceArray.h"

namespace indexlib { namespace index {

#define DECLARE_ATTRIBUTE_WRITER_IDENTIFIER(id)                                                                        \
    static std::string Identifier() { return std::string("attribute.writer." #id); }                                   \
    std::string GetIdentifier() const override { return Identifier(); }

class AttributeWriter
{
public:
    AttributeWriter(const config::AttributeConfigPtr& attrConfig)
        : mAttrConfig(attrConfig)
        , mBuildResourceMetricsNode(NULL)
    {
    }

    virtual ~AttributeWriter() {}

protected:
    typedef std::shared_ptr<autil::mem_pool::ChunkAllocatorBase> AllocatorPtr;
    typedef std::shared_ptr<autil::mem_pool::Pool> PoolPtr;

public:
    virtual void Init(const FSWriterParamDeciderPtr& fsWriterParamDecider,
                      util::BuildResourceMetrics* buildResourceMetrics)
    {
        if (!fsWriterParamDecider) {
            mFsWriterParamDecider.reset(new DefaultFSWriterParamDecider);
        } else {
            mFsWriterParamDecider = fsWriterParamDecider;
        }
        mAllocator.reset(new util::MMapAllocator);
        mPool.reset(new autil::mem_pool::Pool(mAllocator.get(), DEFAULT_CHUNK_SIZE * 1024 * 1024));
        if (buildResourceMetrics) {
            assert(mAttrConfig);
            mBuildResourceMetricsNode = buildResourceMetrics->AllocateNode();
            IE_LOG(INFO, "allocate build resource node [id:%d] for attribute[%s]",
                   mBuildResourceMetricsNode->GetNodeId(), mAttrConfig->GetAttrName().c_str());
        }
    }

    virtual void AddField(docid_t docId, const autil::StringView& attributeValue, bool isNull = false) = 0;

    virtual bool UpdateField(docid_t docId, const autil::StringView& attributeValue, bool isNull = false) = 0;

    virtual void Dump(const file_system::DirectoryPtr& dir, autil::mem_pool::PoolBase* dumpPool) = 0;

    virtual bool IsUpdatable() const { return mAttrConfig->IsAttributeUpdatable(); }

    virtual std::string GetIdentifier() const = 0;

    config::AttributeConfigPtr GetAttrConfig() const;

    virtual const AttributeSegmentReaderPtr CreateInMemReader() const = 0;

    // TODO: delete
    virtual void GetDumpEstimateFactor(std::priority_queue<double>& factors,
                                       std::priority_queue<size_t>& minMemUses) const
    {
    }

    void SetAttrConvertor(const common::AttributeConvertorPtr& attrConvertor);
    const common::AttributeConvertorPtr& GetAttrConvertor() const { return mAttrConvertor; }

    virtual void SetTemperatureLayer(const std::string& layer) { mTemperatureLayer = layer; }
    const std::string& GetTemperatureLayer() const { return mTemperatureLayer; }

protected:
    virtual void UpdateBuildResourceMetrics() = 0;

protected:
    config::AttributeConfigPtr mAttrConfig;
    AllocatorPtr mAllocator;
    PoolPtr mPool;
    util::SimplePool mSimplePool;
    common::AttributeConvertorPtr mAttrConvertor;
    FSWriterParamDeciderPtr mFsWriterParamDecider;
    util::BuildResourceMetricsNode* mBuildResourceMetricsNode;
    std::string mTemperatureLayer;

private:
    friend class AttributeWriterTest;
    IE_LOG_DECLARE();
};

typedef std::shared_ptr<AttributeWriter> AttributeWriterPtr;

//////////////////////////////////////////////////////////////

inline config::AttributeConfigPtr AttributeWriter::GetAttrConfig() const { return mAttrConfig; }
}} // namespace indexlib::index

#endif //__INDEXENGINEATTRIBUTE_WRITER_H
