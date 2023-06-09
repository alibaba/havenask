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
#include "indexlib/index/normal/attribute/accessor/built_attribute_segment_modifier.h"

#include "indexlib/index/normal/attribute/accessor/attribute_updater_factory.h"

using namespace std;
using namespace autil::mem_pool;

using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, BuiltAttributeSegmentModifier);

class AttributeUpdaterDumpItem : public common::DumpItem
{
public:
    AttributeUpdaterDumpItem(const file_system::DirectoryPtr& directory, const AttributeUpdaterPtr& updater,
                             segmentid_t srcSegmentId)
        : mDirectory(directory)
        , mUpdater(updater)
        , mSrcSegId(srcSegmentId)
    {
    }

    ~AttributeUpdaterDumpItem() {}

public:
    void process(autil::mem_pool::PoolBase* pool) override { mUpdater->Dump(mDirectory, mSrcSegId); }

    void destroy() override { delete this; }

    void drop() override { destroy(); }

private:
    file_system::DirectoryPtr mDirectory;
    AttributeUpdaterPtr mUpdater;
    segmentid_t mSrcSegId;
};

BuiltAttributeSegmentModifier::BuiltAttributeSegmentModifier(segmentid_t segId, const AttributeSchemaPtr& attrSchema,
                                                             util::BuildResourceMetrics* buildResourceMetrics)
    : mAttrSchema(attrSchema)
    , mSegmentId(segId)
    , mBuildResourceMetrics(buildResourceMetrics)
{
}

BuiltAttributeSegmentModifier::~BuiltAttributeSegmentModifier() {}

void BuiltAttributeSegmentModifier::Update(docid_t docId, attrid_t attrId, const autil::StringView& attrValue,
                                           bool isNull)
{
    AttributeUpdaterPtr& updater = GetUpdater((uint32_t)attrId);
    assert(updater);
    updater->Update(docId, attrValue, isNull);
}

AttributeUpdaterPtr BuiltAttributeSegmentModifier::CreateUpdater(uint32_t idx)
{
    attrid_t attrId = (attrid_t)idx;
    IE_LOG(DEBUG, "Create Attribute Updater %d in segment %d", attrId, mSegmentId);

    AttributeUpdaterFactory* factory = AttributeUpdaterFactory::GetInstance();
    AttributeConfigPtr attrConfig = mAttrSchema->GetAttributeConfig(attrId);
    AttributeUpdater* updater = factory->CreateAttributeUpdater(mBuildResourceMetrics, mSegmentId, attrConfig);
    assert(updater != NULL);
    return AttributeUpdaterPtr(updater);
}

void BuiltAttributeSegmentModifier::Dump(const DirectoryPtr& dir, segmentid_t srcSegment)
{
    for (size_t i = 0; i < mUpdaters.size(); ++i) {
        if (mUpdaters[i]) {
            mUpdaters[i]->Dump(dir, srcSegment);
        }
    }
}

AttributeUpdaterPtr& BuiltAttributeSegmentModifier::GetUpdater(uint32_t idx)
{
    if (idx >= (uint32_t)mUpdaters.size()) {
        mUpdaters.resize(idx + 1);
    }

    if (!mUpdaters[idx]) {
        mUpdaters[idx] = CreateUpdater(idx);
    }
    return mUpdaters[idx];
}

void BuiltAttributeSegmentModifier::CreateDumpItems(const DirectoryPtr& directory, segmentid_t srcSegmentId,
                                                    vector<std::unique_ptr<common::DumpItem>>* dumpItems)
{
    for (size_t i = 0; i < mUpdaters.size(); ++i) {
        if (mUpdaters[i]) {
            dumpItems->push_back(std::make_unique<AttributeUpdaterDumpItem>(directory, mUpdaters[i], srcSegmentId));
        }
    }
}
}} // namespace indexlib::index
