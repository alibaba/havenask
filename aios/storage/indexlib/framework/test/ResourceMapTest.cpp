#include "indexlib/framework/ResourceMap.h"

#include "indexlib/framework/IResource.h"
#include "unittest/unittest.h"

namespace indexlibv2 { namespace framework {

class ResourceMapTest : public TESTBASE
{
};

class DummyResource : public IResource
{
public:
    DummyResource() = default;
    DummyResource(const std::string value) : _value(value) {}
    void SetValue(const std::string& value) { _value = value; }
    const std::string& GetValue() { return _value; }
    std::shared_ptr<IResource> Clone() override { return std::make_shared<DummyResource>(_value); }

    size_t CurrentMemmoryUse() const override { return _value.size() * sizeof(char); }

private:
    std::string _value;
};

TEST_F(ResourceMapTest, testSimple)
{
    ResourceMap resourceMap;
    std::string resourceName = "key";
    std::string resourceValue = "value";
    std::string resourceName2 = "key2";
    std::string resourceValue2 = "value2";
    std::string resourceName3 = "key3";
    std::string resourceValue3 = "value3";
    auto creator = []() {
        std::shared_ptr<DummyResource> resource = std::make_shared<DummyResource>();
        return resource;
    };
    {
        auto resource = std::dynamic_pointer_cast<DummyResource>(resourceMap.GetResource(resourceName));
        ASSERT_FALSE(resource);
        ASSERT_TRUE(resourceMap.AddVersionResource(resourceName, creator()).IsOK());
        ASSERT_FALSE(resourceMap.AddVersionResource(resourceName, creator()).IsOK());
        resource = std::dynamic_pointer_cast<DummyResource>(resourceMap.GetResource(resourceName));
        ASSERT_NE(nullptr, resource);
        resource->SetValue(resourceValue);

        ASSERT_TRUE(resourceMap.AddVersionResource(resourceName2, creator()).IsOK());
        auto resource2 = std::dynamic_pointer_cast<DummyResource>(resourceMap.GetResource(resourceName2));

        ASSERT_NE(nullptr, resource2);
        resource2->SetValue(resourceValue2);

        ASSERT_FALSE(resourceMap.AddSegmentResource(resourceName3, -1, creator()).IsOK());
        ASSERT_TRUE(resourceMap.AddSegmentResource(resourceName3, segmentid_t(1), creator()).IsOK());
        auto resource3 = std::dynamic_pointer_cast<DummyResource>(resourceMap.GetResource(resourceName3));
        ASSERT_NE(nullptr, resource3);
        resource3->SetValue(resourceValue3);
    }
    {
        auto resource = std::dynamic_pointer_cast<DummyResource>(resourceMap.GetResource(resourceName));
        ASSERT_TRUE(resource);
        ASSERT_EQ(resourceValue, resource->GetValue());

        auto resource2 = std::dynamic_pointer_cast<DummyResource>(resourceMap.GetResource(resourceName2));
        ASSERT_TRUE(resource2);
        ASSERT_EQ(resourceValue2, resource2->GetValue());

        auto resource3 = std::dynamic_pointer_cast<DummyResource>(resourceMap.GetResource(resourceName3));
        ASSERT_TRUE(resource3);
        ASSERT_EQ(resourceValue3, resource3->GetValue());
    }
    {
        auto memUse = (resourceValue.size() + resourceValue2.size() + resourceValue3.size()) * sizeof(char);
        ASSERT_EQ(memUse, resourceMap.CurrentMemmoryUse());
    }
    {
        auto clonedResourceMap = resourceMap.Clone();
        ASSERT_TRUE(clonedResourceMap != nullptr);
        auto resource = std::dynamic_pointer_cast<DummyResource>(clonedResourceMap->GetResource(resourceName));
        ASSERT_FALSE(resource);

        auto resource2 = std::dynamic_pointer_cast<DummyResource>(clonedResourceMap->GetResource(resourceName2));
        ASSERT_FALSE(resource2);

        auto oldResource2 = std::dynamic_pointer_cast<DummyResource>(resourceMap.GetResource(resourceName2));
        ASSERT_TRUE(oldResource2);
        ASSERT_EQ(resourceValue2, oldResource2->GetValue());

        auto resource3 = std::dynamic_pointer_cast<DummyResource>(clonedResourceMap->GetResource(resourceName3));
        ASSERT_TRUE(resource3);
        ASSERT_EQ(resourceValue3, resource3->GetValue());

        auto oldResource3 = std::dynamic_pointer_cast<DummyResource>(resourceMap.GetResource(resourceName3));
        ASSERT_TRUE(oldResource3);
        ASSERT_EQ(resourceValue3, oldResource3->GetValue());

        resource3->SetValue("12345");
        ASSERT_EQ(resourceValue3, oldResource3->GetValue());

        std::set<segmentid_t> reservecSegment({0});
        clonedResourceMap->ReclaimSegmentResource(reservecSegment);

        ASSERT_EQ(0, clonedResourceMap->CurrentMemmoryUse());
    }
}
}} // namespace indexlibv2::framework
