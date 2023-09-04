import os

def get_role_type():
    return os.getenv("roleType")

def get_enable_local_access():
    return os.getenv("enableLocalAccess")

def get_special_catalog_list():
    return os.getenv("specialCatalogList")

def get_enable_multi_partition():
    value = os.getenv("enableMultiPartition")
    if value and value.lower() != "false":
        return True
    else:
        return False

def get_disable_soft_failure():
    value = os.getenv("disableSoftFailure")
    if value and value.lower() == "true":
        return True
    else:
        return False
