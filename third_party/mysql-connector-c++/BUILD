load('//bazel:defs.bzl', 'rpm_library')
hdrs = [
    'jdbc/cppconn/build_config.h', 'jdbc/cppconn/config.h',
    'jdbc/cppconn/connection.h', 'jdbc/cppconn/datatype.h',
    'jdbc/cppconn/driver.h', 'jdbc/cppconn/exception.h',
    'jdbc/cppconn/metadata.h', 'jdbc/cppconn/parameter_metadata.h',
    'jdbc/cppconn/prepared_statement.h', 'jdbc/cppconn/resultset.h',
    'jdbc/cppconn/resultset_metadata.h', 'jdbc/cppconn/sqlstring.h',
    'jdbc/cppconn/statement.h', 'jdbc/cppconn/variant.h',
    'jdbc/cppconn/version_info.h', 'jdbc/cppconn/warning.h',
    'jdbc/mysql_connection.h', 'jdbc/mysql_driver.h', 'jdbc/mysql_error.h',
    'mysqlx/common.h', 'mysqlx/common/api.h', 'mysqlx/common/error.h',
    'mysqlx/common/op_if.h', 'mysqlx/common/settings.h', 'mysqlx/common/util.h',
    'mysqlx/common/value.h', 'mysqlx/common_constants.h',
    'mysqlx/devapi/collations.h', 'mysqlx/devapi/collection_crud.h',
    'mysqlx/devapi/common.h', 'mysqlx/devapi/crud.h',
    'mysqlx/devapi/detail/crud.h', 'mysqlx/devapi/detail/error.h',
    'mysqlx/devapi/detail/result.h', 'mysqlx/devapi/detail/row.h',
    'mysqlx/devapi/detail/session.h', 'mysqlx/devapi/detail/settings.h',
    'mysqlx/devapi/document.h', 'mysqlx/devapi/error.h',
    'mysqlx/devapi/executable.h', 'mysqlx/devapi/mysql_charsets.h',
    'mysqlx/devapi/mysql_collations.h', 'mysqlx/devapi/result.h',
    'mysqlx/devapi/row.h', 'mysqlx/devapi/settings.h',
    'mysqlx/devapi/table_crud.h', 'mysqlx/xapi.h', 'mysqlx/xdevapi.h'
]
shared_lib = 'libmysqlcppconn.so'
rpm_library(
    name='mysql-connector-cpp',
    hdrs=hdrs,
    shared_lib=shared_lib,
    include_path='usr/local/include',
    lib_path='usr/local/lib'
)
