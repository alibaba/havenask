<a name="QYo3Y"></a>

### Description

Havenask supports table-valued functions (TVFs). <br />In a SQL statement, you can use a TVF to perform an operation on a table. The inputs of a TVF include one or more scalar parameters and a SQL statement that specifies a table. The output of a TVF is a table.





<a name="jExuk"></a>

### Syntax

```sql

SELECT:

  SELECT [ DISTINCT ]

    { * | projectItem [, projectItem ]* }

  FROM Table({TVF})

    [ WHERE booleanExpression ]

    [ GROUP BY { groupItem [, groupItem ]* } ]

    [ ORDER BY { orderByItem [, OrderByItem ]* }]

    [ HAVING booleanExpression ]

    [ LIMIT number]

    [ OFFSET number]



TVF:

	{TVF Name}({scalar parameter}*, {SELECT})

```

<a name="iQ5cd"></a>

### Examples

<a name="ZOs8X"></a>

##### Specify a single TVF in a statement

```sql

SELECT

    *

FROM

    TABLE (

        one_part_tvf(

            'rtp_url',

            123,

            (

                SELECT i1, i2, d3, d4 FROM t1

            )

        )

    )

```

<a name="L4Ex2"></a>

##### Specify multiple nested TVFs in a statement

You cannot specify nested TVFs in the following format:

```sql

SELECT

    *

from

    TABLE (

        tvf1(

            tvf2(

                (

                    select

                        *

                    from

                        t1

                )

            )

        )

    )

```

You can specify nested TVFs in the following format:

```sql

SELECT

    *

FROM

    TABLE (

        one_part_tvf_enable_shuffle(

            'rtp_url',

            123,

            (

                SELECT

                    i1, i2

                FROM

                    TABLE (

                        one_part_tvf_enable_shuffle(

                            'rtp_url_2',

                            234,

                            (

                                SELECT i1, i2, d3, d4 FROM t1

                            )

                        )

                    )

            )

        )

    )

```



<a name="cLMVl"></a>

### Built-in TVFs

[View](https://yuque.antfin-inc.com/aios/sql/aq97qe)