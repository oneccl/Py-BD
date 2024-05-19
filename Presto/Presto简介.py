"""
Created with PyCharm.
Author: CC
E-mail: 203717588@qq.com
Date: 2023/12/2
Time: 18:28
Description:
"""

# Presto查询引擎

# 背景

# Facebook的数据仓库存储在少量大型Hadoop/HDFS集群。Hive是Facebook在十几年前专为Hadoop打造的一款数据仓库工具。在以前，Facebook的科学家和分析师一直依靠Hive来做数据分析
# Hive使用MapReduce作底层计算框架，是专为批处理设计的
# 随着数据源的多样化、数据仓库的不断扩展以及数据湖的发展，使用Hive及时获得有用的见解可能变得困难。例如使用Hive进行一个简单的数据查询可能需要花费几分钟甚至几小时，这显然不能满足企业级交互式查询的需求
# 于是Facebook调研了其他比Hive更快的工具，但它们要么在功能有所限制，要么就太简单，以至于无法操作Facebook庞大的数据仓库

# 在Facebook试用了一些外部项目但都不合适后，2012年秋季，他们决定自己开发，Presto应运而生
# 2013年，Facebook正式宣布开源Presto。2015年，Netflix展示了Presto实际上比Hive快10倍，在某些情况下甚至更快

# Hive的问题主要在于它将MapReduce查询的中间结果存储在磁盘上，这会导致在磁盘间产生大量I/O开销。Presto凭借其新的架构和内存引擎，将显着降低其延迟和查询速度，从而允许更多的交互式查询
# Presto的用例范围从交互式即席查询到长时间运行的批量ETL管道，使其能够灵活地适应各种数据驱动的用例和应用程序

# 什么是Presto？

# Presto是由FaceBook开源的一个MPP SQL引擎，主要用来解决Facebook海量Hadoop数据仓库的高延迟交互分析问题
# Facebook版本的Presto更多的是以解决企业内部需求功能为主，也叫PrestoDB，版本号以0.xxx来划分，例如目前的最新版本0.284版本
# 后来，Presto其中的几个人出来创建了更通用的Presto分支，取名PrestoSQL，版本号以xxx来划分，例如315版本，这个开源版本也是更为被大家通用的版本
# 为了更好的与Facebook的Presto进行区分，PrestoSQL于2020年12月27日改名为Trino，除了名字改变了其他都没变。不管是PrestoDB还是PrestoSQL，它们“本是同根生”，因此它们的大部分的机制原理是一样的
# PrestoDB官网：https://prestosql.io/或 https://prestodb.io/
# PrestoDB文档1：https://prestodb.io/docs/0.284/overview.html
# PrestoDB文档2：https://dzone.com/refcardz/getting-started-with-prestodb
# PrestoSQL官网：https://trino.io
# PrestoSQL文档：https://trino.io/docs/current/overview.html

# Presto是一款灵活、可扩展的分布式SQL查询引擎，旨在填补Hive在速度和灵活性（对接多种数据源）上的不足。相似的SQL On Hadoop产品还有Impala和Spark SQL等
# Presto本身并不存储数据，但是可以接入多个异构数据源上的大型数据集，并且支持跨数据源的级联查询。Presto基于内存运算，速度快，实时性高
# Presto是一个OLAP工具，擅长对海量数据进行复杂的分析。Presto可以解析SQL，但它不是一个标准的数据库。不是MySQL、Oracle的替代品，也不能用来处理在线事务（OLTP）
# 和MySQL相比，MySQL是一个数据库，具有存储和计算分析能力；而Presto和Hive、Impala、Spark一样只有计算分析能力
# 和Hive相比，Hive只能从HDFS中读取数据，而Presto⽀持跨数据源进行数据查询和分析
# Presto支持跨源查询，包括HDFS、Hive、关系数据库（MySQL、Oracle等）、NoSQL（MongoDB、Redis等）、Kafka以及一些专有数据存储。一条Presto查询可以将多个数据源的数据进行合并，可以跨越整个组织进行分析

# 特性
# Presto使用Java语言进行开发，具备易用、高性能和强扩展能力等特点，具体如下：
'''
- 完全支持ANSI SQL
- 支持丰富的数据源，例如：Hive、Hudi、Iceberg、Delta Lake、MySQL和PostgreSQL等
- 支持高级数据结构：数组和Map数据、JSON数据、GIS（Geographic Information System）数据等
- 功能扩展能力强，提供了多种扩展机制：扩展数据连接器、自定义数据类型、自定义SQL函数
- 流水线：基于Pipeline处理模型数据在处理过程中实时返回给用户
- 监控接口完善：提供友好的Web UI，可视化的呈现查询任务执行过程，支持JMX协议
- 多租户：它支持并发执行数百个内存、I/O以及CPU密集型的负载查询，并支持集群规模扩展到上千个节点
- 联邦查询：它可以由开发者利用开放接口自定义开发针对不同数据源的连接器（Connector)，从而支持跨多种不同数据源的联邦数据查询
- 内在特性：为了保证引擎的高效性，Presto还进行了一些优化，例如基于JVM运行，Code-Generation等
'''
# 应用场景
# Presto主要用来处理响应时间小于几秒到几分钟的场景
'''
- 交互式分析：Presto的即席计算特性和内部设计机制就是为了能够更好地支持用户进行交互式分析
- 批量ETL：PB级海量数据跨源查询分析；由于完全基于内存并行计算，Presto不适合多个大表的JOIN场景，容易OOM
'''

# 架构

# Presto查询引擎是一个Master-Slave的架构，由一个Coordinator节点，一个Discovery Server节点，多个Worker节点组成，Discovery Server通常内嵌于Coordinator节点中。由客户端提交查询，从Presto命令行CLI提交到Coordinator
# Coordinator负责解析SQL语句，生成执行计划，分发执行任务给Worker节点执行。Worker节点负责实际执行查询任务。Worker节点启动后向Discovery Server服务注册，Coordinator从Discovery Server获得可以正常工作的Worker节点。如果配置了Hive Connector，需要配置一个Hive MetaStore服务为Presto提供Hive元信息，Worker节点与HDFS交互读取数据
# 图
# Presto通过客户端连接到Coordinator，例如JDBC驱动或Presto Cli。然后Coordinator与Worker进行协作，Worker负责访问数据源获取数据
# Coordinator是一个Presto服务，主要负责处理收到的查询，并管理Worker处理这些查询。Worker是Presto中负责执行Task和处理数据的服务
# Discovery Service通常运行在Coordinator上，允许Worker注册到集群中。Client、Coordinator以及Worker之间的数据传输和通信都使用基于HTTP/HTTPS的REST接口

# Presto两类服务器：Coordinator和Worker
# Coordinator
# Coordinator服务器主要用于解析语句、执行计划分析和管理Presto的Worker节点。Coordinator跟踪每个Work的活动情况并协调查询语句的执行。Coordinator为每个查询建立模型，模型包含多个Stage，每个Stage再转为Task分发到不同的Worker上执行
# Coordinator还负责从Worker获取结果并返回最终结果给Client。Coordinator与Worker、Client通信是通过REST API
# Worker
# Worker是负责执行任务和处理数据。Worker从Connector获取数据。Worker之间会交换中间数据
# 当Worker启动时，会广播自己去发现Coordinator，并告知Coordinator它是可用的，随时可以接受Task。Worker与Coordinator、Worker通信是通过REST API

# Presto概念
# Connector
# Connector是适配器，用于Presto和数据源（如Hive、RDBMS）的连接。类似JDBC，但却是Presto的SPI的实现，使用标准的API来与不同的数据源交互
# Presto有几个内建Connector：JMX Connector、System Connector（用于访问内建的System Table）、Hive Connector、TPCH（用于TPC-H基准数据）。还有很多第三方的Connector，所以Presto可以访问不同数据源的数据
# Catalog
# Catalog表示数据源，一个Catalog包含Schema和Connector。例如，你配置JMX的Catalog，通过JXM Connector访问JXM信息。当你执行一条SQL语句时，可以同时运行在多个Catalog
# 每个Catalog都有一个特定的Connector。如果你使用Catalog配置文件，你会发现每个文件都必须包含connector.name属性，用于指定Catalog管理器（创建特定的Connector使用）。多个Catalog用同样的Connector访问的是同样的数据库。例如，有两个Hive集群，你可以在一个Presto集群上配置两个Catalog，两个Catalog都使用Hive Connector，从而达到可以查询两个Hive集群
# Presto处理Table时，是通过表的完全限定（Fully-Qualified）名来找到Catalog。例如，一个表的权限定名是hive.test_data.test，则test是表名（库中的表），test_data是Schema（数据库），hive是Catalog（数据源）。Catalog的定义文件在Presto的配置目录中
# Schema
# Schema类似于MySQL中的数据库，Schema是用于组织Table。把Catalog和Schema结合在一起来包含一组的表。当通过Presto访问Hive或MySQL时，一个Schema会同时转为Hive和MySQL的同等概念
# Table
# Table类似于MySQL数据库中的表，Table跟关系型的表定义一样，但数据和表的映射是交给Connector

# Presto数据模型
# 1）Presto三层表结构
# - Catalog：对应某一类数据源，例如Hive的数据或MySQL的数据
# - Schema：对应该数据源中的数据库
# - Table：对应该数据库中的表
# 2）Presto存储单元
# Page：多行数据的集合，包含多个列的数据，内部仅提供逻辑行（Row），实际以列式存储
# Block：一列数据，根据不同类型的数据，通常采取不同的编码方式
# 3）不同类型的Block
# - Array类型的Block，应用于固定宽度的类型，例如int、long、double。Block由两部分组成：
#   - boolean valueIsNull[]：表示每一行是否有值
#   -T values[]：每一行的具体值
# - 可变宽度的Block，应用于String类型数据，由三部分信息组成：
#   - Slice：所有行的数据拼接起来的字符串
#   - int offsets[]：每一行数据的起始偏移位置。每一行的长度等于下一行的起始偏移减去当前行的起始偏移
#   - boolean valueIsNull[]：表示某一行是否有值。如果有某一行无值，那么这一行的偏移量等于上一行的偏移量
# - 固定宽度的String类型的Block，所有行的数据拼接成一长串Slice，每一行的长度固定
# - 字典类型的Block：对于某些列，Distinct值较少，适合使用字典保存。主要有两部分组成：
#   - 字典：可以是任意一种类型的Block(甚至可以嵌套一个字典Block)，Block中的每一行按照顺序排序编号
#   - int ids[]：表示每一行数据对应的Value在字典中的编号。在查找时，首先找到某一行的id，然后到字典中获取真实的值

# Presto查询过程（执行原理）

# Presto是一个MPP风格的数据库查询引擎，它不依赖于运行Presto服务器的垂直扩展，他可以以水平的方式横向扩展集群，即可以通过增加节点来增大其处理能力。利用这种架构Presto可以跨集群的对大量数据进行处理。Presto的每个节点作为一个单独的服务运行，运行Presto的节点彼此相互协作，构成了Presto集群
# 图

# - 用户通过Presto的JDBC或Cli提交一个查询到Coordinator，Cli使用HTTP协议与Coordinator通信
# - Coordinator收到查询请求后，对语句进行解析，生成查询执行计划，并根据生成的执行计划生成Stage和Task，并将Task分发到需要处理数据的Worker上进行分析
# - Worker负责执行Task，Task通过Connector从数据源中读取需要的数据
# - 上游Stage输出的结果给到下游Stage作为输入，每个Stage的每个Task在Worker内存中进行并行计算和处理
# - Client从提交查询后，就一直监听Coordinator中的查询结果，一有结果就立即输出，直到轮循所有的结果都返回则本次查询结果结束

# Presto优缺点
# MapReduce VS Presto
# 优点
# Presto和Hive都能够处理PB级别的海量数据分析，但Presto是基于内存运算，减少没必要的硬盘I/O，所以更快
# Presto能够连接多个数据源，跨数据源连表查，如从Hive查询大量网站访问记录，然后从MySQL中匹配出设备信息
# Presto部署也比Hive简单，因为Hive是基于HDFS的，需要先部署HDFS
# 缺点
# Presto虽然能够处理PB级别的海量数据分析，但不是代表Presto是把PB级别的数据都放在内存中计算的。而是根据场景，如count、avg等聚合运算，是边读数据边计算，再清内存，再读数据再计算，这种耗的内存并不高。但是JOIN查询，就可能产生大量的临时数据，因此速度会变慢，反而Hive此时会更擅长
# 为了达到实时查询，可能会想到用它直连MySQL来操作查询，这效率并不会提升，瓶颈依然在MySQL，此时还引入网络瓶颈，所以会比原本直接操作数据库要慢

# Presto、Impala性能比较
# Presto和Impala都是基于内存的查询引擎，主要区别在于：Impala性能稍领先于Presto，但是Presto在数据源支持更丰富，包括Hive、图数据库、传统关系型数据库、Redis等
# https://blog.csdn.net/u012551524/article/details/79124532










