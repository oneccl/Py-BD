

# 查询优化器：RBO与CBO

# 数据库系统发展简史
# 数据库系统诞生于20世纪60年代中期，至今已有近50多年的历史，其发展经历了三代演变，造就了四位图灵奖得主，并发展成为一门计算机基础学科，带动了一个巨大的软件产业

# 20世纪60年代后期出现了一种新型数据库软件：决策支持系统（Decision Support System，DSS），其目的是让管理者在决策过程中更有效地利用数据信息
# 决策支持系统是辅助决策者通过数据、模型和知识，以人机交互方式进行半结构化或非结构化决策的计算机应用系统。它是管理信息系统向更高一级发展而产生的先进信息管理系统
# 1988年，为解决企业集成问题，爱尔兰公司的Barry Devlin和Paul Murphy创造性的提出了一个新的概念：信息仓库（InformationWarehouse）
# 1991年，Bill Inmon出版了一本“如何构建数据仓库（DataWarehouse）”的书，使得数据仓库真正开始应用，Bill Inmon凭借这本书奠定了其在数据仓库建设的位置，被称之为“数据仓库之父”
# 数据仓库是决策支持系统和联机分析应用数据源的结构化数据环境，是一个面向主题的（SubjectOriented）、集成的（Integrated）、相对稳定的（Non-Volatile）、反映历史变化（TimeVariant）的数据集合，用于支持管理决策（DecisionMaking Support）

# 数据库系统是操作系统之上最重要的基础设施之一，被称为软件产业的常青树，特别是它所支撑起来的大数据、人工智能应用，更是发展迅猛
# 随着数据库领域的快速发展以及数据量的爆发式增长，如何对海量数据进行管理、分析、挖掘变得尤为重要。SQL优化器正是在这种背景下诞生的

# 查询优化器
# 查询优化器是传统数据库的核心模块，也是大数据计算引擎的核心模块，开源大数据引擎如Impala、Presto、Drill、Spark、Hive等都有自己的查询优化器
# 数据库系统主要由三部分组成：解析器、优化器和执行引擎

# 其中优化器是数据库中用于把关系表达式转换成执行计划的核心组件，很大程度上决定了一个系统的性能。特别是对于现代大数据系统，执行计划的搜索空间异常庞大，研究人员研究了许多方法对执行计划空间进行裁剪，以减少搜索空间的代价
# 在当今数据库系统领域，查询优化器可以说是必备组件，不管是关系型数据库系统Oracle、MySQL，流处理领域的Flink、Storm，批处理领域的Hive、SparKSQL，还是文本搜索领域的Elasticsearch等，都会内嵌一个查询优化器
# 有的数据库系统会采用自研的优化器，而有的则会采用开源的查询优化器框架，例如Oracle数据库的查询优化器，则是Oracle公司自研的一个核心组件，负责解析SQL，其目的是按照一定的原则来获取目标SQL在当前情形下执行的最高效执行路径
# 而Apache Calcite是一个独立于存储与执行的SQL优化引擎，广泛应用于开源大数据计算引擎中，如Hive、Flink、Kylin等，另外，MaxCompute也使用了Calcite作为优化器框架

# 关于查询优化器所要解决的核心问题：具有多个连接操作的复杂查询优化。不少学者相继提出了基于左线性树的查询优化算法、基于右线性树的查询优化算法、基于片段式右线性树的查询优化算法、基于浓密树的查询优化算法、基于操作森林的查询优化算法等。这些算法在搜索代价和最终获得的查询计划的效率之间有着不同的权衡
# 总的来说，查询优化器在很大程度上决定了一个数据库系统的性能，优化器的作用就好比找到两点之间的最短路径

# 查询优化器分类
# 查询优化器分为两类：基于规则的优化器(Rule-Based Optimizer，RBO)和基于代价的优化器(Cost-Based Optimizer，CBO)

# RBO即基于规则的优化器，该优化器按照硬编码在数据库中的一系列规则来决定SQL的执行计划。以Oracle数据库为例，RBO根据Oracle指定的优先顺序规则，对指定的表进行执行计划的选择。比如在规则中：索引的优先级大于全表扫描
# 通过Oracle的这个例子我们可以感受到，在RBO中，有着一套严格的使用规则，只要你按照规则去写SQL语句，无论数据表中的内容怎样，也不会影响到你的“执行计划”，也就是说RBO对数据不“敏感”。这就要求开发人员非常了解RBO的各项细则，不熟悉规则的开发人员写出来的SQL性能可能非常差
# 但在实际的过程中，数据的量级会严重影响同样SQL的性能，这也是RBO的缺陷所在。毕竟规则是死的，数据是变化的，所以RBO生成的执行计划往往是不可靠的，不是最优的

# CBO即基于代价的优化器，该优化器通过根据优化规则对关系表达式进行转换，生成多个执行计划，然后CBO会通过根据统计信息(Statistics)和代价模型(Cost Model)计算各种可能“执行计划”的代价(COST)，并从中选用COST最低的执行方案，作为实际运行方案
# CBO依赖数据库对象的统计信息，统计信息的准确与否会影响CBO做出最优的选择。以Oracle数据库为例，统计信息包括SQL执行路径的I/O、网络资源、CPU的使用情况
# 目前各大数据库和大数据计算引擎都倾向于使用CBO，例如从Oracle 10开始，Oracle已经彻底放弃RBO，转而使用CBO；而Hive从0.14.0版本开始也引入了CBO

# 以下是一个例子：
# 众所周知，join是非常耗时的一个操作，且性能与join双方数据量大小呈线性关系（通常情况下）。那么很自然的一个优化，就是尽可能减少join双方的数据量，于是就想到了先filter再join这样一个Rule。而非常多个类似的Rule，就构成了RBO
# 但后面开发者发现，RBO确实能够在通用情况下对SQL进行优化，但在有些需要本地状态才能优化的场景却无能为力。比如某个计算引擎，在数据量较小的时候，可以做一些特殊的优化操作，这种场景下RBO无能为力
# 此时，CBO就成为首选。例如Spark的join：在Spark中，join会触发Shuffle操作，这是非常消耗资源的。而Spark有三种类型的join：
# Broadcast Hash Join：将小的表广播到所有节点，在内存中hash碰撞进行join，这种join避免节点间Shuffle操作，性能最好，但条件也苛刻
# Hash Join：即普通的Shuffle Join
# Sort Merge Join：先排序然后join，类似归并的思想，排序后能减少一些hash碰撞后的数据扫描，在join双方都是大表的情况下性能较好
# 选择哪种类型的join，就要根据数据来选择，如果一方是小表，就用Broadcast Hash Join，如果双方都是大表，就用Sort Merge Join，否则就用Hash Join。而这就需要用到CBO


# 查询优化器执行过程
# 无论是RBO，还是CBO都包含了一系列优化规则，这些优化规则可以对关系表达式进行等价转换，常见的优化规则包括：谓词下推、列裁剪、常量折叠等

# 在这些优化规则的基础上，就能对关系表达式做相应的等价转换，从而生成执行计划

# RBO：
# 1）Transformation：遍历关系表达式，只要模式能够满足特定优化规则就进行转换，生成了一个逻辑执行计划（仅逻辑上可行）
# 2）Build Physical Plan：将逻辑执行计划Build成物理执行计划，即决定各个Operator的具体实现。如join算子的具体实现选择Broadcast Hash Join还是Sort Merge Join

# CBO：
# 1）Exploration：根据优化规则进行等价转换，生成多个等价关系表达式，此时原有关系表达式会被保留
# 2）Build Physical Plan：根据CBO实现的两种模型Volcano模型(先Explore后Build)和Cascades模型(边Explore边Build)决定各个Operator的具体实现
# 3）Find Best Plan：根据统计信息计算各个执行计划的Cost，选择Cost最小的执行计划执行

# RBO与CBO的区别在于，RBO只会应用提供的Rule，而CBO会根据Cost智能应用Rule，求出一个最低Cost的执行计划，CBO也是基于Rule的

# CBO框架Calcite简介

# Calcite的产生背景
# 在上世纪，关系型数据库系统基本主导了数据处理领域，但是在Google三篇创世纪论文发表后，大家开始意识到，一种适合所有场景的数据库是不存在的
# 事实上，今天也确实是这样，许多特定场景下的数据处理系统已经成为主流，例如流处理领域的Flink、Storm，批处理领域的Hive、SparkSQL，文本搜索领域的Elasticsearch等
# 而在开发不同特定场景的数据处理系统的时候，主要存在两个问题：
# 1）每种系统都需要查询语言（SQL）及相关拓展（如流式SQL查询），或是开发过程中碰到查询优化问题，没有一个统一框架，每个系统都要一套自己的查询解析框架，重复造轮子
# 2）开发的这些系统通常要对接或集成其他系统，比如Kylin集成MR，Spark，Hbase等，如何支持跨异构数据源成为一大问题
# 因此，Calcite应运而生

# Calcite官网：https://calcite.apache.org/

# Calcite中的优化器
# Calcite中的优化器详解参考文章：https://matt33.com/2019/03/17/apache-calcite-planner/

# 参考文章：
# https://zhuanlan.zhihu.com/p/40478975
# https://www.cnblogs.com/mzq123/p/10398701.html
# https://www.cnblogs.com/JasonCeng/p/14199298.html
# https://www.cnblogs.com/listenfwind/p/13192259.html




