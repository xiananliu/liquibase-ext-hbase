# liquibase-ext-hbase
liquibase support hbase-phoenix 

为liquibase 添加phoenix 支持，基于https://github.com/manirajv06/liquibase-hbase 修改而来
主要修改项目结构从而适配了liquibase 的spi 机制,使得该项目可以支持liquibase 4.x.x 

目前只在 hbase 2.0 和phoenix 5.0.0上测试过，其他版本暂未测试

#使用方法

添加本项目依赖即可

```
<dependency>
  <groupId>io.github.xiananliu</groupId>
  <artifactId>liquibase-ext-hbase</artifactId>
  <version>1.0</version>
</dependency>

```