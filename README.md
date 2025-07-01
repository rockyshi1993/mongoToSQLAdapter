# CRUD 操作示例

## 目录
- [准备工作](#准备工作)
- [插入数据](#插入数据)
    - [插入单条数据](#插入单条数据)
    - [插入多条数据](#插入多条数据)
- [更新数据](#更新数据)
    - [单个更新](#单个更新)
    - [批量更新](#批量更新)
    - [自增/自减字段值](#自增自减字段值)
    - [对字段值进行乘法或除法运算](#对字段值进行乘法或除法运算)
- [删除数据](#删除数据)
    - [单个删除](#单个删除)
    - [批量删除](#批量删除)
- [查询数据](#查询数据)
    - [统计总记录数](#统计总记录数)
    - [单个查找](#单个查找)
    - [批量查找](#批量查找)
    - [分页查询](#分页查询)
    - [聚合分页查询](#聚合分页查询)
    - [聚合查询](#聚合查询)

本指南提供了使用 `Curd` 库执行常见数据库操作（如插入、更新、删除和查询）的示例。

## 准备工作

首先，你需要安装 `mongotomysqladapter` 包。你可以使用以下命令来安装：

```sh
npm install mongotomysqladapter
```

确保已安装所需依赖项并正确配置了数据库连接：

```javascript
const mongoToSQLAdapter = require('mongotomysqladapter');
const Mysql = require('./mysql2');     // 参考 example 配置
const db = (databaseName) => new mongoToSQLAdapter(Mysql, databaseName, true, false, true);
```

## 插入数据

### 插入单条数据
- `data`: 要插入的对象。
- `options.upsert`: 如果为 `true` 且 `name` 字段具有唯一索引，则会更新现有记录；如果为 `false` 则不会执行更新，而是抛出错误。
- `options.fields`: 指定在 `upsert` 操作中要更新的字段列表，若为空则更新所有字段。

```javascript
// 示例
await db('test').insertOne(
    { name: 'test3', age: 7, data: { field1: 1, field2: 1 } },
    { upsert: true, fields: ['data', 'age'] }
)
```

### 插入多条数据
- `data`: 要插入的数据数组。
- `options.upsert`: 规则同上。
- 注意：数组中的每组对象字段需保持一致才生效。

```javascript
// 示例
await db('test').insertMany(
    [
        { name: 'test7', age: 10, data: { field1: 0, field2: 0 } },
        { name: 'test8', age: 11, data: { field1: 1, field2: 1 } }
    ],
    { upsert: true, fields: ['data'] }
)
```

## 更新数据

### 单个更新
```javascript
// 示例
await db('test').updateOne(
    { name: 'test' },
    { age: 100 } // 或者 { $set: { age: 100 } }
)
```

### 批量更新
```javascript
// 示例
await db('test').updateMany(
    { age: 20 },
    { age: 100 } // 或者 { $set: { age: 100 } }
)
```

### 自增/自减字段值
```javascript
// 示例
await db('test').updateOne(
    { name: 'test' },
    {
        $inc: { age: 1 }  // 若要实现自减可以设置为 -1
    }
)
```

### 对字段值进行乘法或除法运算
```javascript
// 示例
await db('test').updateOne(
    { age: 20 },
    {
        $mul: { age: 2 }  // 若要实现除法则可以设置为 0.2
    }
)
```

## 删除数据

### 单个删除
```javascript
// 示例
await db('test').deleteOne(
    { name: "test8" }
)
```

### 批量删除
```javascript
// 示例
await db('test').deleteMany(
    { age: 10 }
)
```

## 查询数据

### 统计总记录数
```javascript
// 示例
await db('test').count({ name: "test" });
```

### 单个查找
```javascript
// 示例
await db('users').findOne(
    {
        query: { name: 'Alice' },
        project: ['id', 'name', 'orders.user_id AS test', 'payments.amount'],  // orders.user_id AS test 设置别名为 test
        joins: [
            {
                tableName: "orders",
                on: "orders.user_id = users.id"
            },
            {
                tableName: "payments",
                on: "orders.user_id = payments.id"
            }
        ]
    }
)
```

### 批量查找
```javascript
// 示例
await db('users').findMany({
    query: {},
    project: ['id', 'email', 'orders.total_amount AS orders_total_amount'],
    joins: [
        {
            tableName: "orders",
            alias: 'orders',
            on: "orders.user_id = users.id"
        },
        { tableName: "payments", on: "orders.user_id = payments.id" }
    ],
    skip: 0,
    limit: 10,
    sort: { "users.id": 1 }
});
```

### 分页查询
```javascript
// 示例
await db('users').findPaginate({
    query: { 'users.id': { $gte: 1 } },
    project: ['id', 'email', 'orders.total_amount AS orders_total_amount'],
    joins: [
        {
            tableName: "orders",
            alias: 'orders',
            on: "orders.user_id = users.id"
        },
        { tableName: "payments", on: "orders.user_id = payments.id" }
    ],
    page: 1,
    pageSize: 11,
    sort: { "users.id": 1 },
    total: true
});
```

### 聚合分页查询
```javascript
// 示例
await db('users').aggregatePaginate({
    query: {},
    sort: { 'users.id': 1 },
    joins: [
        {
            tableName: "orders",
            alias: 'orders',
            on: "orders.user_id = users.id"
        }
    ],
    group: {
        _id: "$users.email",        // 根据 users.email 分组
        sum: { $sum: "$orders.total_amount" },
        avg: { $avg: "$orders.total_amount" },
        max: { $max: "$orders.total_amount" },
        min: { $min: "$orders.total_amount" },
        count: { $count: "$orders.total_amount" },
        email: { $first: "$users.email" }
    }
});
```

### 聚合查询
```javascript
// 示例
const aggregate = db('users').aggregate
    .project(['user_id', 'name'])
    .match({ login_date: { $gte: '2025-02-11 00:00:00', $lt: '2025-02-12 00:00:00' } })
    .group({ _id: "$user_id" })
    .subQuery(                     // 子查询阶段
        'user_order',   // 子查询表名 映射名称，不填写默认为 orders
        db('orders').aggregate
        .project(['user_id', 'name'])
        .match({ user_id: { $eq: { $col: 'users.user_id' } } }) // 将 orders.user_id 与外层的 users.user_id 进行比较
        .count('total')
    )
    .sort({ name: 1 })
    .limit(10)
    .toSQL();     // toArray 则返回执行结果
```

> 更复杂的聚合查询暂未适配，请用原生SQL

请检查以上内容是否符合您的需求。如果有任何需要调整的地方，请告诉我！
