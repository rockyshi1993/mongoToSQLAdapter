
const Curd = require('../lib/curd');
const Mysql = require('./msyql');

(async () => {

    const db = (tableName)=>new Curd(Mysql, tableName,true, false,true);

    /**
     * 插入单条数据
     * - `data`: 插入的对象
     * - `options.upsert`: 若 `true`，且 `name` 字段有唯一索引，则更新已存在数据
     * - `options.upsert`: 若 `false`，不执行更新，命中唯一索引会抛出错误
     * - `options.fields`: 指定 `upsert` 时更新的字段，若为空，则更新所有字段
     */
    // const insertOne = await db('test').insertOne(
    //     { name: 'test3', age: 7, data: { field1: 1, field2: 1 }},
    //     { upsert: true,  fields: ['data','age'] }
    // )

    /**
     * 插入多条数据
     * - `data`: 插入的数组
     * - `options.upsert`: 若 `true`，且 `name` 字段有唯一索引，则更新已存在数据，注意，数组中每组对象字段要保持一致才生效
     * - `options.upsert`: 若 `false`，不执行更新，命中唯一索引会抛出错误
     * - `options.fields`: 指定 `upsert` 时更新的字段，若为空，则更新所有字段
     */
    // const insertMany = await db('test').insertMany(
    //     [
    //         { name: 'test7', age: 10, data:{ field1: 0, field2: 0 } },
    //         { name: 'test8', age: 11, data:{ field1: 1, field2: 1 } }
    //     ],
    //     {upsert: true, fields: ['data']  }
    // )

    // 单个更新
    // const updateOne = await db('test').updateOne(
    //     { name:'test' },
    //     { age: 100 }    // or { $set: { age: 100 } }
    // )

    // 批量更新
    // const updateMany = await db('test').updateMany(
    //     { age: 20 },
    //     { age: 100 }    // or { $set: { age: 100 } }
    // )

    // 字段值自增\自减
    // const inc = await db('test').updateOne(
    //     { name: 'test' },
    //     {
    //         $inc: { age: 1 }  // 自减 -1
    //     }
    // )

    // 将age字段乘以\除以
    // const mul = await db('test').updateOne(
    //     { age: 20 },
    //     {
    //         $mul: { age: 2 }  // 如果要实现除法可以这样 { age: 0.2 }
    //     }
    // )

    // 单个删除
    // const deleteOne = await db('test').deleteOne(
    //     { name: "test8" }
    // )

    // 批量删除
    // const deleteMany = await db('test').deleteMany(
    //     { age: 10 }
    // )

    // 统计总记录
    // const count = await db('test').count({ name: "test"});

    // 单个查找
    // const findOne = await db('users').findOne(
    //     {
    //         query: {name:'Alice'},
    //         project:['id','name','orders.user_id AS test','payments.amount'],  // orders.user_id AS test 设置别名为 test , 如未设置会自动别名 orders_user_id
    //         joins:[
    //             {
    //                 tableName: "orders",
    //                 // joinType: "LEFT JOIN",   // 可选，默认 LEFT JOIN
    //                 // alias: "o",              // 可选，默认为表名
    //                 on: "orders.user_id = users.id"
    //             },
    //             {
    //                 tableName: "payments",
    //                 on: "orders.user_id = payments.id"
    //             }
    //         ],
    //     }
    // );

    // 批量查找
    // const findMany = await db('users').findMany({
    //     query: {},
    //     project:['id','email','orders.total_amount AS orders_total_amount'],
    //     joins:[
    //         {
    //             tableName: "orders",
    //             alias: 'orders',
    //             on: "orders.user_id = users.id"
    //         },
    //         { tableName: "payments", on: "orders.user_id = payments.id"}
    //     ],
    //     skip:0,
    //     limit:10,
    //     sort:{ "users.id": 1 }
    // });

    // 简单分页查找
    // const findPaginate = await db('users').findPaginate({
    //     query: {'users.id':{$gte:1}},
    //     project:['id','email','orders.total_amount AS orders_total_amount'],
    //     joins:[
    //         {
    //             tableName: "orders",
    //             alias: 'orders',
    //             on: "orders.user_id = users.id"
    //         },
    //         { tableName: "payments", on: "orders.user_id = payments.id"}
    //     ],
    //     page:1,
    //     pageSize:11,
    //     sort:{ "users.id": 1 },
    //     total:true
    // });

    // 聚合分页查询
    // const aggregatePaginate = await db('users').aggregatePaginate({
    //     query: { },
    //     // project:['id','email','data','orders.total_amount'],
    //     // page:1,
    //     // pageSize:2,
    //     sort:{ 'users.id': 1 },
    //     joins:[
    //         {
    //             tableName: "orders",
    //             alias: 'orders',
    //             on: "orders.user_id = users.id"
    //         }
    //     ],
    //     group:{
    //         _id: "$users.email",        //根据 users.email 分组
    //         sum: { $sum: "$orders.total_amount" },
    //         avg: { $avg: "$orders.total_amount" },
    //         max: { $max: "$orders.total_amount" },
    //         min: { $min: "$orders.total_amount" },
    //         count: { $count: "$orders.total_amount" },
    //         email: { $first: "$users.email" }
    //     }
    // })

    // 聚合查询
    // const aggregate = db('users').aggregate
    //     .project(['user_id', 'name'])
    //     .match({ login_date: { $gte: '2025-02-11 00:00:00', $lt: '2025-02-12 00:00:00' } })
    //     .group({ _id: "$user_id" })
    //     .subQuery(                     //子查询阶段
    //         'user_order',   // 子查询表名 映射名称，不填写默认为 orders
    //         db('orders').aggregate
    //         .project(['user_id', 'name'])
    //         .match({ user_id: { $eq: { $col: 'users.user_id' } } }) // 将 orders.user_id 与外层的 users.user_id 进行比较
    //         .count('total')
    //     )
    //     .sort({ name: 1 })
    //     .limit(10)
    //     .toSQL();     // toArray 则返回执行结果
















    // ------------ 更复杂的聚合查询暂未适配，请用原生SQL -----------

})();


// 还需要写每个筛选符示例

// 如果字段类型是 string，例如 address 值是：field1,field2,field3,可以这样
// const findInSet = await db('test').findOne(
//     {
//         address: { $findInSet: 'field1' }
//     },
// )
