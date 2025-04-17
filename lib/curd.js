
const { MongoInsertBuilder,MongoUpdateBuilder,MongoDeleteBuilder,MongoQueryBuilder,MongoAggregationBuilder } = require('./index');
const Logger = require('./logger');

/**
 * 统一处理错误日志
 * @param logger
 * @param error
 */
function retryError(logger,error){
    logger.error(error);    // 记录错误日志
    // throw error;         // 两处都有捕获错误，此处不用返回，否则会造成重复捕获
}

//显示执行的sql
function showSQL(sql,params,logger,isShowSQL){
    if(isShowSQL){
        logger.info(`SQL: ${sql}`);
        logger.info(`Params: ${params}`);
    }
}

// 在外部扩展 原型，注意使用普通函数以保证 this 正常指向实例
async function toArray (){
    try {
        const { isStream, logger, isShowSQL, db} = this;
        const { sql, params } = this.toSQL();
        showSQL(sql,params,logger,isShowSQL);
        return await db.query(sql, params, isStream)
    }catch(error){
        retryError(this.logger,error);
    }
}

MongoQueryBuilder.prototype.toArray = toArray;
MongoAggregationBuilder.prototype.toArray = toArray;

/**
 * @class Crud
 * @description 提供通用的 CRUD（插入、更新、修改、删除、聚合）操作，基于 MongoDB 语法
 */
class Crud {
    /**
     * @constructor
     * @param {Function} db - 数据库
     * @param {string} tableName - 操作的 MongoDB 集合名称
     * @param {Object} logger - 日志记录器实例
     */
    constructor(db, tableName, isStream, logger, isShowSQL) {
        this.db = db;
        this.tableName = tableName;
        this.isShowSQL = isShowSQL;
        this.isStream = isStream;
        this.logger = Logger(logger); // 绑定日志记录器
        this.insert = new MongoInsertBuilder(tableName); // 初始化插入构造器
        this.update = new MongoUpdateBuilder(tableName); // 初始化更新构造器
        this.delete = new MongoDeleteBuilder(tableName); // 初始化删除构造器

        // 创建 MongoAggregationBuilder 实例后注入 db 属性
        this.aggregate = new MongoAggregationBuilder(tableName);  //初始化聚合构造器
        this.aggregate.db = this.db; // 将 db 注入到实例中
        this.aggregate.isStream = isStream;
        this.aggregate.logger = this.logger;
        this.aggregate.showSQL = showSQL;

        // 创建 MongoQueryBuilder 实例后注入 db 属性
        this.find = new MongoQueryBuilder(tableName);
        this.find.db = this.db; // 将 db 注入到实例中
        this.find.isStream = isStream;
        this.find.logger = this.logger;
        this.find.showSQL = showSQL;

    }

    async getResult(sql,params){
        showSQL(sql,params,this.logger,this.isShowSQL);
        return await this.db.query(sql, params,this.isStream);
    }

    /**
     * 执行sql
     * @param SQL
     * @returns {Promise<*>}
     */
    async execute(SQL){
        try {
            const {sql, params} = SQL.toSQL(); //转换为 SQL 语句
            return this.getResult(sql,params);
        }catch(error){
            retryError(this.logger,error)
        }
    }

    /* ============================================================
    insert、insertMany
    ============================================================ */
    /**
     * 根据命中唯一索引判断是插入新数据还是更新已存在的数据（Upsert 操作）
     * @param {Object} sql - MongoDB 查询对象
     * @param {Object} [options={}] - 选项参数
     * @param {boolean} [options.upsert=false] - 是否启用 upsert，即插入或更新
     * @param {string[]} [options.fields=[]] - 需要更新的字段列表，若为空，则更新所有字段
     * @returns {Promise<Object>} 生成的 SQL 语句对象
     */
    async upsert(sql, options = {}) {
        const { upsert, fields } = options;

        // 若启用了 upsert，则根据提供的字段进行更新
        if (typeof options === "object" && upsert) {
            sql = sql.upsert(upsert, fields);
        }
        return await this.execute(sql)
    }

    /**
     * 插入单条数据
     * @param {Object} data - 需要插入的对象数据
     * @param {Object} [options={}] - 额外的插入选项
     * @returns {Promise<Object>} 生成的 SQL 语句对象
     */
    async insertOne(data, options = {}) {
        let sql = this.insert.insertOne(data); // 生成插入语句
        return await this.upsert(sql, options); // 进行插入或更新操作
    }

    /**
     * 插入多条数据
     * @param {Object} data - 需要插入的数据对象数组
     * @param {Object} [options={}] - 额外的插入选项
     * @returns {Promise<Object>} 生成的 SQL 语句对象
     */
    async insertMany(data, options = {}) {
        let sql = this.insert.insertMany(data); // 生成插入语句
        return await this.upsert(sql, options); // 进行插入或更新操作
    }

    /* ============================================================
    updateOne、updateMany
    ============================================================ */

    // 单个更新
    async updateOne(query, data = {}) {
        let sql = this.update.query(query).update(data).single();
        return await this.execute(sql)
    }

    // 批量更新
    async updateMany(query, data = {}) {
        let sql = this.update.query(query).update(data);
        return await this.execute(sql)
    }

    /* ============================================================
    deleteOne、deleteMany
    ============================================================ */

    // 单个删除
    async deleteOne(query) {
        let sql = this.delete.query(query).single();
        return await this.execute(sql)
    }

    // 批量删除
    async deleteMany(query) {
        let sql = this.delete.query(query);
        return await this.execute(sql)
    }

    /* ============================================================
    count、findOne、find、findPaginate、
    ============================================================ */

    // 统计总记录
    async count(query) {
        try{
            let { sql, params } = this.find.query(query).count();
            return await this.getResult(sql,params);
        }catch(error){
            retryError(this.logger,error)
        }
    }

    // 处理表前缀
    normalizeResultFields(resultRow){
        const normalized = {};
        for (let key in resultRow) {
            // 如果 key 是以 tableName_ 开头，则去除这个前缀
            if (key.startsWith(`${this.tableName}_`)) {
                normalized[key.substring(this.tableName.length + 1)] = resultRow[key];
            } else {
                normalized[key] = resultRow[key];
            }
        }
        return normalized;
    }

    async executeQueryWithJoinsAndNormalization(sql, joins, project) {
        // 如果有连表配置，则设置 join
        if (Array.isArray(joins) && joins.length > 0) {
            sql = sql.join(joins);
        }
        // 执行 SQL
        let result = await this.execute(sql);

        // 如果存在连表且有投影字段，则需要处理表前缀
        if (result && Array.isArray(joins) && joins.length > 0 && project.length > 0 && result.length > 0) {
            result = result.map(item => this.normalizeResultFields(item, this.tableName));
        }
        return result;
    }

    // 单个查找
    async findOne({query={},project=[],joins = []}) {
        let sql = this.find.query(query).project(project).limit(1);
        return await this.executeQueryWithJoinsAndNormalization(sql, joins, project);
    }

    // 批量查找
    async findMany({ query, skip = 1, limit = 10, project = [], sort = { }, joins = [] }) {
        let sql = this.find.query(query).project(project).skip(skip).limit(limit).sort(sort);
        return await this.executeQueryWithJoinsAndNormalization(sql, joins, project);
    }

    // 简单分页查询
    async findPaginate({ query, page = 1, pageSize = 10, project = [], sort = {  }, joins = [], total = false }){
        const offset = (page - 1) * pageSize;   // 计算分页偏移量
        let sql = this.find.query(query).project(project).sort(sort).limit(pageSize).skip(offset);
        let result = await this.executeQueryWithJoinsAndNormalization(sql, joins, project);
        result = { page, pageSize, data:result};
        if (total) {
            const [count] = await this.count(query);
            result.total = count.total;
        }
        return result;
    }

    // 聚合分页查询
    async aggregatePaginate({ query = {}, sort = {}, page, pageSize, joins=[], project = {}, unwind , group, total = false }){
        const offset = (page - 1) * pageSize;   // 计算分页偏移量
        let sql = this.aggregate.match(query).project(project).skip(offset).limit(pageSize).sort(sort).join(joins);
        if(group){
            sql = sql.group(group)
        }
        if(unwind){
            sql = sql.unwind(unwind);
        }
        return await this.executeQueryWithJoinsAndNormalization(sql, joins, project);

    }

}

// 导出 `crud` 类，供外部模块使用
module.exports = Crud;
